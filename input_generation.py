import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import warnings
from dateutil import parser
import sys, os

from src.rts_preprocess_modules.order_preprocess_central import Preprocessing_Central
from src.rts_preprocess_modules.order_preprocess_northern import Preprocessing_Northern
from src.rts_preprocess_modules.order_preprocess_southern import Preprocessing_Southern
from src.rts_preprocess_modules.order_preprocess_eastern import Preprocessing_Eastern
from src.rts_routing_modules.rts_route_commercial import commercial_input
warnings.filterwarnings("ignore")
from pandas.api.types import CategoricalDtype
from src.rts_routing_modules.rts_routing_utilities import calculate_tank_status, change_timedelta_format, renaming_product_id, renaming_product_name, change_strtodatetime_format, change_datetimetofloat_format, modify_multiload_retain, convert_time_interval, texify_numeric_station_id, replace_opening_time_scheduling, replace_start_shift_time
from src.input_gen_modules.rts_input_handler import InputHandler
from conf import Config, Logger
from joblib import Parallel, delayed, parallel_backend

from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)
class InputGen():
    def __init__(self, option, order_id, date, region, loop_status, txn_id, start_time, capacity, largest_capacity, smallest_capacity, post_process = False, input_source = None, multiload_all_tanker = None, normal_all_tanker = None, all_cap_toggle = None, opo2_first = False, opo2_first_capacity = None, tanker_capacity_list = None, order_id_ori = None):
      
        self.option = option #option = 0
        self.order_id = order_id
        self.date = date #date = datetime(2022, 5, 25)
        self.region = region #IN CAPTIAL LETTER, region = 'Central' # list of choices: ['Central', 'Southern', 'Northern', 'Eastern', 'Sabah', 'Sarawak']
        self.loop_status = loop_status
        self.txn_id = txn_id
        self.start_time = start_time
        self.capacity = capacity
        self.largest_capacity = largest_capacity
        self.smallest_capacity = smallest_capacity
        self.post_process = post_process
        self.input_source = input_source
        self.multiload_all_tanker = multiload_all_tanker
        self.normal_all_tanker = normal_all_tanker
        self.all_cap_toggle = all_cap_toggle
        self.sales_time_slot = None
        self.opo2_first = opo2_first
        self.opo2_first_capacity = opo2_first_capacity
        self.tanker_capacity_list = tanker_capacity_list
        self.order_id_ori = order_id_ori

        # Read and process terminal gps data and dates
        self.depot_data_gps = InputHandler.get_terminal_data(self.region)
        self.depot_dates = InputHandler.get_terminal_dates(self.region)

        time_list = ['time_from','time_to']
        for col in time_list :
            self.depot_dates[col] = change_timedelta_format(self.depot_dates[col])     

        # Extract retail stations present in order_bank from retail_customer and retail_customer_storage Table
        # Read and process Retail master data
        pd.set_option("display.max_rows", None, "display.max_columns", None)
        self.retail_data = InputHandler.get_dmr_output_data(self.order_id, self.region, self.date)
        # self.commercial_data = InputHandler.get_commercial_data(self.order_id, self.region, self.date)
        order_retail_list = self.retail_data['retail'].tolist()

        # Change numeric station id into "00XXX..."
        modified_retail_list = texify_numeric_station_id(order_retail_list)

        self.retail_master = InputHandler.get_retail_master(self.region, modified_retail_list, self.date)
        
        # Replace NaN sold_to_party with ship_to_party and change numeric into "00XXX..."
        self.retail_master['sold_to_party'] = self.retail_master['sold_to_party'].fillna(self.retail_master['ship_to_party'].apply(lambda x: x))
        self.retail_master['sold_to_party'] = texify_numeric_station_id(self.retail_master['sold_to_party'])

        print(f"[InputGeneration] Checking order_retail and retail_master extraction. Unique retail stations from: order_data={len(set(modified_retail_list))}; retail_master={len(self.retail_master['ship_to_party'].unique())}")

        # TODO : To add more if else based on region specific long distance condition
        # Set value for long distance based on region
        if Config.MODEL_INPUTOUTPUT['model_region'][self.region] == "CENTRAL" :
            self.retail_master['ld_tag'] = Config.MODEL_INPUTOUTPUT['long_distance'][self.region]
        else :
            self.retail_master.ld_tag = self.retail_master.ld_tag.fillna('non_LD')        
    
    def read_input(self): 
        print("[InputGeneration] Reading master data, master terminal, forecast data, station_restriction, inventory data and historical sales hourly distribution from database is being initiated...")

        # Check to impute retain of multiload (or multiple normal) orders of same product, quantity and retain for distinction of row
        impute_duplicate_retain = False
        if self.loop_status.split('_')[1] == 'First' and self.capacity == self.largest_capacity and self.post_process == False and self.multiload_all_tanker == False and self.normal_all_tanker == False:
            impute_duplicate_retain = True
        elif self.loop_status.split('_')[1] == 'First' and self.multiload_all_tanker == True and self.capacity != self.largest_capacity and self.post_process == False:
            impute_duplicate_retain = True
        elif self.loop_status.split('_')[1] == 'First' and self.normal_all_tanker == True and self.capacity != self.largest_capacity and self.post_process == False:
            impute_duplicate_retain = True
        else :
            impute_duplicate_retain = False

        # Read orderbank data to reattach order id in ori processed orders to merge back order id when output scheduled orders back into orderbank
        pd.set_option("display.max_rows", None, "display.max_columns", None)
        retail_id_df = self.retail_data[['id','multi_load_id','retail','product','volume','retain']]

        # Add 1 hour to retain of multiload (or multiple normal) orders of same product, quantity and retain (for merging order id in pre-process 1st loop)
        if impute_duplicate_retain :   
            retain_column = ['retain']
            column_list = ['retail','product','multi_load_id','volume']
            sort_list = ['retail','product']
            multiload_df = modify_multiload_retain(retain_column, column_list, sort_list, retail_id_df)
            if len(multiload_df) > 0 :
                retail_id_df.loc[multiload_df.index] = multiload_df       

        # Rename to old column schema from AWSM schema      
        master_all = self.retail_master.rename(columns={'ship_to_party':'acc','cloud': 'Cloud2', 'distance':'distance_terminal_u95','road_tanker_accessibility':'accessibility','ld_tag':'long_distance'})
                       
        # Standardize product id based on config
        master_all = renaming_product_id(master_all,"product") 
                                        
        # Subset and process master terminal data by product from master Data                
        master_terminal_data = master_all[['acc','terminal','product']]

        # Standardize product name based on config
        master_terminal_data = renaming_product_name(master_terminal_data,"product")

        # Pivot station terminal to wide data
        tmp = []
        for var in ['terminal']:
            master_terminal_data['tmp_idx'] = var + '_' + master_terminal_data['product'].astype(str)
            tmp.append(master_terminal_data.pivot(index='acc',columns='tmp_idx',values=var))
        master_terminal_data = pd.concat(tmp,axis=1)

        # TODO : once P97 default terminal issue rectify, remove this chunk and uncomment fill NA
        # Impute station terminal with no P97 for Central only
        if Config.MODEL_INPUTOUTPUT['model_region'][self.region] == "CENTRAL":
            master_terminal_data['terminal_PRIMAX_97'] = "NO U97"
        else :
            master_terminal_data['terminal_PRIMAX_97'] = master_terminal_data[['terminal_PRIMAX_97']].fillna(value="NO U97")
        
        # Impute station with no P95 terminal with other products except P97, assuming they are taking from same terminal 
        master_terminal_data.reset_index(inplace=True)
        try :
            master_terminal_data['terminal_PRIMAX_95'] = np.where(master_terminal_data['terminal_PRIMAX_95'].isnull(), master_terminal_data['terminal_BIODIESEL_B10'], master_terminal_data['terminal_PRIMAX_95'])
            if 'terminal_BIODIESEL_B7' in master_terminal_data.columns:
                master_terminal_data['terminal_PRIMAX_95'] = np.where(master_terminal_data['terminal_PRIMAX_95'].isnull(), master_terminal_data['terminal_BIODIESEL_B7'], master_terminal_data['terminal_PRIMAX_95'])
            master_terminal_data['terminal_PRIMAX_95'] = master_terminal_data[['terminal_PRIMAX_95']].fillna(value="NO U95")
            master_terminal_data['terminal_BIODIESEL_B10'] = master_terminal_data[['terminal_BIODIESEL_B10']].fillna(value="NO B10")
            master_terminal_data['terminal_BIODIESEL_B7'] = master_terminal_data[['terminal_BIODIESEL_B7']].fillna(value="NO B7")
        except :
            pass
                       
        # Read and process forecast tomorrow
        forecast_data = InputHandler.get_forecast_data(self.order_id, self.date)

        # Read and process station no delivery interval
        retail_active_region_list = self.retail_master[Config.TABLES['retail_customer_master']["columns"]["acc2"]].unique().ravel().tolist()
        station_restriction = InputHandler.get_retail_dates(retail_active_region_list)

        time_list = ['Opening_Time','Closing_Time','no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
        for col in time_list :
            station_restriction[col] = change_timedelta_format(station_restriction[col])

        # Merge product and terminal into station no delivery interval for different product taking from different terminal scenarios, hence will have different opening times
        station_restriction = pd.merge(station_restriction, self.retail_data[['retail','product','terminal']], 'left', on = 'retail')
        station_restriction = station_restriction.drop_duplicates(subset=['retail','product','terminal'],keep="first")
        station_restriction = station_restriction.rename(columns={"retail": "Ship To"})

        # To replace station opening time 00:00:00 with terminal opening time and closing time 00:00:00 with max station closing time 
        # retail_terminal_merge = self.retail_master[[Config.TABLES['retail_customer_master']["columns"]["acc2"], 'terminal', 'product']].drop_duplicates()
        station_restriction['Opening_Time'] = station_restriction.apply(lambda x: replace_opening_time_scheduling(x, self.depot_dates), axis=1)
        max_station_end_time = str(max(station_restriction['Closing_Time']))
        station_restriction['Closing_Time'] = np.where(station_restriction['Closing_Time'].astype(str) == '24:00:00', '23:59:00', station_restriction['Closing_Time'].astype(str)) 
        station_restriction['Closing_Time'] = np.where(station_restriction['Closing_Time'].astype(str) == '00:00:00', max_station_end_time, station_restriction['Closing_Time'].astype(str))            

        time_list = ['Opening_Time','Closing_Time','no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
        for col in time_list :
            station_restriction[col] = change_strtodatetime_format(station_restriction[col], self.date + timedelta(days=1))     
            
        # Read opening inventory tomorrow
        inventory_data_tomorrow = InputHandler.get_inventory_data(self.order_id, self.date)

        # Read historical sales distribution
        self.sales_time_slot = InputHandler.get_retain_data(self.region)
                
        print("[InputGeneration] Reading master data, master terminal, forecast data, station_restriction, inventory data and historical sales hourly distribution from database is completed...")
        
        # 1st Loop - Read and process order data 
        if self.loop_status.split('_')[1] == 'First': 
            # Read and process dmr/opo orders from delivery note
            if self.option:
                # order_data = InputHandler.get_delivery_note(import_settings)
                # order_data = pd.merge(order_data, master_all[['acc', 'Cloud2','distance_terminal_u95','accessibility']], 'left',
                #                     left_on=['Ship-to'],
                #                     right_on=['acc'])
                # order_data = order_data.rename(columns={'      Qty':'Quantity', 'Remarks':'Remark'})
                pass
            
            else:
                # Read and process unscheduled and unmatched orders for both normal and post process sequential loops - biggest tanker first loop ONLY
                # or Read and process dmr/opo orders for normal sequential tanker capacity loops - first loop ONLY
                if self.capacity == self.largest_capacity:
                    # Read and process unscheduled and unmatched orders from previous normal sequential loops for post process sequential loops - biggest tanker first loop ONLY
                    if self.post_process:
                        log_module = "[InputGeneration]"
                        previous_capacity_loop = f"{self.smallest_capacity}_Second"

                        try:
                            print("{} Reading UNSCHEDULED data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                            unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                            unscheduled_orders['Product'] = unscheduled_orders['Product'].astype(int)
                            retail_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "RE"]
                            retail_unscheduled = retail_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unscheduled']]
                        except Exception as e:
                            print("{} Unable to load UNSCHEDULED data as : {}".format(log_module, e))
                            retail_unscheduled = pd.DataFrame()
                        
                        try :
                            print("{} Subsetting commercial data from UNSCHEDULED data for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id)) 
                            unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                            commercial_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "CO"]
                            commercial_unscheduled, unmatched_commercial_1 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unscheduled)
                            commercial_unscheduled = commercial_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unscheduled']]
                        except Exception as e:
                            print("{} Unable to load UNSCHEDULED commercial data as : {}".format(log_module, e))
                            commercial_unscheduled = pd.DataFrame()
                            unmatched_commercial_1 = pd.DataFrame()                         
                        print("{} Reading UNSCHEDULED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))

                        try:    
                            print("{} Reading UNMATCHED order data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                            unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                            retail_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "RE"] 
                            retail_unmatched = retail_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unmatched']]
                        except Exception as e:
                            print("{} Unable to load UNMATCHED data as : {}".format(log_module,e))
                            retail_unmatched = pd.DataFrame()
                        
                        try :
                            print("{} Subsetting commercial data from UNMATCHED data for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id)) 
                            unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                            commercial_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "CO"]
                            commercial_unmatched, unmatched_commercial_2 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unmatched)
                            commercial_unmatched = commercial_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unmatched']]
                        except Exception as e:
                            print("{} Unable to load UNMATCHED commercial data as : {}".format(log_module, e))
                            commercial_unmatched = pd.DataFrame()
                            unmatched_commercial_2 = pd.DataFrame() 
                        print("{} Reading UNMATCHED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))
    
                        print("{} Compiling UNSCHEDULED and UNMATCHED orders from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                        if len(retail_unscheduled) > 0 or len(retail_unmatched) > 0 :
                            order_data = pd.concat([retail_unscheduled, retail_unmatched], axis = 0)
                        else :
                            print("{} No UNSCHEDULED and UNMATCHED orders left to be processed from {} loop. System existing ...".format(log_module, previous_capacity_loop))
                            end_time = datetime.now()
                            diff = end_time - self.start_time
                            print("Total run time is {}.".format(diff))
                            raise Exception("No UNSCHEDULED and UNMATCHED orders left") 

                        if len(commercial_unscheduled) > 0 or len(commercial_unmatched) > 0 :
                            commercial_data = pd.concat([commercial_unscheduled, commercial_unmatched], axis = 0)
                            unmatched_commercial = pd.concat([unmatched_commercial_1, unmatched_commercial_2], axis = 0)
                            if len(unmatched_commercial) > 0 :
                                unmatched_commercial = unmatched_commercial[Config.MODEL_INPUTOUTPUT['order_input_columns']['unmatched_commercial']]
                            else :
                                unmatched_commercial = pd.DataFrame()                        
                        else :
                            commercial_data = pd.DataFrame()
                            unmatched_commercial = pd.DataFrame()

                        order_data['Tank_Status'] = [calculate_tank_status(master_all, forecast_data, x, y) for x, y in zip(order_data['Destinations'], order_data['Product'])]
                        order_data = order_data.rename(columns = {'Destinations':'Ship To','Product':'Material', 'Order_Quantity':'Quantity','Retain_Hour':'retain_hour','Terminal_Default':'terminal'}).reset_index(drop=True)
                        order_data = pd.merge(order_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['order_subset_2']], master_all[Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2']].drop_duplicates(subset=Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2'],keep="first"), 'left', left_on=['Ship To', 'Material'], right_on=['acc', 'product'])  

                    # Read and process unscheduled and unmatched orders from previous all capacity second loops for normal sequential loops - biggest tanker first loop ONLY if strategy starts with "all_capacity_first" or "multiload_all_capacity_first"   
                    elif self.multiload_all_tanker == True or self.normal_all_tanker == True:
                        log_module = "[InputGeneration]"
                        previous_capacity_loop = "AllCap_Second"
                        
                        try:
                            print("{} Reading UNSCHEDULED data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                            unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                            unscheduled_orders['Product'] = unscheduled_orders['Product'].astype(int)
                            retail_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "RE"]
                            retail_unscheduled = retail_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unscheduled']]
                        except Exception as e:
                            print("{} Unable to load UNSCHEDULED data as : {}".format(log_module, e))
                            retail_unscheduled = pd.DataFrame()
                        
                        try :
                            unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                            commercial_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "CO"]
                            commercial_unscheduled, unmatched_commercial_1 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unscheduled)
                            commercial_unscheduled = commercial_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unscheduled']]
                        except Exception as e:
                            print("{} Unable to load UNSCHEDULED commercial data as : {}".format(log_module, e))
                            commercial_unscheduled = pd.DataFrame()
                            unmatched_commercial_1 = pd.DataFrame()                       
                        print("{} Reading UNSCHEDULED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))

                        try:    
                            print("{} Reading UNMATCHED order data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                            unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                            retail_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "RE"] 
                            retail_unmatched = retail_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unmatched']]
                        except Exception as e:
                            print("{} Unable to load UNMATCHED data as : {}".format(log_module,e))
                            retail_unmatched = pd.DataFrame()
                        
                        try :
                            print("{} Subsetting commercial data from UNMATCHED data for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id)) 
                            unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                            commercial_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "CO"]
                            commercial_unmatched, unmatched_commercial_2 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unmatched)
                            commercial_unmatched = commercial_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unmatched']]
                        except Exception as e:
                            print("{} Unable to load UNMATCHED commercial data as : {}".format(log_module, e))
                            commercial_unmatched = pd.DataFrame()
                            unmatched_commercial_2 = pd.DataFrame() 
                        print("{} Reading UNMATCHED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))
    
                        print("{} Compiling UNSCHEDULED and UNMATCHED orders from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                        if len(retail_unscheduled) > 0 or len(retail_unmatched) > 0 :
                            order_data = pd.concat([retail_unscheduled, retail_unmatched], axis = 0)
                        else :
                            print("{} No UNSCHEDULED and UNMATCHED orders left to be processed from {} loop. System existing ...".format(log_module, previous_capacity_loop))
                            end_time = datetime.now()
                            diff = end_time - self.start_time
                            print("Total run time is {}.".format(diff))
                            raise Exception("No UNSCHEDULED and UNMATCHED orders left") 

                        if len(commercial_unscheduled) > 0 or len(commercial_unmatched) > 0 :
                            commercial_data = pd.concat([commercial_unscheduled, commercial_unmatched], axis = 0)
                            unmatched_commercial = pd.concat([unmatched_commercial_1, unmatched_commercial_2], axis = 0)
                            if len(unmatched_commercial) > 0 :
                                unmatched_commercial = unmatched_commercial[Config.MODEL_INPUTOUTPUT['order_input_columns']['unmatched_commercial']]
                            else :
                                unmatched_commercial = pd.DataFrame()                        
                        else :
                            commercial_data = pd.DataFrame()
                            unmatched_commercial = pd.DataFrame()

                        order_data['Tank_Status'] = [calculate_tank_status(master_all, forecast_data, x, y) for x, y in zip(order_data['Destinations'], order_data['Product'])]
                        order_data = order_data.rename(columns = {'Destinations':'Ship To','Product':'Material','Order_Quantity':'Quantity','Retain_Hour':'retain_hour','Terminal_Default':'terminal'}).reset_index(drop=True)
                        order_data = pd.merge(order_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['order_subset_2']], master_all[Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2']].drop_duplicates(subset=Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2'],keep="first"), 'left', left_on=['Ship To', 'Material'], right_on=['acc', 'product'])  

                    # Read and process dmr/opo orders for normal sequential loops - biggest tanker first loop ONLY if strategy starts with "sequential_fist"
                    else :
                        if self.input_source == "dmr":
                            print("[InputGeneration] Reading DMR output data from database for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                            order_data = InputHandler.get_dmr_output_data(self.order_id, self.region, self.date)
                        # if self.input_source == "opo":
                        #     print("[InputGeneration] Reading OPO output data from database for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                        #     order_data = InputHandler.get_opo_output_data(import_settings, self.region, self.date)
                        #     order_data['Remark'] = np.where(order_data['Remark'] > 0, order_data['Remark'], 2)

                        if len(order_data) == 0:
                            print("[InputGeneration] NO orders to be processed. System existing ...")
                            end_time = datetime.now()
                            diff = end_time - self.start_time
                            print("Total run time is {}.".format(diff))
                            raise Exception("NO orders to be processed") 

                        else:
                            # Read Commercial Order Data
                            print("[InputGeneration] Reading Commercial order data from database for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                            commercial_data, unmatched_commercial = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, self.order_id)
                            # Merging terminal info into commercial order data
                            try :
                                commercial_data = commercial_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_input']]
                            except Exception as e:
                                print("[InputGeneration] NO Commercial orders for this DMR due to {} and COMM order has {} orders".format(e,len(commercial_data)))
                                commercial_data = pd.DataFrame()

                            order_data = order_data.rename(columns={'retail':'Ship To','priority':'Priority','order_type':'Order Type','product':'Material','volume':'Quantity','retain':'retain_hour','runout':'Runout','multi_load_id':'Multiload_Status'})
                            order_data['Tank_Status'] = [calculate_tank_status(master_all, forecast_data, x, y) for x, y in zip(order_data['Ship To'], order_data['Material'])]
                            order_data = pd.merge(order_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['order_subset_1']], master_all[Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_1']].drop_duplicates(subset=Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_1'],keep="first"), 'left', left_on=['Ship To', 'Material'], right_on=['acc', 'product'])
                            print("[InputGeneration] Reading DMR output data from database for {} in {} is completed...".format(self.loop_status,self.txn_id)) 
                
                # Read and process dmr/opo orders for all tanker capacity loops - first loop ONLY
                elif self.multiload_all_tanker == True or self.normal_all_tanker == True :
                    if self.input_source == "dmr":
                        print("[InputGeneration] Reading DMR output data from database for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                        order_data = InputHandler.get_dmr_output_data(self.order_id, self.region, self.date)
                    # if self.input_source == "opo":
                    #     print("[InputGeneration] Reading OPO output data from database for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    #     order_data = InputHandler.get_opo_output_data(import_settings, self.region, self.date)
                    #     order_data['Remark'] = np.where(order_data['Remark'] > 0, order_data['Remark'], 2)

                    if len(order_data) == 0:
                        print("[InputGeneration] NO orders to be processed. System existing ...")
                        end_time = datetime.now()
                        diff = end_time - self.start_time
                        print("Total run time is {}.".format(diff))
                        raise Exception("NO orders to be processed") 

                    else:
                        # Read Commercial Order Data
                        print("[InputGeneration] Reading Commercial order data from database for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                        commercial_data, unmatched_commercial = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, self.order_id)
                        # Merging terminal info into commercial order data
                        try :
                            commercial_data = commercial_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_input']]                        
                        except Exception as e:
                            print("[InputGeneration] NO Commercial orders for this DMR due to {} and COMM order has {} orders".format(e,len(commercial_data)))
                            commercial_data = pd.DataFrame()

                        order_data = order_data.rename(columns={'retail':'Ship To','priority':'Priority','order_type':'Order Type','product':'Material','volume':'Quantity','retain':'retain_hour','runout':'Runout','multi_load_id':'Multiload_Status'})                 
                        order_data['Tank_Status'] = [calculate_tank_status(master_all, forecast_data, x, y) for x, y in zip(order_data['Ship To'], order_data['Material'])]
                        order_data = pd.merge(order_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['order_subset_0']], master_all[Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_1']].drop_duplicates(subset=Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_1'],keep="first"), 'left', left_on=['Ship To', 'Material'], right_on=['acc','product'])
                        print("[InputGeneration] Reading DMR output data from database for {} in {} is completed...".format(self.loop_status,self.txn_id)) 

                # Read and process unscheduled and unmatched orders from previous normal sequential second loop for normal sequential subsequent first loop - except biggest tanker first loop
                else :
                    if self.region == "CENTRAL" :
                        if self.capacity != 21840 :
                            previous_capacity = self.capacity + 5460
                        else:
                            previous_capacity = self.capacity + 10920

                    if self.region == "SOUTHERN" :
                        if self.capacity == 43680 :
                            previous_capacity = self.capacity + 10920
                        elif self.capacity == 32760 :
                            previous_capacity = self.capacity + 10920
                        else :
                            previous_capacity = self.capacity + 5460

                    if self.post_process:
                        previous_capacity_loop = f"post{previous_capacity}_Second"
                        log_module = "[InputGeneration Post Process]"
                    else :
                        previous_capacity_loop = f"{previous_capacity}_Second"
                        log_module = "[InputGeneration]"
                    
                    try:
                        print("{} Reading UNSCHEDULED data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                        unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                        unscheduled_orders['Product'] = unscheduled_orders['Product'].astype(int)
                        retail_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "RE"]
                        retail_unscheduled = retail_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unscheduled']]
                    except Exception as e:
                        print("{} Unable to load UNSCHEDULED data as : {}".format(log_module, e))
                        retail_unscheduled = pd.DataFrame()
                    
                    try :
                        unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                        commercial_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "CO"]
                        commercial_unscheduled, unmatched_commercial_1 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unscheduled)
                        commercial_unscheduled = commercial_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unscheduled']]
                    except Exception as e:
                        print("{} Unable to load UNSCHEDULED commercial data as : {}".format(log_module, e))
                        commercial_unscheduled = pd.DataFrame()
                        unmatched_commercial_1 = pd.DataFrame()                      
                    print("{} Reading UNSCHEDULED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))

                    try:    
                        print("{} Reading UNMATCHED order data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                        unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                        retail_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "RE"] 
                        retail_unmatched = retail_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unmatched']]
                    except Exception as e:
                        print("{} Unable to load UNMATCHED data as : {}".format(log_module,e))
                        retail_unmatched = pd.DataFrame()
                    
                    try :
                        print("{} Subsetting commercial data from UNMATCHED data for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id)) 
                        unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                        commercial_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "CO"]
                        commercial_unmatched, unmatched_commercial_2 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unmatched)
                        commercial_unmatched = commercial_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unmatched']]
                    except Exception as e:
                        print("{} Unable to load UNMATCHED commercial data as : {}".format(log_module, e))
                        commercial_unmatched = pd.DataFrame()
                        unmatched_commercial_2 = pd.DataFrame() 
                    print("{} Reading UNMATCHED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))

                    print("{} Compiling UNSCHEDULED and UNMATCHED orders from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                    if len(retail_unscheduled) > 0 or len(retail_unmatched) > 0 :
                        order_data = pd.concat([retail_unscheduled, retail_unmatched], axis = 0)
                    else :
                        print("{} No UNSCHEDULED and UNMATCHED orders left to be processed from {} loop. System existing ...".format(log_module, previous_capacity_loop))
                        end_time = datetime.now()
                        diff = end_time - self.start_time
                        print("Total run time is {}.".format(diff))
                        raise Exception("No UNSCHEDULED and UNMATCHED orders left") 

                    if len(commercial_unscheduled) > 0 or len(commercial_unmatched) > 0 :
                        commercial_data = pd.concat([commercial_unscheduled, commercial_unmatched], axis = 0)
                        unmatched_commercial = pd.concat([unmatched_commercial_1, unmatched_commercial_2], axis = 0)
                        if len(unmatched_commercial) > 0 :
                            unmatched_commercial = unmatched_commercial[Config.MODEL_INPUTOUTPUT['order_input_columns']['unmatched_commercial']]
                        else :
                            unmatched_commercial = pd.DataFrame()                    
                    else :
                        commercial_data = pd.DataFrame()
                        unmatched_commercial = pd.DataFrame()

                    order_data['Tank_Status'] = [calculate_tank_status(master_all, forecast_data, x, y) for x, y in zip(order_data['Destinations'], order_data['Product'])]

                    order_data = order_data.rename(columns = {'Destinations':'Ship To','Product':'Material','Order_Quantity':'Quantity','Retain_Hour':'retain_hour','Terminal_Default':'terminal'}).reset_index(drop=True)
                    order_data = pd.merge(order_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['order_subset_2']], master_all[Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2']].drop_duplicates(subset=Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2'],keep="first"), 'left', left_on=['Ship To', 'Material'], right_on=['acc', 'product'])   
             
        # 2nd Loop - Read from unmatched + unscheduled orders 1st loop                      
        else :
            if self.multiload_all_tanker == True or self.normal_all_tanker == True:
                previous_capacity_loop = "AllCap_First"
                log_module = "[InputGeneration]"
            else:
                if self.post_process :
                    previous_capacity_loop = f"post{self.capacity}_First"
                    log_module = "[InputGeneration Post Process]"
                else :
                    previous_capacity_loop = f"{self.capacity}_First"
                    log_module = "[InputGeneration]"
     
            try:
                print("{} Reading UNSCHEDULED data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                unscheduled_orders['Product'] = unscheduled_orders['Product'].astype(int)
                retail_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "RE"]
                retail_unscheduled = retail_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unscheduled']]
            except Exception as e:
                print("{} Unable to load UNSCHEDULED data as : {}".format(log_module, e))
                retail_unscheduled = pd.DataFrame()
            
            try :
                unscheduled_orders = InputHandler.get_unscheduled_data(previous_capacity_loop, self.txn_id)
                commercial_unscheduled = unscheduled_orders[unscheduled_orders['Order_Type'] == "CO"]
                commercial_unscheduled, unmatched_commercial_1 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unscheduled)
                commercial_unscheduled = commercial_unscheduled[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unscheduled']]
            except Exception as e:
                print("{} Unable to load UNSCHEDULED commercial data as : {}".format(log_module, e))
                commercial_unscheduled = pd.DataFrame()
                unmatched_commercial_1 = pd.DataFrame()                      
            print("{} Reading UNSCHEDULED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))

            try:    
                print("{} Reading UNMATCHED order data from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
                unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                retail_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "RE"] 
                retail_unmatched = retail_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['retail_unmatched']]
            except Exception as e:
                print("{} Unable to load UNMATCHED data as : {}".format(log_module,e))
                retail_unmatched = pd.DataFrame()
            
            try :
                print("{} Subsetting commercial data from UNMATCHED data for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id)) 
                unmatched_orders = InputHandler.get_unmatched_data(previous_capacity_loop, self.txn_id)
                commercial_unmatched = unmatched_orders[unmatched_orders['Order_Type'] == "CO"]
                commercial_unmatched, unmatched_commercial_2 = commercial_input(self.date, self.region, self.loop_status, self.start_time, self.txn_id, self.capacity, self.depot_dates, None, commercial_unmatched)
                commercial_unmatched = commercial_unmatched[Config.MODEL_INPUTOUTPUT['order_input_columns']['commercial_unmatched']]
            except Exception as e:
                print("{} Unable to load UNMATCHED commercial data as : {}".format(log_module, e))
                commercial_unmatched = pd.DataFrame()
                unmatched_commercial_2 = pd.DataFrame() 
            print("{} Reading UNMATCHED data from database for {} in {} has completed...".format(log_module, self.loop_status, self.txn_id))

            print("{} Compiling UNSCHEDULED and UNMATCHED orders from database for {} in {} is being initiated...".format(log_module, self.loop_status,self.txn_id))
            if len(retail_unscheduled) > 0 or len(retail_unmatched) > 0 :
                order_data = pd.concat([retail_unscheduled, retail_unmatched], axis = 0)
            else :
                print("{} No UNSCHEDULED and UNMATCHED orders left to be processed from {} loop. System existing ...".format(log_module, previous_capacity_loop))
                end_time = datetime.now()
                diff = end_time - self.start_time
                print("Total run time is {}.".format(diff))
                raise Exception("No UNSCHEDULED and UNMATCHED orders left") 

            if len(commercial_unscheduled) > 0 or len(commercial_unmatched) > 0 :
                commercial_data = pd.concat([commercial_unscheduled, commercial_unmatched], axis = 0)
                unmatched_commercial = pd.concat([unmatched_commercial_1, unmatched_commercial_2], axis = 0)
                if len(unmatched_commercial) > 0 :
                    unmatched_commercial = unmatched_commercial[Config.MODEL_INPUTOUTPUT['order_input_columns']['unmatched_commercial']]
                else :
                    unmatched_commercial = pd.DataFrame()
            else :
                commercial_data = pd.DataFrame()
                unmatched_commercial = pd.DataFrame()

            order_data['Tank_Status'] = [calculate_tank_status(master_all, forecast_data, x, y) for x, y in zip(order_data['Destinations'], order_data['Product'])]
            order_data = order_data.rename(columns = {'Destinations':'Ship To','Product':'Material','Order_Quantity':'Quantity','Retain_Hour':'retain_hour','Terminal_Default':'terminal'}).reset_index(drop=True)
            order_data = pd.merge(order_data[Config.MODEL_INPUTOUTPUT['order_input_columns']['order_subset_2']], master_all[Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2']].drop_duplicates(subset=Config.MODEL_INPUTOUTPUT['order_input_columns']['master_data_2'],keep="first"), 'left', left_on=['Ship To', 'Material'], right_on=['acc', 'product'])

        # Add 1 hour to retain of multiload (or multiple normal) orders of same product, quantity and retain (before below re-adjustment)
        if impute_duplicate_retain :
            retain_column = ['retain_hour']
            column_list = ['Ship To','Material','Quantity','Multiload_Status']
            sort_list = ['Ship To','Material']
            multiload_df = modify_multiload_retain(retain_column, column_list, sort_list, order_data)
            if len(multiload_df) > 0 :
                order_data.loc[multiload_df.index] = multiload_df
        
        # Re-adjust retain to suit respective ordering stations opening time and no delivery interval        
        order_data = pd.merge(order_data, station_restriction[['Ship To','product','Opening_Time','Closing_Time','no_delivery_start_1','no_delivery_end_1','no_delivery_start_2','no_delivery_end_2']], 'left', on = ['Ship To','product'])
        order_data = order_data.rename(columns={'Quantity':'Qty', 'Ship To':'Ship-to'})  # Rename of ship to and quantity is here, after collating all inputs
        # order_data['retain_hour'] = order_data['retain_hour'].apply(lambda x: pd.to_datetime(x).time())
        order_data['retain_ori'] = order_data['retain_hour'].copy() # IMPORTANT : store original retain before readjustment 
        order_data['retain_hour'] = np.where(order_data['retain_hour'] < order_data['Opening_Time'] , order_data['Opening_Time'], order_data['retain_hour'])
        order_data['retain_hour'] = np.where((order_data['retain_hour'] < order_data['no_delivery_end_1']) & (order_data['retain_hour'] > order_data['no_delivery_start_1']) , order_data['no_delivery_end_1'], order_data['retain_hour'])
        order_data['retain_hour'] = np.where((order_data['retain_hour'] < order_data['no_delivery_end_2']) & (order_data['retain_hour'] > order_data['no_delivery_start_2']) , order_data['no_delivery_end_2'], order_data['retain_hour'])
        # order_data['retain_hour'] = order_data['retain_hour'].apply(lambda x: datetime.combine(self.date + timedelta(days=1), x))
        order_data['Ship-to'] = order_data['Ship-to'].astype(int)

        # Add 1 hour to retain of multiload orders of same product and retain (after above re-adjustment)
        if impute_duplicate_retain :
            retain_column = ['retain_hour']
            column_list = ['Ship-to','Order Type','Material','Multiload_Status']
            sort_list = ['Ship-to','Material']
            multiload_df = modify_multiload_retain(retain_column, column_list, sort_list, order_data)
            if len(multiload_df) > 0 :
                order_data.loc[multiload_df.index] = multiload_df
                
        # Get GPS data and subset only order from retail stations found in GPS directory
        print("[InputGeneration] Reading GPS data from database is being initiated...")
        gps_data = InputHandler.get_gps_data(self.region)
        print("[InputGeneration] Reading GPS data from database has completed...")
        order_data = order_data[(order_data['Ship-to'].isin(list(gps_data['ship_to_party'].values)))]

        # post process - change original cluster of orders to new crossed cluster
        if self.post_process and self.capacity == self.largest_capacity and self.loop_status.split('_')[1] == 'First':
            print("[InputGeneration] Cross Cluster for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
            cloud = InputHandler.get_cross_cloud_paring_data(self.region)
            order_data = pd.merge(order_data, cloud[['ship_to_party', 'alternative_cluster']], how="left", left_on=['Ship-to'], right_on=['ship_to_party'])
            order_data['alternative_cluster'] = np.where(order_data['alternative_cluster'].isnull(), order_data['Cloud2'], order_data['alternative_cluster'])
            order_data.drop('ship_to_party', axis=1, inplace=True)
            order_data['Cloud2'] = order_data['alternative_cluster']
            order_data['Cloud2'] = order_data['Cloud2'].astype(int)
            print(f"[InputGeneration] After Post Process, current length of order : {len(order_data)}")

        # filter certain cloud / stations to troubleshoot code
        # order_data = order_data[order_data['Cloud2'].isin([22])]
        # order_data = order_data[order_data['Ship-to'].isin([90039407,90032527])]
        # order_data = order_data[order_data['Multiload_Status'].isin(['Multiload'])]
        order_data = order_data.sort_values('Ship-to').reset_index(drop=True)

        # Get full list of order before grouping same stations different products orders together, with cloud number
        order_data_full = order_data.copy()

        print("[InputGeneration] Cluster in current input orders for {} in {}: {}".format(self.loop_status,self.txn_id,order_data_full.Cloud2.unique()))
        print("[InputGeneration] [DMR : {}] Total number of current input orders for {} in {}: {}".format(self.date,self.loop_status,self.txn_id,len(order_data_full)))

        # Get density to calculate weight of product
        print("[InputGeneration] Reading density data from database is being initiated...")
        product_density = InputHandler.get_product_density_data(self.region)
        print("[InputGeneration] Reading density data from database has completed...")
        order_weight = pd.merge(order_data, product_density[['Product','Density_values','terminal']], 'left', left_on=['Material', 'terminal'], right_on=['Product','terminal'])
        # sizes = order_weight.groupby(['Ship-to','Material'])['Material'].transform("size")
        # order_weight.loc[sizes <= 2, 'Density_values'] = order_weight.loc[sizes <= 2, 'Density_values'].ffill()

        # TODO: Temporary Fix to fill in missing product_density 
        order_weight[['Product', 'Density_values']] = order_weight.groupby('Material')[['Product', 'Density_values']].apply(lambda x: x.ffill().bfill())        
        # Overwrite missing values in Product and Density values column if missing
        if order_weight['Density_values'].isna().sum() > 0:
            missing_density_dict = dict(zip(product_density['Product'], product_density['Density_values']))
            order_weight['Product'] = np.where(order_weight['Product'].isna(), order_weight['Material'], order_weight['Product'])
            order_weight['Density_values'] = order_weight['Density_values'].fillna(order_weight['Product'].map(missing_density_dict))        
        order_weight['Total_weight'] = order_weight['Qty'] * order_weight['Density_values']

        # Subset order columns based on loop and strategy condition
        if self.loop_status.split('_')[1] == 'First' and self.capacity == self.largest_capacity and self.post_process == False and self.multiload_all_tanker == False and self.normal_all_tanker == False:
            order_data = order_weight[['Ship-to', 'Priority', 'Order Type', 'Material', 'terminal', 'Qty', 'retain_hour', 'retain_ori', 'Runout','Total_weight', 'acc', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status', 'long_distance', 'Multiload_Status', 'Opening_Time', 'Closing_Time']]
        elif self.loop_status.split('_')[1] == 'First' and self.multiload_all_tanker == True and self.capacity != self.largest_capacity and self.post_process == False:
            order_data = order_weight[['Ship-to', 'Priority', 'Order Type', 'Material', 'terminal', 'Qty', 'retain_hour', 'retain_ori', 'Runout','Total_weight', 'acc', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status', 'long_distance', 'Multiload_Status', 'Opening_Time', 'Closing_Time']]
        elif self.loop_status.split('_')[1] == 'First' and self.normal_all_tanker == True and self.capacity != self.largest_capacity and self.post_process == False:
            order_data = order_weight[['Ship-to', 'Priority', 'Order Type', 'Material', 'terminal', 'Qty', 'retain_hour', 'retain_ori', 'Runout','Total_weight', 'acc', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status', 'long_distance', 'Multiload_Status', 'Opening_Time', 'Closing_Time']]
        elif self.loop_status.split('_')[1] == 'First' :
            order_data = order_weight[['Ship-to', 'Priority', 'Material', 'terminal', 'Qty', 'retain_hour', 'retain_ori', 'Runout','Total_weight', 'acc', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status', 'long_distance', 'Multiload_Status', 'condition', 'grouping','order_status', 'Opening_Time', 'Closing_Time']]
        else :
            order_data = order_weight[['Ship-to', 'Priority', 'Material', 'terminal', 'Qty', 'retain_hour', 'retain_ori', 'Runout','acc', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status', 'long_distance', 'Multiload_Status','condition', 'grouping','order_status','Density_values', 'Opening_Time', 'Closing_Time']]
        
        # Set Tank Status and Multiload Status to Categorical
        cat_type = CategoricalDtype(categories=['Normal', 'LV2', 'LV1', 'TC', 'Error', 'STATION INACTIVE', 'PRODUCT NOT RECOGNISED', 'NO FORECAST DATA'], ordered=True)
        order_data['Tank_Status'] = order_data['Tank_Status'].astype(cat_type)
        cat_type_multi = CategoricalDtype(categories=['Normal', 'Multiload'], ordered=True)
        order_data['Multiload_Status'] = order_data['Multiload_Status'].astype(cat_type_multi)

        # Calculate runout duration
        order_data['runout_duration'] = order_data.apply(lambda row: row.Runout - row.retain_hour, axis=1)

        # Calculate user specified max retain datetime before delivery can execute after quantity modification in opo2
        end_day = self.date + timedelta(days=2)

        # TODO : To include pre-process for other regions by adding elif condition
        # Pre-process order data by region
        if Config.MODEL_INPUTOUTPUT['model_region'][self.region] == "CENTRAL" :
            order_data.drop('long_distance', axis=1, inplace=True)            
            preprocess_central = Preprocessing_Central(end_day, order_data, master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, commercial_data, unmatched_commercial, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df)
            ori_order_multiload, processed_order_multiload, full_list = preprocess_central.preprocess_order()
            ori_order_multiload['opo2'] = False
            processed_order_multiload['opo2'] = False
            full_list['opo2'] = False

            # Further pre-process order data with OPO2 first loop
            if self.opo2_first :
                full_list_b4_opo2 = full_list.copy()
                full_list_b4_opo2 = full_list_b4_opo2[full_list_b4_opo2['order_status'] != 'smp'] # exclude SMP orders from OPO2 first to avoid duplicates
                full_list_b4_opo2['condition'] = full_list_b4_opo2['condition'].str.replace(r'_[0-9]+','')
                full_list_b4_opo2['ori_status'] = 'original'
                full_list_b4_opo2 = pd.merge(full_list_b4_opo2, product_density[['Product', 'Density_values']], left_on='Material', right_on='Product')
                full_list_b4_opo2.drop('Total_weight', axis=1, inplace=True)
                preprocess_central = Preprocessing_Central(end_day, full_list_b4_opo2, master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, commercial_data, unmatched_commercial, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df, self.opo2_first)
                ori_order_multiload_opo2, processed_order_multiload_opo2, full_list_opo2 = preprocess_central.preprocess_order()
                
                ori_order_multiload_opo2['opo2'] = True
                processed_order_multiload_opo2['opo2'] = True
                full_list_opo2['opo2'] = True
                                
                ori_order_multiload = pd.concat([ori_order_multiload, ori_order_multiload_opo2], axis=0)
                processed_order_multiload = pd.concat([processed_order_multiload, processed_order_multiload_opo2], axis=0)
                full_list = pd.concat([full_list, full_list_opo2], axis=0)
            else :
                ori_order_multiload['ori_status'] = 'original'
                processed_order_multiload['ori_status'] = 'original'
                full_list['ori_status'] = 'original'
                
            ori_order_multiload['long_distance'] = Config.MODEL_INPUTOUTPUT['long_distance'][self.region]
            processed_order_multiload['long_distance'] = Config.MODEL_INPUTOUTPUT['long_distance'][self.region]
            full_list['long_distance'] = Config.MODEL_INPUTOUTPUT['long_distance'][self.region]
                        
        elif Config.MODEL_INPUTOUTPUT['model_region'][self.region] == "SOUTHERN":
            terminal_list = list(order_data['terminal'].unique())
            runs = [Preprocessing_Southern(end_day, order_data[order_data['terminal']==terminal], master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df) for terminal in terminal_list]
            results = []

            with parallel_backend(backend="loky"):
                delayed_funcs = [delayed(lambda x:x.preprocess_order())(run) for run in runs]
            try :
                parallel = Parallel(n_jobs=-1, verbose=100)
                results = parallel(delayed_funcs)
            except Exception as e :
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno, e)
                parallel = Parallel(n_jobs=4, verbose=100)
                results = parallel(delayed_funcs)
            
            ori_order_multiload = pd.concat([results[0][0], results[1][0]])
            processed_order_multiload = pd.concat([results[0][1], results[1][1]])
            full_list = pd.concat([results[0][2], results[1][2]])
            
            if self.loop_status.split('_')[1] == 'First' :
                full_list_export = pd.concat([results[0][3], results[1][3]])
            
                if len(unmatched_commercial) > 0:
                    unmatched_commercial = unmatched_commercial[['Route_ID','Terminal_97','Terminal_Default','No_Compartment','Tanker_Cap','Order_ID','Destinations','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Utilization','Retain_Hour','Runout','End_Stock_Day','Travel_Time','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Min_Retain_Hour','Min_Runout','DMR_Date','Loop_Counter','Job_StartTime','Txn_ID','Region','Order_Type','delta','id']]                        
                else :
                    unmatched_commercial = pd.DataFrame()
                
                if len(commercial_data) > 0:
                    comm_list_export = commercial_data.append(unmatched_commercial)
                    comm_list_export['retain_ori'] = comm_list_export['Retain_Hour']
                    comm_list_export['Total_weight'] = 0
                    comm_list_export['distance_terminal_u95'] = 0
                    comm_list_export = comm_list_export.rename(columns = {'Destinations':'Ship-to', 'Product':'Material', 'Order_Quantity':'Qty', 'Retain_Hour':'retain_hour', 'Cloud':'Cloud2', 'Accessibility':'accessibility'})
                    comm_list_export = comm_list_export[['Ship-to', 'Priority', 'Terminal_97', 'Terminal_Default', 'Material', 'Qty', 'Priority', 'retain_hour', 'retain_ori', 'Runout', 'Opening_Time', 'Closing_Time', 'Total_weight', 'End_Stock_Day', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status', 'long_distance', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'Txn_ID', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Order_Type', 'id']]
                    full_list_export = full_list_export.append(comm_list_export)
                    
                DBConnector.save(full_list_export, Config.TABLES['processed_ori_orders']["tablename"], if_exists='append')                
            
            ori_order_multiload['opo2'] = False
            processed_order_multiload['opo2'] = False
            full_list['opo2'] = False

            # Further pre-process order data with OPO2 first loop
            if self.opo2_first :
                full_list_b4_opo2 = full_list.copy()
                full_list_b4_opo2 = full_list_b4_opo2[full_list_b4_opo2['order_status'] != 'smp'] # exclude SMP orders from OPO2 first to avoid duplicates
                full_list_b4_opo2['condition'] = full_list_b4_opo2['condition'].str.replace(r'_[0-9]+','')
                full_list_b4_opo2['ori_status'] = 'original'
                full_list_b4_opo2 = pd.merge(full_list_b4_opo2, product_density[['Product', 'Density_values']], left_on='Material', right_on='Product')
                full_list_b4_opo2.drop(['Total_weight','terminal_97'], axis=1, inplace=True)
                full_list_b4_opo2 = full_list_b4_opo2.rename(columns = {'terminal_95':'terminal'})

                terminal_list = list(full_list_b4_opo2['terminal'].unique())
                runs = [Preprocessing_Southern(end_day, full_list_b4_opo2[full_list_b4_opo2['terminal']==terminal], master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df, self.opo2_first) for terminal in terminal_list]
                results = []

                with parallel_backend(backend="loky"):
                    delayed_funcs = [delayed(lambda x:x.preprocess_order())(run) for run in runs]
                try :
                    parallel = Parallel(n_jobs=-1, verbose=100)
                    results = parallel(delayed_funcs)
                except Exception as e :
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno, e)
                    parallel = Parallel(n_jobs=4, verbose=100)
                    results = parallel(delayed_funcs)

                ori_order_multiload_opo2 = pd.concat([results[0][0], results[1][0]])
                processed_order_multiload_opo2 = pd.concat([results[0][1], results[1][1]])
                full_list_opo2 = pd.concat([results[0][2], results[1][2]])
                
                ori_order_multiload_opo2['opo2'] = True
                processed_order_multiload_opo2['opo2'] = True
                full_list_opo2['opo2'] = True
                                
                ori_order_multiload = pd.concat([ori_order_multiload, ori_order_multiload_opo2], axis=0)
                processed_order_multiload = pd.concat([processed_order_multiload, processed_order_multiload_opo2], axis=0)
                full_list = pd.concat([full_list, full_list_opo2], axis=0)

            else :
                ori_order_multiload['ori_status'] = 'original'
                processed_order_multiload['ori_status'] = 'original'
                full_list['ori_status'] = 'original'

        elif Config.MODEL_INPUTOUTPUT['model_region'][self.region] == "EASTERN":
            preprocess_eastern = Preprocessing_Eastern(end_day, order_data, master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, commercial_data, unmatched_commercial, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df)

            ori_order_multiload, processed_order_multiload, full_list = preprocess_eastern.preprocess_order()
            ori_order_multiload['opo2'] = False
            processed_order_multiload['opo2'] = False
            full_list['opo2'] = False

            # Further pre-process order data with OPO2 first loop
            if self.opo2_first :
                full_list_b4_opo2 = full_list.copy()
                full_list_b4_opo2 = full_list_b4_opo2[full_list_b4_opo2['order_status'] != 'smp'] # exclude SMP orders from OPO2 first to avoid duplicates
                full_list_b4_opo2['condition'] = full_list_b4_opo2['condition'].str.replace(r'_[0-9]+','')
                full_list_b4_opo2['ori_status'] = 'original'
                full_list_b4_opo2 = pd.merge(full_list_b4_opo2, product_density[['Product', 'Density_values']], left_on='Material', right_on='Product')
                full_list_b4_opo2.drop(['Total_weight','terminal_97'], axis=1, inplace=True)
                full_list_b4_opo2 = full_list_b4_opo2.rename(columns = {'terminal_95':'terminal'})

                preprocess_eastern = Preprocessing_Eastern(end_day, full_list_b4_opo2, master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, commercial_data, unmatched_commercial, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df, opo2_first=True)
                ori_order_multiload_opo2, processed_order_multiload_opo2, full_list_opo2 = preprocess_eastern.preprocess_order()
                ori_order_multiload_opo2['opo2'] = True
                processed_order_multiload_opo2['opo2'] = True
                full_list_opo2['opo2'] = True
                                
                ori_order_multiload = pd.concat([ori_order_multiload, ori_order_multiload_opo2], axis=0)
                processed_order_multiload = pd.concat([processed_order_multiload, processed_order_multiload_opo2], axis=0)
                full_list = pd.concat([full_list, full_list_opo2], axis=0)
            else :
                ori_order_multiload['ori_status'] = 'original'
                processed_order_multiload['ori_status'] = 'original'
                full_list['ori_status'] = 'original'

        elif Config.MODEL_INPUTOUTPUT['model_region'][self.region] == "NORTHERN":
            preprocess_northern = Preprocessing_Northern(end_day, order_data, master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, commercial_data, unmatched_commercial, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df)
            ori_order_multiload, processed_order_multiload, full_list = preprocess_northern.preprocess_order()
            ori_order_multiload['opo2'] = False
            processed_order_multiload['opo2'] = False
            full_list['opo2'] = False

            # Further pre-process order data with OPO2 first loop (not yet run during first loop)
            if self.opo2_first :
                full_list_b4_opo2 = full_list.copy()
                full_list_b4_opo2 = full_list_b4_opo2[full_list_b4_opo2['order_status'] != 'smp'] # exclude SMP orders from OPO2 first to avoid duplicates
                full_list_b4_opo2['condition'] = full_list_b4_opo2['condition'].str.replace(r'_[0-9]+','')
                full_list_b4_opo2['ori_status'] = 'original'
                full_list_b4_opo2 = pd.merge(full_list_b4_opo2, product_density[['Product', 'Density_values']], left_on='Material', right_on='Product')
                full_list_b4_opo2.drop(['Total_weight','terminal_97'], axis=1, inplace=True)
                full_list_b4_opo2 = full_list_b4_opo2.rename(columns = {'terminal_95':'terminal'})

                preprocess_northern = Preprocessing_Northern(end_day, full_list_b4_opo2, master_all, forecast_data, inventory_data_tomorrow, self.sales_time_slot, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, commercial_data, unmatched_commercial, self.post_process, self.multiload_all_tanker, self.normal_all_tanker, master_terminal_data, retail_id_df, self.opo2_first)
                ori_order_multiload_opo2, processed_order_multiload_opo2, full_list_opo2 = preprocess_northern.preprocess_order()

                ori_order_multiload_opo2['opo2'] = True
                processed_order_multiload_opo2['opo2'] = True
                full_list_opo2['opo2'] = True
                                
                ori_order_multiload = pd.concat([ori_order_multiload, ori_order_multiload_opo2], axis=0)
                processed_order_multiload = pd.concat([processed_order_multiload, processed_order_multiload_opo2], axis=0)
                full_list = pd.concat([full_list, full_list_opo2], axis=0)
                
            else :
                ori_order_multiload['ori_status'] = 'original'
                processed_order_multiload['ori_status'] = 'original'
                full_list['ori_status'] = 'original'

        else :
            # print("Wrong Region or Region selected is not in Config : {}. Please check again, system existing ...".format(self.region))
            end_time = datetime.now()
            diff = end_time - self.start_time
            print("Total run time is {}.".format(diff))
            raise Exception("Wrong Region or Region selected is not in Config : {}. Please check again, system existing ...".format(self.region)) 
                
        # Double check if orders count are okay
        print("\n--- Order data to be returned from input_gen --- ")
        print("[InputGeneration] Number of multiload retail orders before processed : {}".format(len(ori_order_multiload)))
        print("[InputGeneration] Full Data - number of normal + multiload retail orders before processed : {}".format(len(full_list)))
        print("[InputGeneration] Full Data - number of commercial orders : {}".format(len(commercial_data)))
        print("[InputGeneration] Full Data - number of unmatched commercial : {}".format(len(unmatched_commercial)))
        print("[InputGeneration] Number of normal + multiload retail orders after processed : {}".format(len(processed_order_multiload)))
        
        if len(ori_order_multiload) > 0:
            ori_order_multiload = ori_order_multiload.rename(columns={"Ship-to":'Ship To','retain_hour':'Remark','Qty':'Quantity','long_distance':'Long_Distance'})
            if self.loop_status.split('_')[1] == "First" and self.opo2_first == False : 
                ori_order_multiload = ori_order_multiload[["Ship To","Priority","Material","Quantity","Remark","Runout","Total_weight","End_Stock_Day","Tank_Status","Long_Distance","Multiload_Status","condition","grouping","order_status","opo2"]]
            elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                ori_order_multiload = ori_order_multiload[["Ship To","Priority","Material","Quantity","Remark","Runout","Total_weight","End_Stock_Day","Tank_Status","Long_Distance","Multiload_Status","condition","grouping","order_status","opo2","ori_status"]]
            else :
                pass

        # pre process to decide the sequence of cloud run
        # 1.0 prioritize cloud with higher number of small orders ( taking quantity for 5460 only), highest percentage over total orders in individual clouds
        groupby_Order = order_data_full.groupby(['Cloud2','Qty']).agg({'Qty' : ['count']}).reset_index()
        groupby_Order.columns = ["_".join(x) for x in groupby_Order.columns.ravel()]
        groupby_Order['cumsum'] = groupby_Order.groupby(['Cloud2_'])['Qty_count'].apply(lambda x: x.cumsum())
        groupby_Order_Total = groupby_Order.groupby(['Cloud2_'])['cumsum'].max().reset_index()

        orderCount_total = pd.merge(groupby_Order[['Cloud2_', 'Qty_', 'Qty_count']], groupby_Order_Total, how='left', on='Cloud2_')
        orderCount_total = orderCount_total[orderCount_total['Qty_']==5460]
        orderCount_total['perct'] = round(orderCount_total['Qty_count']/orderCount_total['cumsum'], 2)
        orderCount_total = orderCount_total.sort_values('perct', ascending=False)
        cloud_ordered = (orderCount_total['Cloud2_'].to_list())
        list_cloud = [x for x in sorted(order_data_full['Cloud2'].unique()) if x != 99]
        for item in list_cloud:
            if not item in cloud_ordered:
                cloud_ordered.append(item)
        
        # 2.0 prioritize cloud with higher number of total orders (based on all clouds)
        count_order = order_data_full.groupby(['Cloud2']).size().reset_index(name='counts')
        count_order = count_order.sort_values('counts', ascending=False)
        count_order['Total'] = np.sum(count_order['counts'])
        count_order['perct'] = count_order['counts']/count_order['Total']
        cloud_ordered = count_order['Cloud2'].to_list()

        # Load truck data from truck master
        print("[InputGeneration] Loading truck data for {} in {}".format(self.loop_status,self.txn_id))
        print("[InputGeneration] Reading truck master from database is being initiated...")
        truck_data, _, _, _ = self.load_truck_data(self.depot_dates, "routing", self.region, self.date, self.order_id_ori, self.capacity, self.largest_capacity, self.normal_all_tanker, self.multiload_all_tanker, self.opo2_first, self.opo2_first_capacity)
        
        # Remove single shift tanker for generate all possible routes under maximum tanker shift period 
        truck_data = truck_data[(truck_data['end_shift'].dt.hour > 12)].reset_index(drop=True)
        print("[InputGeneration] Reading truck master from database has completed...")

        # try dropping any order id in commercial orders before return to route main
        try:
            commercial_data = commercial_data.drop(['id'], axis=1)
        except KeyError:
            pass
        try:
            unmatched_commercial = unmatched_commercial.drop(['id'], axis=1)
        except KeyError:
            pass
        
        return processed_order_multiload, truck_data, cloud_ordered, ori_order_multiload, full_list, master_all, commercial_data, unmatched_commercial, master_terminal_data, self.depot_dates
        

    @classmethod
    def load_truck_data(cls, depot_dates, stage, region, date, order_id_ori, capacity = None, largest_capacity = None, normal_all_tanker = None, multiload_all_tanker = None, opo2_first = None, opo2_first_capacity = None):
        
        if stage == "main" :
            # Read and process terminal dates
            depot_dates = InputHandler.get_terminal_dates(region)

            time_list = ['time_from','time_to']
            for col in time_list :
                depot_dates[col] = change_timedelta_format(depot_dates[col])          
        
        # Subset active and available tankers for the region using sap/awsm status and other terminal mobilization / block date range / shift off
        # Read original active tanker list for the region's terminal from tanker master
        truck_master_region = InputHandler.get_truck_data(region)
        truck_active_region_list = truck_master_region[Config.TABLES['truck_master']["columns"]["vehicle_number"]].unique().ravel().tolist()

        # Read active tanker dates condition
        truck_active_dates = InputHandler.get_truck_dates(region, truck_active_region_list)
        # Flag if tanker dates restriction collides with scheduling day to add/remove those tankers into/from active region tanker list
        truck_active_dates['unavail_flag'] = np.where(truck_active_dates['date_from'] == date + timedelta(days=1), True, np.where(((truck_active_dates['date_from'] < date + timedelta(days=1)) & (truck_active_dates['date_to'] >= date + timedelta(days=1))), True, False))

        # Subset list of tankers serving current region's terminal but mobilized to another region's terminal during scheduling day
        truck_to_other_terminal_list = truck_active_dates[(truck_active_dates['unavail_flag'] == True) & (truck_active_dates['field'] == 'other terminal mobilization') & (truck_active_dates['field'] == 'other terminal mobilization') & (~truck_active_dates['terminal'].isin(Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']))]['road_tanker'].unique().ravel().tolist()

        # Subset list of tankers NOT serving current region's terminal but mobilized from another region's terminal to serve here during scheduling day
        truck_from_other_terminal_list = truck_active_dates[(truck_active_dates['unavail_flag'] == True) & (truck_active_dates['field'] == 'other terminal mobilization') & (~truck_active_dates['default_terminal'].isin(Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']))]['road_tanker'].unique().ravel().tolist()
        
        # Subset list of tankers blocked or shift off during scheduling day 
        truck_block_date_list = truck_active_dates[(truck_active_dates['unavail_flag'] == True) & (truck_active_dates['field'] == 'block date range')]['road_tanker'].unique().ravel().tolist()
        truck_off_list = truck_active_dates[(truck_active_dates['unavail_flag'] == True) & (truck_active_dates['shift_type'] == 'Off')]['road_tanker'].unique().ravel().tolist()

        # Add tankers mobilized from another region's terminal to original active tanker list for the region's terminal
        truck_active_region_list.extend(truck_from_other_terminal_list)
        # Remove tankers mobilized to another region's terminal from above list
        truck_active_region_list = [x for x in truck_active_region_list if x not in truck_to_other_terminal_list]
        # Remove tankers blocked from above list
        truck_active_region_list = [x for x in truck_active_region_list if x not in truck_block_date_list]
        # Remove tankers shift off from above list
        truck_active_region_list = [x for x in truck_active_region_list if x not in truck_off_list]

        # Read updated active tanker list from tanker master 
        truck_master_region = InputHandler.get_truck_data(None, truck_active_region_list)

        # Replace terminal of tankers mobilized from another region to current terminal if there's one
        truck_from_other_terminal = truck_active_dates[(truck_active_dates['unavail_flag'] == True) & (truck_active_dates['field'] == 'other terminal mobilization') & (~truck_active_dates['default_terminal'].isin(Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']))]
        if len(truck_from_other_terminal) > 0 :
            truck_master_region = pd.merge(truck_master_region, truck_from_other_terminal[['road_tanker','terminal']].drop_duplicates(), 'left', left_on = ['vehicle'], right_on = ['road_tanker'])
            truck_master_region['default_terminal'] = np.where((truck_master_region['vehicle'].isin(truck_from_other_terminal['road_tanker'].unique())), truck_master_region['terminal'], truck_master_region['default_terminal'])
            truck_master_region.drop(columns=['road_tanker','terminal'],inplace=True)

        # Remove tanker where the capacity is not in respectice region default configuration (ie. ODD compartment)
        truck_capacity_check = Config.MODEL_INPUTOUTPUT['truck_capacity'][region]
        truck_master_region = truck_master_region[truck_master_region['max_volume'].isin(truck_capacity_check)]

        truck_master_region = truck_master_region.rename(columns = {"vehicle":"Vehicle Number",
                                                                    "daily_available_hours":"balance_shift",
                                                                    "max_volume":"Vehicle maximum vol.",
                                                                    "unladen_weight":"Vehicle unl.weight",
                                                                    "max_weight":"Vehicle max.weight"
                                                                    })  

        truck_master_region['start_shift'] = truck_master_region.apply(lambda x: replace_start_shift_time(x, depot_dates), axis=1)
        truck_master_region['end_shift'] = np.where(truck_master_region['shift_type'] == 'Double', "23:59:00", "20:00:00")
        truck_data = truck_master_region.copy()

        truck_shift_df = truck_active_dates[(truck_active_dates['unavail_flag'] == True) & (truck_active_dates['field'] == 'shift type')]
        truck_shift_df = truck_shift_df[truck_shift_df['road_tanker'].isin(truck_active_region_list)]

        # Change start and end shift if there's UI tanker gann chart status changes for the delivery date to override default tanker master data
        if len(truck_shift_df) > 0 :
            time_list = ['time_from','time_to']
            for col in time_list :
                truck_shift_df[col] = truck_shift_df.apply(lambda x: change_timedelta_format(x[[col]]) if x['shift_type'] != 'OH' else x[[col]], axis=1)

            # Adjust start and end shift based on On1 and On2
            truck_data = truck_data.merge(truck_shift_df[['road_tanker','shift_type','time_from','time_to']], left_on='Vehicle Number',right_on='road_tanker', how='left')
        
            truck_data['shift_type_x'] = np.where(truck_data['shift_type_y'].isnull(), truck_data['shift_type_x'], truck_data['shift_type_y'])
            truck_data['start_shift'] = np.where((truck_data['shift_type_x'] == 'On1') | (truck_data['shift_type_x'] == 'On2') , truck_data['time_from'], truck_data['start_shift'])
            truck_data['end_shift'] = np.where((truck_data['shift_type_x'] == 'On1') | (truck_data['shift_type_x'] == 'On2') , truck_data['time_to'], truck_data['end_shift'])

            truck_data['start_shift'] = np.where(truck_data['shift_type_x'] == 'OH', '08:00:00', truck_data['start_shift'])
            truck_data['end_shift'] = np.where(truck_data['shift_type_x'] == 'OH', '20:00:00', truck_data['end_shift'])

            truck_data = truck_data.rename(columns = {'shift_type_x':'shift_type'})
            truck_data = truck_data.drop(['shift_type_y','time_from','time_to','road_tanker'], axis=1)

        # Reformat start and end shift to YYYY-MM-DD HH:MM:SS
        time_list = ['start_shift','end_shift']
        for col in time_list :
            truck_data[col] = change_strtodatetime_format(truck_data[col], date + timedelta(days=1))         
        truck_data['start_shift'] = pd.to_datetime(truck_data['start_shift'], format = "%Y-%m-%d %H:%M:%S")
        truck_data['end_shift'] = pd.to_datetime(truck_data['end_shift'], format = "%Y-%m-%d %H:%M:%S")

        if capacity != None :
            if multiload_all_tanker == True and capacity != largest_capacity : 
                #truck_data = truck_data[~truck_data['Vehicle maximum vol.'].isin([16380,10920])] 
                truck_data = truck_data[truck_data['Vehicle maximum vol.'].isin(capacity)]
            elif normal_all_tanker == True and capacity != largest_capacity : 
                #truck_data = truck_data[~truck_data['Vehicle maximum vol.'].isin([16380,10920])]
                truck_data = truck_data[truck_data['Vehicle maximum vol.'].isin(capacity)]               
            else : 
                truck_data = truck_data[truck_data['Vehicle maximum vol.'].isin([capacity])]
        
        if stage != "main" : 
            # Give score to tanker capacity based on number of tanker population for respective capacity
            truck_rate = pd.DataFrame(truck_data.groupby('Vehicle maximum vol.').size().to_frame('Count').reset_index())
            truck_rate['Score'] = np.ceil(100 * truck_rate['Count']  / truck_rate['Count'].sum()).astype('int')

            if opo2_first :
                truck_rate['Score'][truck_rate['Vehicle maximum vol.'].isin(opo2_first_capacity)] =  truck_rate['Score']*10

            truck_data = pd.merge(truck_data, truck_rate[['Vehicle maximum vol.', 'Score']], 'left', on = 'Vehicle maximum vol.')

     
        if truck_data.empty == True:
            raise RuntimeError('Empty tanker dataframe')

        else :
            # Read locked/pre-shipment orders
            _ , locked_orders = InputHandler.get_locked_orders(order_id_ori, region, date)            

            # Calculate tanker utilization time of locked orders and remove overutilized tanker
            if len(locked_orders) > 0 :

                locked_order_toggle = True
                truck_ori_shift = truck_data.drop_duplicates(subset=['Vehicle Number','balance_shift'], keep="first")

                time_list = ['start_shift','end_shift']
                for col in time_list :            
                    truck_ori_shift[col] = change_datetimetofloat_format(truck_ori_shift[col])
                truck_ori_shift['balance_shift'] = truck_ori_shift['end_shift'] - truck_ori_shift['start_shift']                 

                locked_orders = locked_orders[locked_orders['Vehicle Number'].isin(truck_ori_shift['Vehicle Number'].unique())]
                locked_orders['Time complete route'] = np.where(locked_orders['Time complete route'].dt.date > (date + timedelta(days=1)).date(), np.datetime64(str((date + timedelta(days=1)).date()) + ' ' + '23:59:00'), locked_orders['Time complete route'])
                locked_orders = pd.merge(locked_orders, truck_ori_shift[['Vehicle Number','balance_shift']], 'left', on=['Vehicle Number'])  
                time_list = ['Time start route','Time complete route']
                for col in time_list :            
                    locked_orders[col] = change_datetimetofloat_format(locked_orders[col])
                locked_orders['Balance'] = locked_orders['Time complete route'] - locked_orders['Time start route']

                # Calculate the remaining tanker shift balance after usage for locked orders
                tanker_balance = locked_orders.copy()                
                tanker_balance['Balance'] = tanker_balance['Balance'].groupby(tanker_balance['Vehicle Number']).transform('sum')
                tanker_balance = tanker_balance.drop_duplicates(subset=['Vehicle Number','Balance'], keep="first")
                tanker_balance['Balance'] = tanker_balance['balance_shift'] - tanker_balance['Balance']

                # Remove overutilized tanker
                overutil_tanker = list(set(tanker_balance[tanker_balance['Balance'] < 0]['Vehicle Number']))
                if len(overutil_tanker) > 0 :
                    truck_data = truck_data[~truck_data['Vehicle Number'].isin(overutil_tanker)]
                    
                    locked_orders = locked_orders[~locked_orders['Vehicle Number'].isin(overutil_tanker)]
                    if locked_orders.empty == True :
                        locked_order_toggle = False
                 
            else :
                locked_order_toggle = False
                locked_orders = pd.DataFrame()

            if stage == "scheduling" :
                # Reformat depot start and close time to YYYY-MM-DD HH:MM:SS
                time_list = ['time_from','time_to']
                for col in time_list :
                    depot_dates[col] = change_strtodatetime_format(depot_dates[col], date + timedelta(days=1))         
                depot_dates['time_from'] = pd.to_datetime(depot_dates['time_from'], format = "%Y-%m-%d %H:%M:%S")
                depot_dates['time_to'] = pd.to_datetime(depot_dates['time_to'], format = "%Y-%m-%d %H:%M:%S")

                # Reformat depot start and close time to XX.XX hour
                time_list = ['time_from','time_to']
                for col in time_list :            
                    depot_dates[col] = change_datetimetofloat_format(depot_dates[col])                

                # Reformat shift start and end time to XX.XX hour
                time_list = ['start_shift','end_shift']
                for col in time_list :            
                    truck_data[col] = change_datetimetofloat_format(truck_data[col])
                truck_data['balance_shift'] = truck_data['end_shift'] - truck_data['start_shift']

                truck_data = pd.merge(truck_data, depot_dates[['terminal','time_from','time_to']], 'left', left_on=['default_terminal'], right_on=['terminal'])
                truck_data.drop(['terminal'], axis=1, inplace=True)
                truck_data = truck_data.rename(columns={'time_from':'depot_start_time','time_to':'depot_close_time'})

            elif stage == "main" :
                truck_data['balance_shift'] = (truck_data['end_shift'] - truck_data['start_shift']).dt.total_seconds()/3600

            else :
                pass         

            truck_capacity = truck_data['Vehicle maximum vol.'].unique()
         
        return truck_data, truck_capacity, locked_orders, locked_order_toggle


    def scheduling_input(self, locked_orders = None):
        locked_order_toggle = False
        print("[InputGeneration] Loading route output data for {} in {} from DB is being initiated...".format(self.loop_status,self.txn_id))
        order_data = InputHandler.get_route_output_data(self.txn_id, self.loop_status)
        print("[InputGeneration] Loading route output data for {} in {} from DB has completed...".format(self.loop_status,self.txn_id))

        def adjust_retain_w_restriction(station_restriction, order_data, max_station_end_time):
            """
            This function re-adjust min retain hour and max runout of orders with respect to no delivery intervals 
            """

            time_list = ['Opening_Time','Closing_Time','no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
            for col in time_list :
                station_restriction[col] = change_strtodatetime_format(station_restriction[col], self.date + timedelta(days=1))  
                                            
            time_list = ['no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
            for col in time_list :            
                station_restriction[col] = change_datetimetofloat_format(station_restriction[col]) 

            station_restriction = station_restriction.rename(columns = {"Ship To":"Destinations", "product":"Product"})

            # tag all stations with NO (NaN) start and end no delivery interval to one as flag      
            station_restriction[['no_delivery_start_1', 'no_delivery_end_1','no_delivery_start_2', 'no_delivery_end_2']] =  station_restriction[['no_delivery_start_1', 'no_delivery_end_1','no_delivery_start_2', 'no_delivery_end_2']].fillna(pd.Timedelta(hours=1))                

            # convert all stations that has zero start and end no delivery interval to one as flag
            station_restriction['no_delivery_start_1'] = np.where((station_restriction['no_delivery_start_1'] == 0.0) & (station_restriction['no_delivery_end_1'] == 0.0), 1.0, station_restriction['no_delivery_start_1'])
            station_restriction['no_delivery_end_1'] = np.where((station_restriction['no_delivery_start_1'] == 1.0) & (station_restriction['no_delivery_end_1'] == 0.0), 1.0, station_restriction['no_delivery_end_1'])
            station_restriction['no_delivery_start_2'] = np.where((station_restriction['no_delivery_start_2'] == 0.0) & (station_restriction['no_delivery_end_2'] == 0.0), 1.0, station_restriction['no_delivery_start_2'])
            station_restriction['no_delivery_end_2'] = np.where((station_restriction['no_delivery_start_2'] == 1.0) & (station_restriction['no_delivery_end_2'] == 0.0), 1.0, station_restriction['no_delivery_end_2'])

            # convert all stations that has no delivery interval starts at 12am (0.0 hour) to 1.1 for differantiation from no delivery interval of value 1 like above
            station_restriction['no_delivery_start_1'] = np.where(station_restriction['no_delivery_start_1'] == 0.0, 1.1, station_restriction['no_delivery_start_1'])
            station_restriction['no_delivery_end_1'] = np.where(station_restriction['no_delivery_end_1'] == 0.0, 1.1, station_restriction['no_delivery_end_1'])
            station_restriction['no_delivery_start_2'] = np.where(station_restriction['no_delivery_start_2'] == 0.0, 1.1, station_restriction['no_delivery_start_2'])
            station_restriction['no_delivery_end_2'] = np.where(station_restriction['no_delivery_end_2'] == 0.0, 1.1, station_restriction['no_delivery_end_2'])
            
            order_data = pd.merge(order_data, station_restriction[['Destinations',"Product","no_delivery_start_1","no_delivery_end_1","no_delivery_start_2","no_delivery_end_2"]], 'left', on = ['Destinations','Product'])

            # get the minimum between max station end time of the day vs min runout of route if the runout date is same as delivery date
            max_station_end_time = datetime.combine(self.date + timedelta(days=1), pd.to_datetime(max_station_end_time).time())
            order_data['Min_Runout'] = np.where(order_data['Min_Runout'] > max_station_end_time, round(max_station_end_time.hour + (max_station_end_time.minute/60), 2), round(order_data['Min_Runout'].dt.hour + order_data['Min_Runout'].dt.minute/60, 2))
            order_data['Min_Retain_Hour'] = round(order_data['Min_Retain_Hour'].dt.hour + order_data['Min_Retain_Hour'].dt.minute/60, 2)

            # form additional route with early windows for retain before and runout later than no delivery 1 and form additional route with late windows for runout later than no delivery 2
            order_duplicate = order_data[(order_data['no_delivery_end_1'] != 1.0) & (order_data['no_delivery_end_2'] != 1.0)]
            order_duplicate = order_duplicate[order_duplicate['no_delivery_end_2'] != 1.0]
            order_duplicate = order_duplicate[order_duplicate['no_delivery_start_1'] != 1.0]
            
            if len(order_duplicate) > 0 :

                order_duplicate_1 = order_duplicate[(order_duplicate['Min_Retain_Hour'] < order_duplicate['no_delivery_start_1']) & (order_duplicate['Min_Runout'] >= order_duplicate['no_delivery_start_1'])]
                order_duplicate_2 = order_duplicate[(order_duplicate['Min_Retain_Hour'] <= order_duplicate['no_delivery_end_2']) & (order_duplicate['Min_Runout'] > order_duplicate['no_delivery_end_2'])]

                order_dub_route_1 = order_duplicate_1['Route_ID'].unique()
                order_dub_route_2 = order_duplicate_2['Route_ID'].unique()

                order_dup_1 = order_data[order_data['Route_ID'].isin(order_dub_route_1)]
                order_dup_2 = order_data[order_data['Route_ID'].isin(order_dub_route_2)]
        
                order_duplicate_1['no_delivery_start_1_new'] = order_duplicate_1.groupby('Route_ID')['Min_Runout'].transform('first')
                order_duplicate_1['no_delivery_end_1_new'] = order_duplicate_1.groupby('Route_ID')['Min_Retain_Hour'].transform('first')
                order_duplicate_1['no_delivery_start_2_new'] = order_duplicate_1.groupby('Route_ID')['no_delivery_start_1'].transform('min')
                order_duplicate_1['no_delivery_end_2_new'] = order_duplicate_1.groupby('Route_ID')['Min_Runout'].transform('first')
                
                order_duplicate_2['no_delivery_start_1_new'] = order_duplicate_2.groupby('Route_ID')['Min_Retain_Hour'].transform('first')
                order_duplicate_2['no_delivery_end_1_new'] = order_duplicate_2.groupby('Route_ID')['no_delivery_end_2'].transform('max')
                order_duplicate_2['no_delivery_start_2_new'] = order_duplicate_2.groupby('Route_ID')['Min_Runout'].transform('first')
                order_duplicate_2['no_delivery_end_2_new'] = order_duplicate_2.groupby('Route_ID')['Min_Retain_Hour'].transform('first')

                order_dup_1 = pd.merge(order_dup_1, order_duplicate_1[['Route_ID','no_delivery_start_1_new','no_delivery_end_1_new','no_delivery_start_2_new','no_delivery_end_2_new']].drop_duplicates(), 'left', on=['Route_ID']) 
                order_dup_2 = pd.merge(order_dup_2, order_duplicate_2[['Route_ID','no_delivery_start_1_new','no_delivery_end_1_new','no_delivery_start_2_new','no_delivery_end_2_new']].drop_duplicates(), 'left', on=['Route_ID']) 

                order_dup_1['Route_ID'] = order_dup_1['Route_ID'].astype(str) + '_' + '1'
                order_dup_2['Route_ID'] = order_dup_2['Route_ID'].astype(str) + '_' + '2'

                order_duplicate = pd.concat([order_dup_1, order_dup_2], axis=0)
            
            else :
                order_duplicate = pd.DataFrame()


            # convert all station that has zero no delivery interval into min retain & min runout
            order_data['no_delivery_end_1'] = np.where((order_data['no_delivery_end_1'] == 1.0) & (order_data['no_delivery_end_2'] == 1.0), order_data['Min_Retain_Hour'], order_data['no_delivery_end_1'])
            order_data['no_delivery_start_1'] = np.where((order_data['no_delivery_start_1'] == 1.0) & (order_data['no_delivery_start_2'] == 1.0), order_data['Min_Runout'], order_data['no_delivery_start_1'])
            order_data['no_delivery_end_2'] = np.where((order_data['no_delivery_end_1'] == order_data['Min_Retain_Hour']) & (order_data['no_delivery_end_2'] == 1.0), order_data['Min_Retain_Hour'], order_data['no_delivery_end_2'])
            order_data['no_delivery_start_2'] = np.where((order_data['no_delivery_start_1'] == order_data['Min_Runout']) & (order_data['no_delivery_start_2'] == 1.0), order_data['Min_Runout'], order_data['no_delivery_start_2'])

            # move station no delivery interval 1 that has value to no delivery interval 2 which has one flag (zero interval)
            order_data['no_delivery_start_2'] = np.where((order_data['no_delivery_start_2'] == 1.0) & (order_data['no_delivery_start_1'] > 1.0), order_data['no_delivery_start_1'], order_data['no_delivery_start_2'])
            order_data['no_delivery_end_2'] = np.where((order_data['no_delivery_end_2'] == 1.0) & (order_data['no_delivery_end_1'] > 1.0), order_data['no_delivery_end_1'], order_data['no_delivery_end_2'])
            order_data['no_delivery_start_1'] = np.where((order_data['no_delivery_start_1'] == order_data['no_delivery_start_2']), 1.0, order_data['no_delivery_start_1'])
            order_data['no_delivery_end_1'] = np.where((order_data['no_delivery_end_1'] == order_data['no_delivery_end_2']), 1.0, order_data['no_delivery_end_1'])

            # readjust min retain and min runout so to avoid no delivery interval 1 & 2 then finalize min retain to be at no delivery end 1 and min runout to be at no delivery start 2
            # order_data['no_delivery_start_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_start_1'])
            # order_data['no_delivery_end_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_end_1'])

            # order_data['no_delivery_start_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_end_2']), order_data['no_delivery_end_2'], order_data['no_delivery_start_1_new'])
            # order_data['no_delivery_end_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_end_2']), order_data['no_delivery_end_2'], order_data['no_delivery_end_1_new'])

            # order_data['no_delivery_start_1_new'] = np.where((order_data['no_delivery_start_1'] == 1.0) & (order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']), order_data['no_delivery_end_2'], order_data['no_delivery_start_1_new'])
            # order_data['no_delivery_end_1_new'] = np.where((order_data['no_delivery_end_1'] == 1.0) & (order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']), order_data['no_delivery_end_2'], order_data['no_delivery_end_1_new'])

            # order_data['no_delivery_end_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_end_1_new'])
            # order_data['no_delivery_start_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_start_1_new'])
            
            # order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_start_2'])
            # order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_end_2'])

            # order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_start_2_new'])
            # order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_end_2_new'])

            # order_data['no_delivery_start_2_new'] = np.where((order_data['no_delivery_end_1'] == order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_start_2_new'])
            # order_data['no_delivery_end_2_new'] = np.where((order_data['no_delivery_start_1'] == order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_end_2_new'])

            # order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']) & (order_data['Min_Runout'] >= order_data['no_delivery_start_2']), order_data['no_delivery_start_2'], order_data['no_delivery_start_2_new'])
            # order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']) & (order_data['Min_Runout'] >= order_data['no_delivery_start_2']), order_data['no_delivery_end_2'], order_data['no_delivery_end_2_new'])

            # order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']) & (order_data['Min_Runout'] < order_data['no_delivery_start_2']), order_data['Min_Runout'], order_data['no_delivery_start_2_new'])
            # order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']) & (order_data['Min_Runout'] < order_data['no_delivery_start_2']), order_data['Min_Runout'], order_data['no_delivery_end_2_new'])

            # order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] < order_data['no_delivery_end_1']) & (order_data['no_delivery_start_2'] != 1.0) & (order_data['Min_Runout'] >= order_data['no_delivery_start_2']), order_data['no_delivery_start_2'], order_data['no_delivery_start_2_new'])
            # order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] < order_data['no_delivery_end_1']) & (order_data['no_delivery_start_2'] != 1.0) & (order_data['Min_Runout'] >= order_data['no_delivery_start_2']), order_data['no_delivery_end_2'], order_data['no_delivery_end_2_new'])

            # order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] < order_data['no_delivery_end_1']) & (order_data['no_delivery_start_2'] != 1.0) & (order_data['Min_Runout'] < order_data['no_delivery_start_2']), order_data['Min_Runout'], order_data['no_delivery_start_2_new'])
            # order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] < order_data['no_delivery_end_1']) & (order_data['no_delivery_start_2'] != 1.0) & (order_data['Min_Runout'] < order_data['no_delivery_start_2']), order_data['Min_Runout'], order_data['no_delivery_end_2_new'])

            order_data['no_delivery_start_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_start_1'])
            order_data['no_delivery_end_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_end_1'])

            order_data['no_delivery_start_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_end_2']), order_data['no_delivery_end_2'], order_data['no_delivery_start_1_new'])
            order_data['no_delivery_end_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_end_2']), order_data['no_delivery_end_2'], order_data['no_delivery_end_1_new'])

            order_data['no_delivery_start_1_new'] = np.where((order_data['no_delivery_start_1'] == 1.0) & (order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']), order_data['no_delivery_end_2'], order_data['no_delivery_start_1_new'])
            order_data['no_delivery_end_1_new'] = np.where((order_data['no_delivery_end_1'] == 1.0) & (order_data['Min_Retain_Hour'] >= order_data['no_delivery_start_2']), order_data['no_delivery_end_2'], order_data['no_delivery_end_1_new'])

            order_data['no_delivery_end_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_end_1_new'])
            order_data['no_delivery_start_1_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Retain_Hour'], order_data['no_delivery_start_1_new'])
            
            order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_start_2'])
            order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_end_2'])

            order_data['no_delivery_start_2_new'] = np.where((order_data['no_delivery_end_1'] == order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_start_2_new'])
            order_data['no_delivery_end_2_new'] = np.where((order_data['no_delivery_start_1'] == order_data['no_delivery_end_2']), order_data['Min_Runout'], order_data['no_delivery_end_2_new'])

            order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']), order_data['no_delivery_start_2'], order_data['no_delivery_start_2_new'])
            order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] >= order_data['no_delivery_end_1']) & (order_data['Min_Retain_Hour'] <= order_data['no_delivery_start_2']), order_data['no_delivery_end_2'], order_data['no_delivery_end_2_new'])

            order_data['no_delivery_start_2_new'] = np.where((order_data['Min_Retain_Hour'] < order_data['no_delivery_end_1']) & (order_data['no_delivery_start_2'] != 1.0), order_data['no_delivery_start_2'], order_data['no_delivery_start_2_new'])
            order_data['no_delivery_end_2_new'] = np.where((order_data['Min_Retain_Hour'] < order_data['no_delivery_end_1']) & (order_data['no_delivery_start_2'] != 1.0), order_data['no_delivery_end_2'], order_data['no_delivery_end_2_new'])

            order_data = pd.concat([order_data, order_duplicate], axis=0)
            order_data = order_data.drop(['no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1'], axis = 1)

            order_data = order_data.rename(columns = {"no_delivery_start_2_new":"no_delivery_start_2",
                                                      "no_delivery_start_1_new":"no_delivery_start_1",
                                                      "no_delivery_end_2_new":"no_delivery_end_2",
                                                      "no_delivery_end_1_new":"no_delivery_end_1"})            

            # Readjust min runout to be min station closing time of route if later than the closing time 
            order_data['no_delivery_start_2'] = np.where((order_data['no_delivery_start_2'] > order_data['Closing_Time']), order_data['Closing_Time'], order_data['no_delivery_start_2'])
            mapper = order_data.groupby('Route_ID').agg({'no_delivery_start_2': min})[['no_delivery_start_2']].reset_index()
            order_data['no_delivery_start_2'] = order_data['Route_ID'].map(mapper.set_index('Route_ID')['no_delivery_start_2'])

            station_restriction = station_restriction.rename(columns = {"Ship To":"Destinations",
                                                                        "no_delivery_start_2":"start_2",
                                                                        "no_delivery_start_1":"start_1",
                                                                        "no_delivery_end_2":"end_2",
                                                                        "no_delivery_end_1":"end_1"})

            order_data = pd.merge(order_data, station_restriction[['Destinations', 'Product', 'start_1', 'end_1',  'start_2', 'end_2']], 'left', on = ['Destinations', 'Product'])
            
            order_data_upload = order_data[['Route_ID', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID', 'Destinations', 'Product', 'Order_Quantity', 'Opening_Time', 'Closing_Time', 'Priority', 'Utilization', 'Retain_Hour', 'Runout', 'no_delivery_end_1', 'no_delivery_start_2', 'End_Stock_Day', 'Travel_Time', 'Accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud', 'Min_Retain_Hour', 'Min_Runout', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Txn_ID', 'Region', 'Order_Type', 'delta', 'Strategy', 'Filter']]
            order_data_upload = convert_time_interval('no_delivery_end_1', 'no_delivery_start_2', order_data_upload, self.date)
            order_data_upload['Destinations_2'] = order_data_upload['Destinations'].astype(str) + '_' + order_data_upload['grouping'].astype(str)
            try :
                DBConnector.save(order_data_upload, Config.TABLES['awsm_scheduling_timewindows']["tablename"], if_exists='append')
                print("[InputGeneration] Scheduling No Delivery Interval Compliace logs for {} in {} successfully pushed to DB.".format(self.loop_status, self.txn_id))
            except Exception as err :
                print("[InputGeneration] Scheduling No Delivery Interval Compliace logs for {} in {} failed to push to DB due to {}.".format(self.loop_status, self.txn_id, err))    

            return order_data                               

        # Seperate commercial orders for pre-process delivery windows of locations
        if len(order_data[order_data['Order_Type'] == "CO"]) > 0 :
            commercial_data = order_data[order_data['Order_Type'] == "CO"]

            # Read and process commercial master data
            comm_master = InputHandler.get_commercial_master(self.region, self.date)            

            # Read and process commercial restriction
            commercial_active_region_list = commercial_data['Destinations'].unique().ravel().tolist() 
            commercial_restriction = InputHandler.get_commercial_dates(commercial_active_region_list)
            time_list = ['Opening_Time','Closing_Time','no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
            for col in time_list :
                commercial_restriction[col] = change_timedelta_format(commercial_restriction[col])

            # Merge product and terminal into station no delivery interval for different product taking from different terminal scenarios, hence will have different opening times
            commercial_restriction = commercial_restriction.rename(columns={'commercial':'Destinations'})
            commercial_restriction = pd.merge(commercial_restriction, commercial_data[['Destinations','Product','Terminal_Default']], 'left', on = 'Destinations')
            commercial_restriction = commercial_restriction.drop_duplicates(subset=['Destinations','Product','Terminal_Default'],keep="first")
            commercial_restriction = commercial_restriction.rename(columns = {'Destinations':'Ship To','Terminal_Default':'terminal'})
                         
            # To replace commercial opening time 00:00:00 with terminal opening time and closing time 00:00:00 with min location closing time 
            # commercial_terminal_merge = comm_master[[Config.TABLES['commercial_customer_master']["columns"]["acc"], 'terminal']].drop_duplicates()
            commercial_restriction['Opening_Time'] = commercial_restriction.apply(lambda x: replace_opening_time_scheduling(x, self.depot_dates), axis=1)
            max_station_end_time = str(max(commercial_restriction['Closing_Time']))
            commercial_restriction['Closing_Time'] = np.where(commercial_restriction['Closing_Time'].astype(str) == '24:00:00', '23:59:00', commercial_restriction['Closing_Time'].astype(str)) 
            commercial_restriction['Closing_Time'] = np.where(commercial_restriction['Closing_Time'].astype(str) == '00:00:00', max_station_end_time, commercial_restriction['Closing_Time'].astype(str))            

            # Re-adjust min retain hour and max runout of orders with respect to no delivery intervals
            commercial_data = adjust_retain_w_restriction(commercial_restriction, commercial_data, max_station_end_time)

        else :
            print("[InputGeneration] No commercial route in {}".format(self.loop_status))            
            commercial_data = pd.DataFrame()
        
        # Seperate retail orders for pre-process delivery windows of stations
        if len(order_data[order_data['Order_Type'] == "RE"]) > 0 :
            order_data = order_data[order_data['Order_Type'] == "RE"]
                            
            # Read and process station restriction
            retail_active_region_list = order_data['Destinations'].unique().ravel().tolist()
            retail_active_region_list = [str(station).zfill(10) for station in retail_active_region_list]
            station_restriction = InputHandler.get_retail_dates(retail_active_region_list)
            time_list = ['Opening_Time','Closing_Time','no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
            for col in time_list :
                station_restriction[col] = change_timedelta_format(station_restriction[col])

            # Merge product and terminal into station no delivery interval for different product taking from different terminal scenarios, hence will have different opening times
            station_restriction = pd.merge(station_restriction, self.retail_data[['retail','product','terminal']], 'left', on = 'retail')
            station_restriction = station_restriction.drop_duplicates(subset=['retail','product','terminal'],keep="first")
            station_restriction = station_restriction.rename(columns={'retail': 'Ship To'})                
                        
            # To replace station opening time 00:00:00 with terminal opening time and closing time 00:00:00 with max station closing time 
            # retail_terminal_merge = self.retail_master[[Config.TABLES['retail_customer_master']["columns"]["acc2"], 'terminal', 'road_tanker_requirement']].drop_duplicates()
            station_restriction['Opening_Time'] = station_restriction.apply(lambda x: replace_opening_time_scheduling(x, self.depot_dates), axis=1)
            max_station_end_time = str(max(station_restriction['Closing_Time'])) 
            station_restriction['Closing_Time'] = np.where(station_restriction['Closing_Time'].astype(str) == '24:00:00', '23:59:00', station_restriction['Closing_Time'].astype(str)) 
            station_restriction['Closing_Time'] = np.where(station_restriction['Closing_Time'].astype(str) == '00:00:00', max_station_end_time, station_restriction['Closing_Time'].astype(str))            

            # Re-adjust min retain hour and max runout of orders with respect to no delivery intervals
            order_data = adjust_retain_w_restriction(station_restriction, order_data, max_station_end_time)

        else :
            print("[InputGeneration] No retail route in {}".format(self.loop_status))
            order_data = pd.DataFrame()
        
        if self.loop_status.split('_')[1] == "First" :
            iteration = None
            if self.capacity == self.largest_capacity and self.multiload_all_tanker == True and self.post_process == False :
                iteration = "sequential_after_multiload_first"
                previous_loop = "AllCap_Second"
            elif self.capacity == self.largest_capacity and self.normal_all_tanker == True and self.post_process == False :
                iteration = "sequential_after_all_cap_first"
                previous_loop = "AllCap_Second"            
            elif self.capacity == self.largest_capacity and self.multiload_all_tanker == False and self.normal_all_tanker == False and self.post_process == False :
                iteration = "sequential_first"
            elif self.multiload_all_tanker == False and self.normal_all_tanker == False and self.post_process == True :
                iteration = "sequential_postprocess_first"
                previous_loop = f"{self.capacity}_Second"
            elif self.multiload_all_tanker == True and self.capacity != self.largest_capacity :
                iteration = "multiload_first"
            elif self.normal_all_tanker == True and self.capacity != self.largest_capacity :
                iteration = "all_cap_first"
            elif self.normal_all_tanker == False and self.multiload_all_tanker == False and self.all_cap_toggle == True : 
                iteration = "allcap_first_intermediate_sequential_loop"
                previous_loop = "AllCap_Second"
            else :
                iteration = "sequential_first_intermediate_sequential_loop"
                # if self.region == "Central" :
                #     if self.capacity != 21840:
                #         previous_capacity = self.capacity + 5460
                #     else:
                #         previous_capacity = self.capacity + 10920
                # if self.region == "Southern" :
                #     if self.capacity == 43680 :
                #         previous_capacity = self.capacity + 10920
                #     elif self.capacity == 32760 :
                #         previous_capacity = self.capacity + 10920
                #     else :
                #         previous_capacity = self.capacity + 5460
                # previous_loop = f"{previous_capacity}_Second"


            if iteration == "sequential_after_multiload_first" or iteration == "sequential_after_all_cap_first" or iteration == "allcap_first_intermediate_sequential_loop" and self.capacity != 16380 and self.capacity != 10920 :
                print("[InputGeneration] [{}] Loading updated tanker data for {} in {} from DB is being initiated...".format(iteration, self.loop_status,self.txn_id))
                truck_data, truck_capacity = InputHandler.get_updated_truck_data(self.txn_id, previous_loop, self.capacity)
                if len(locked_orders) > 0 :
                    locked_order_toggle = True
                
                # Reformat depot start and close time to YYYY-MM-DD HH:MM:SS
                time_list = ['time_from','time_to']
                for col in time_list :
                    self.depot_dates[col] = change_strtodatetime_format(self.depot_dates[col], self.date + timedelta(days=1))         
                self.depot_dates['time_from'] = pd.to_datetime(self.depot_dates['time_from'], format = "%Y-%m-%d %H:%M:%S")
                self.depot_dates['time_to'] = pd.to_datetime(self.depot_dates['time_to'], format = "%Y-%m-%d %H:%M:%S")

                # Reformat depot start and close time to XX.XX hour
                time_list = ['time_from','time_to']
                for col in time_list :            
                    self.depot_dates[col] = change_datetimetofloat_format(self.depot_dates[col])         

                truck_data = pd.merge(truck_data, self.depot_dates[['terminal','time_from','time_to']], 'left', left_on=['default_terminal'], right_on=['terminal'])
                truck_data.drop(['terminal'], axis=1, inplace=True)
                truck_data = truck_data.rename(columns={'time_from':'depot_start_time','time_to':'depot_close_time'})                    
                print("[InputGeneration] [{}] Loading updated {} tanker data for {} in {} from DB has completed...".format(iteration, truck_capacity, self.loop_status,self.txn_id))
            
            elif iteration == "sequential_postprocess_first" : 
                print("[InputGeneration] [{}] Loading updated tanker data for {} in {} from DB is being initiated...".format(iteration, self.loop_status,self.txn_id))
                truck_data, truck_capacity = InputHandler.get_updated_truck_data(self.txn_id, previous_loop, self.capacity)
                if len(locked_orders) > 0 :
                    locked_order_toggle = True

                # Reformat depot start and close time to YYYY-MM-DD HH:MM:SS
                time_list = ['time_from','time_to']
                for col in time_list :
                    self.depot_dates[col] = change_strtodatetime_format(self.depot_dates[col], self.date + timedelta(days=1))         
                self.depot_dates['time_from'] = pd.to_datetime(self.depot_dates['time_from'], format = "%Y-%m-%d %H:%M:%S")
                self.depot_dates['time_to'] = pd.to_datetime(self.depot_dates['time_to'], format = "%Y-%m-%d %H:%M:%S")

                # Reformat depot start and close time to XX.XX hour
                time_list = ['time_from','time_to']
                for col in time_list :            
                    self.depot_dates[col] = change_datetimetofloat_format(self.depot_dates[col])         

                truck_data = pd.merge(truck_data, self.depot_dates[['terminal','time_from','time_to']], 'left', left_on=['default_terminal'], right_on=['terminal'])
                truck_data.drop(['terminal'], axis=1, inplace=True)
                truck_data = truck_data.rename(columns={'time_from':'depot_start_time','time_to':'depot_close_time'})                     
                print("[InputGeneration] [{}] Loading updated {} tanker data for {} in {} from DB has completed...".format(iteration, truck_capacity, self.loop_status,self.txn_id))
            
            elif iteration == "sequential_first" or iteration == "multiload_first" or iteration == "all_cap_first" :
                print("[InputGeneration] [{}] Loading tanker master data for {} in {} from DB is being initiated...".format(iteration, self.loop_status,self.txn_id))
                truck_data, truck_capacity, locked_orders, locked_order_toggle = self.load_truck_data(self.depot_dates, "scheduling", self.region, self.date, self.order_id_ori, self.capacity, self.largest_capacity, self.normal_all_tanker, self.multiload_all_tanker, self.opo2_first, self.opo2_first_capacity)
                print("[InputGeneration] [{}] Loading {} tanker master data for {} in {} from DB has completed...".format(iteration, truck_capacity, self.loop_status,self.txn_id))
            else :
                print("[InputGeneration] [{}] Loading tanker master data for {} in {} from DB is being initiated...".format(iteration, self.loop_status,self.txn_id))
                truck_data, truck_capacity, locked_orders, locked_order_toggle = self.load_truck_data(self.depot_dates, "scheduling", self.region, self.date, self.order_id_ori, self.capacity, self.largest_capacity, self.normal_all_tanker, self.multiload_all_tanker, self.opo2_first, self.opo2_first_capacity)
                print("[InputGeneration] [{}] Loading {} tanker master data for {} in {} from DB has completed...".format(iteration, truck_capacity, self.loop_status,self.txn_id))

        else :
            if self.post_process == True:
                previous_loop = f"post{self.capacity}_First"
                iteration = "sequential_postprocess_second"
            elif self.multiload_all_tanker == True:
                previous_loop = "AllCap_First"
                iteration = "multiload_second"
            elif self.normal_all_tanker == True:
                previous_loop = "AllCap_First"
                iteration = "all_cap_second"                
            else :
                previous_loop = f"{self.capacity}_First"
                iteration = "sequential_intermediate_loop_second"
            
            print("[InputGeneration] [{}] Loading updated tanker data for {} in {} from DB is being initiated...".format(iteration, self.loop_status,self.txn_id))
            truck_data, truck_capacity = InputHandler.get_updated_truck_data(self.txn_id, previous_loop, self.capacity)
            if len(locked_orders) > 0 :
                locked_order_toggle = True

            # Reformat depot start and close time to YYYY-MM-DD HH:MM:SS
            time_list = ['time_from','time_to']
            for col in time_list :
                self.depot_dates[col] = change_strtodatetime_format(self.depot_dates[col], self.date + timedelta(days=1))         
            self.depot_dates['time_from'] = pd.to_datetime(self.depot_dates['time_from'], format = "%Y-%m-%d %H:%M:%S")
            self.depot_dates['time_to'] = pd.to_datetime(self.depot_dates['time_to'], format = "%Y-%m-%d %H:%M:%S")

            # Reformat depot start and close time to XX.XX hour
            time_list = ['time_from','time_to']
            for col in time_list :            
                self.depot_dates[col] = change_datetimetofloat_format(self.depot_dates[col])       

            truck_data = pd.merge(truck_data, self.depot_dates[['terminal','time_from','time_to']], 'left', left_on=['default_terminal'], right_on=['terminal'])
            truck_data.drop(['terminal'], axis=1, inplace=True)
            truck_data = truck_data.rename(columns={'time_from':'depot_start_time','time_to':'depot_close_time'})                 
            print("[InputGeneration] [{}] Loading updated {} tanker data for {} in {} from DB has completed...".format(iteration, truck_capacity, self.loop_status,self.txn_id))

        return order_data, commercial_data, truck_data, iteration, locked_orders, locked_order_toggle


if __name__ == "__main__":
    date = datetime(2020, 9, 22)
    region = 'Central'
    loop_1 = '43680_First'
    loop_2 = '43680_Second'
    module_1 = 'Route'
    module_2 = 'OPO 2'
    txn_id = '8PTQEU' # random id just to trigger the class to run
    start_time_loop = datetime.now()
    capacity = 43680
    largest_capacity = 43680
    input_gen = input_gen(0, 'Yes', date, region, loop_1, txn_id, start_time_loop, capacity, largest_capacity)
    order_data, truck_data, cloud_ordered_list, duplicate_station_full, order_data_full = input_gen.read_input()

