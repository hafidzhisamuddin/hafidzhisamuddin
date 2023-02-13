# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
# if './rts_scheduling_modules' not in sys.path:
#     sys.path.append('./rts_scheduling_modules')
# if './rts_routing_modules' not in sys.path:
#     sys.path.append('./rts_routing_modules')
from src.rts_scheduling_modules.rts_milp_model import MILP
from src.rts_scheduling_modules.rts_call_solver import ModelSolver
from src.rts_scheduling_modules.rts_scheduling_post import eta_calculation, convert_eta, update_truck_order
from src.input_gen_modules.rts_input_handler import InputHandler
from conf import Logger, Config
from src.input_gen_modules import input_generation
from datetime import datetime, timedelta
from conf import Config
#from datetime import datetime
#import datetime
from sqlalchemy import create_engine
from src.rts_routing_modules.rts_routing_utilities import frmt, change_datetimetofloat_format, find_truck_free_windows
from src.rts_routing_modules.rts_route_commercial import commercial_input

from database_connector import DatabaseConnector, YAMLFileConnector

from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

class SchedulingLoop():
    def __init__(self, date, loop_status, workload_obj, txn_id, region, capacity, largest_capacity, post_process = None, smallest_capacity= None, input_source = None, multiload_all_tanker = None, normal_all_tanker = None, all_cap_toggle = None, rts_strategy = None, opo2_first = False, opo2_first_capacity = None, order_id = None, order_id_ori= None, locked_orders = None):
        # self._logger = Logger().logger
        self.date = date
        self.loop_status = loop_status
        self.workload_obj = workload_obj
        self.txn_id = txn_id
        self.region = region
        self.capacity = capacity
        self.largest_capacity = largest_capacity
        self.smallest_capacity = smallest_capacity
        self.post_process = post_process
        self.input_source = input_source
        self.multiload_all_tanker = multiload_all_tanker
        self.normal_all_tanker = normal_all_tanker
        self.all_cap_toggle = all_cap_toggle
        self.rts_strategy = rts_strategy
        self.opo2_first = opo2_first
        self.opo2_first_capacity = opo2_first_capacity
        self.order_id = order_id
        self.order_id_ori = order_id_ori
        self.locked_orders = locked_orders


    def scheduling_loop(self):
        
        db = True
        start_time = datetime.now()
        print("[SchedulingModule] [{}] [{}] Scheduling inputs are being processed...".format(self.loop_status, self.txn_id))

        # Read Retails Order Data
        input_gen = input_generation.InputGen(0, self.order_id, self.date, self.region, self.loop_status, self.txn_id, start_time, self.capacity, self.largest_capacity, self.smallest_capacity, self.post_process, self.input_source, self.multiload_all_tanker, self.normal_all_tanker, self.all_cap_toggle, self.opo2_first, self.opo2_first_capacity, None, self.order_id_ori)
        
        retail_data, commercial_data, truck_data, _, locked_orders, locked_order_toggle = input_gen.scheduling_input(self.locked_orders)
        
        # Append Retail + Commercial order data
        if len(retail_data) > 0 :
            # order_data = retail_data[retail_data['No_Compartment'] > 3]
            order_data = retail_data.copy()
            if len(commercial_data) > 0 :
                order_data = order_data.append(commercial_data)
        elif len(commercial_data) > 0 :
            order_data = commercial_data.copy()
        elif (self.capacity != self.smallest_capacity) and self.post_process == False and self.loop_status.split('_')[0] != "AllCap":
            order_data = pd.DataFrame()
        elif (self.capacity == self.smallest_capacity) and self.post_process == False and self.loop_status.split('_')[0] != "AllCap":
            order_data = pd.DataFrame()  
        elif (self.capacity != self.smallest_capacity) and self.post_process == True and self.loop_status.split('_')[0] != "AllCap":
            order_data = pd.DataFrame()
        elif (self.capacity == self.smallest_capacity) and self.loop_status.split('_')[1] == "First" and self.post_process == True and self.loop_status.split('_')[0] != "AllCap":
            order_data = pd.DataFrame()
        else :
            raise Exception("[SchedulingModule] NO Scheduling inputs, please check. System existing ...") 

        if not order_data.empty :
            order_data_display = order_data.copy()
            order_data_display['grouping'] = order_data_display['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_multiload_early': 'combined', 'single_multiload_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_normal_early': 'combined', 'single_normal_late': 'combined', 'single_multiload': 'single'})
            print("[SchedulingModule] [{}] [{}] Total number of orders need to be scheduled : {}".format(self.loop_status, self.txn_id, order_data_display.groupby(['Destinations', 'Product','condition','grouping','order_status']).ngroups))
            print("[SchedulingModule] [{}] [{}] Total number of routes : {}".format(self.loop_status, self.txn_id, order_data.groupby(['Route_ID']).ngroups))
            print("[SchedulingModule] [{}] [{}] Total number of RT : {}".format(self.loop_status, self.txn_id, truck_data.groupby(['Vehicle Number']).ngroups))
                      
            first_loop_toggle = None
            
            if self.loop_status.split('_')[1] == "First" and self.multiload_all_tanker == True and self.capacity != self.largest_capacity:
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift']
                first_loop_toggle = True
            elif self.loop_status.split('_')[1] == "First" and self.normal_all_tanker == True and self.capacity != self.largest_capacity:
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift']
                first_loop_toggle = True              
            elif self.loop_status.split('_')[1] == "First" and self.multiload_all_tanker == False and self.normal_all_tanker == False and self.capacity == self.largest_capacity and self.post_process == False and self.all_cap_toggle == False :
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift']
                first_loop_toggle = True
            elif self.loop_status.split('_')[1] == "First" and self.multiload_all_tanker == False and self.normal_all_tanker == False and self.capacity != self.largest_capacity and self.post_process == False and self.all_cap_toggle == False :
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift']
                first_loop_toggle = True
            elif self.loop_status.split('_')[1] == "First" and self.multiload_all_tanker == False and self.normal_all_tanker == False and self.capacity != self.largest_capacity and self.post_process == False and self.all_cap_toggle == True and (self.capacity == 16380 or self.capacity == 10920) :
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift']  
                first_loop_toggle = True
            else :
                first_loop_toggle = False
                             
            # Common tanker shift balance df pre-process
            truck_ori_shift = truck_data.drop_duplicates(subset=['Vehicle Number','balance_shift'], keep="first")
            truck_shift_balance = truck_data[['Vehicle Number','Time start route','Time complete route','start_shift','end_shift']]
            truck_shift_balance = pd.merge(truck_shift_balance, truck_ori_shift[['Vehicle Number','balance_shift']], 'left', on=['Vehicle Number'])

            # Final pre-process of tanker shift balance using first loop and locked orders logic
            # Condition when there's no locked orders in first loop
            if first_loop_toggle == True and locked_order_toggle == False :
                truck_shift_balance['Balance'] = truck_shift_balance['Time complete route'] - truck_shift_balance['Time start route']
                refined_truck_data = truck_data.copy()                

            # Condition when there's locked orders in first loop - calculate shift balance after some tankers used by locked orders   
            elif first_loop_toggle == True and locked_order_toggle == True :
                # Concate locked orders tanker with existing active tanker master
                truck_data = pd.concat([truck_data, locked_orders[['Vehicle Number', 'Time start route', 'Time complete route']]], axis=0)
                # Forward fill all NA static columns of locked orders
                truck_data = truck_data.groupby('Vehicle Number').apply(lambda x: x.ffill(limit=len(x)))

                # Remove tankers original data from master if used in locked orders, but remain the later into refined tanker data
                refined_truck_data = truck_data.copy()
                refined_truck_data['count'] = refined_truck_data.groupby('Vehicle Number')['Vehicle Number'].transform('count')
                refined_truck_data['remove_flag'] = np.where((refined_truck_data['start_shift'] == refined_truck_data['Time start route']) & (refined_truck_data['end_shift'] == refined_truck_data['Time complete route']) & (refined_truck_data['count'] > 1), 1, 0)
                refined_truck_data = refined_truck_data[(refined_truck_data['count'] >= 1) & (refined_truck_data['remove_flag']!=1)]

                truck_used = locked_orders['Vehicle Number'].unique()
                truck_used_balance = locked_orders.copy()

                # Calculate the remaining tanker shift balance after usage of locked orders                
                truck_used_balance['Balance'] = truck_used_balance['Balance'].groupby(truck_used_balance['Vehicle Number']).transform('sum')
                truck_used_balance = truck_used_balance.drop_duplicates(subset=['Vehicle Number','Balance'], keep="first")
                truck_used_balance['Balance'] = truck_used_balance['balance_shift'] - truck_used_balance['Balance']

                # Calculate the remaining tanker shift balance for tankers not used by locked orders
                truck_unused_balance = truck_shift_balance[~truck_shift_balance['Vehicle Number'].isin(truck_used)]
                truck_unused_balance['Balance'] = truck_unused_balance['Time complete route'] - truck_unused_balance['Time start route']
                
                # Combine used and unused tanker shift balance
                truck_shift_balance = pd.concat([truck_unused_balance, truck_used_balance], axis=0)
                
                # Getting back the original default start and end shift of each tanker after above concat
                truck_shift_balance = truck_shift_balance.merge(truck_ori_shift[['Vehicle Number','start_shift','end_shift']], on='Vehicle Number')
                truck_shift_balance['start_shift'] = truck_shift_balance['start_shift_x'].where(truck_shift_balance['start_shift_x'].notnull(), truck_shift_balance['start_shift_y'])   
                truck_shift_balance['end_shift'] = truck_shift_balance['end_shift_x'].where(truck_shift_balance['end_shift_x'].notnull(), truck_shift_balance['end_shift_y']) 

                truck_shift_balance = truck_shift_balance.drop(['start_shift_x','end_shift_x','start_shift_y','end_shift_y'],axis=1)
                print("[SchedulingModule] [{}] [{}] Locked Orders and tanker shift balance updated...".format(self.loop_status, self.txn_id))

            # Condition in 2nd loop with/without locked orders - calculate shift balance after some tankers used by 1st loop and/or locked orders
            else :
                refined_truck_data = truck_data.copy()
                refined_truck_data['count'] = refined_truck_data.groupby('Vehicle Number')['Vehicle Number'].transform('count')
                refined_truck_data['remove_flag'] = np.where((refined_truck_data['start_shift'] == refined_truck_data['Time start route']) & (refined_truck_data['end_shift'] == refined_truck_data['Time complete route']) & (refined_truck_data['count'] > 1), 1, 0)
                refined_truck_data = refined_truck_data[(refined_truck_data['count'] >= 1) & (refined_truck_data['remove_flag']!=1)]

                truck_count = truck_shift_balance.value_counts(['Vehicle Number']).reset_index(name='count')
                
                # Segregate used (from 1st loop and/or locked orders) and unused tanker
                if locked_order_toggle == True :
                    truck_used = np.append(truck_count[truck_count['count'] > 1]['Vehicle Number'].unique(), locked_orders['Vehicle Number'].unique())
                    truck_used = list(set(truck_used))
                    truck_unused = truck_count[truck_count['count'] < 2]['Vehicle Number'].unique()
                    truck_unused = list(set(truck_unused) - set(truck_used))
                
                else :
                    truck_used = truck_count[truck_count['count'] > 1]['Vehicle Number'].unique()
                    truck_unused = truck_count[truck_count['count'] < 2]['Vehicle Number'].unique()

                # Calculate the remaining tanker shift balance after usage of 1st loop and/or locked order
                truck_used_balance = truck_shift_balance[truck_shift_balance['Vehicle Number'].isin(truck_used)]
                truck_used_balance['Balance'] = truck_used_balance['Time complete route'] - truck_used_balance['Time start route']
                truck_used_balance = truck_used_balance[~(truck_used_balance['Balance']==truck_used_balance['balance_shift'])]
                
                truck_used_balance['Balance'] = truck_used_balance['Balance'].groupby(truck_used_balance['Vehicle Number']).transform('sum')
                truck_used_balance = truck_used_balance.drop_duplicates(subset=['Vehicle Number','Balance'], keep="first")
                truck_used_balance['Balance'] = truck_used_balance['balance_shift'] - truck_used_balance['Balance']
                
                # Calculate the remaining tanker shift balance for tankers not used by 1st loop and/or locked order
                truck_unused_balance = truck_shift_balance[truck_shift_balance['Vehicle Number'].isin(truck_unused)]
                truck_unused_balance['Balance'] = truck_unused_balance['Time complete route'] - truck_unused_balance['Time start route'] 
                
                # Combine used and unused tanker shift balance
                truck_shift_balance = pd.concat([truck_unused_balance, truck_used_balance], axis=0)

                # Getting back the original default start and end shift of each tanker after above concatination
                truck_shift_balance = truck_shift_balance.merge(truck_ori_shift[['Vehicle Number','start_shift','end_shift']], on='Vehicle Number')
                truck_shift_balance['start_shift'] = truck_shift_balance['start_shift_x'].where(truck_shift_balance['start_shift_x'].notnull(), truck_shift_balance['start_shift_y'])   
                truck_shift_balance['end_shift'] = truck_shift_balance['end_shift_x'].where(truck_shift_balance['end_shift_x'].notnull(), truck_shift_balance['end_shift_y'])   

                truck_shift_balance = truck_shift_balance.drop(['start_shift_x','end_shift_x','start_shift_y','end_shift_y'],axis=1)
                print("[SchedulingModule] [{}] [{}] Tanker shift balance updated...".format(self.loop_status, self.txn_id))

            #order_data['Min_Retain_Hour'] = order_data['Min_Retain_Hour'].dt.hour
            #order_data['Min_Runout'] = order_data['Min_Runout'].dt.hour

            order_data[['Min_Retain_Hour','Min_Runout']] = order_data[['Min_Retain_Hour','Min_Runout']].fillna(0)
            order_data[['Min_Retain_Hour','Min_Runout']] = order_data[['Min_Retain_Hour','Min_Runout']].astype(int)
            
            # Find minimum runout duration for each route
            order_data['runout_duration'] = order_data.apply(lambda row: (abs(row.Runout - row.Retain_Hour).total_seconds() / 3600), axis=1)
            mins_runout_df = order_data.groupby(['Route_ID'])['runout_duration'].agg('min').reset_index()
            mins_runout_df = mins_runout_df.rename(columns={"runout_duration": "min_runout_duration"})
            order_data = pd.merge(order_data, mins_runout_df, 'left', on=['Route_ID'])    

            # Find untilized time windows for each tanker following shift type for time windows constraint in MILP
            if locked_order_toggle == True or self.loop_status.split('_')[1] == "Second" :

                refined_truck_data_dup = refined_truck_data.copy()
                refined_truck_data_dup = pd.merge(refined_truck_data_dup, truck_shift_balance[['Vehicle Number','Balance']], how='left', on='Vehicle Number')

                # Replace start and end shift with respective tanker's terminal start and close time to find realistic free windows in below find_truck_free_windows()
                refined_truck_data_dup['start_shift'] = np.where(refined_truck_data_dup['start_shift'] >= refined_truck_data_dup['depot_start_time'], refined_truck_data_dup['start_shift'], refined_truck_data_dup['depot_start_time'])
                refined_truck_data_dup['end_shift'] = np.where(refined_truck_data_dup['end_shift'] <= refined_truck_data_dup['depot_close_time'], refined_truck_data_dup['end_shift'], refined_truck_data_dup['depot_close_time'])
                truck_time = refined_truck_data_dup.sort_values(['Time start route'],ascending=True).groupby('Vehicle Number')[['start_shift', 'end_shift','Time start route','Time complete route','balance_shift','Balance']].apply(lambda g: list(map(tuple, g.values.tolist()))).to_dict()

                truck_free_tw = pd.DataFrame()
                for truck in truck_time :
                    fw_list = find_truck_free_windows(truck_time.get(truck))
                    rt_df = pd.DataFrame(fw_list, columns=['Time start route', 'Time complete route'])
                    rt_df['Vehicle Number'] = truck
                    truck_free_tw = pd.concat([truck_free_tw, rt_df], axis = 0)

                # Merge back utilized time windows to refined tanker data
                truck_merge = refined_truck_data.drop_duplicates(subset=refined_truck_data.columns.difference(['Time start route','Time complete route','count','remove_flag']), keep="first")
                truck_free_tw = pd.merge(truck_free_tw, truck_merge[truck_merge.columns.difference(['Time start route','Time complete route','count','remove_flag'])], how='left', on='Vehicle Number')
                refined_truck_data = truck_free_tw.copy()
                        
            milp_model = MILP(self.workload_obj, order_data, refined_truck_data, truck_shift_balance, self.region, self.loop_status, self.rts_strategy, self.opo2_first)
            model_build = milp_model.model

            result = ModelSolver(model_build, self.loop_status)
            result_1 = result.get_output(model_build)
                        
        else:
            result_1 = pd.DataFrame()

        if not result_1.empty :
            print("[SchedulingModule] [{}] [{}] Postprocess for scheduled & unscheduled orders are being initiated...".format(self.loop_status, self.txn_id))

            distance_time_api = InputHandler.get_dist_matrix(self.region)


            # Append all iteration results
            new_truck_data = update_truck_order(result_1, truck_data)
            new_truck_data = new_truck_data.drop_duplicates(subset=['Vehicle Number','Time start route','Time complete route'], keep="first")

            check_routes = order_data.merge(result_1[['Route_ID','visit_station_or_not','Time start route','Time complete route', 'Trip', 'Vehicle Number']], 'left', on=['Route_ID'])
            scheduled_orders = check_routes[check_routes['Route_ID'].isin(result_1['Route_ID'].unique())]
            
            scheduled_orders = eta_calculation(scheduled_orders, distance_time_api, self.region) # to generate the eta for each station
            # route_no_distance = scheduled_orders[np.isnan(scheduled_orders.ETA)]['Route_ID'].unique()
            # scheduled_orders = scheduled_orders[~scheduled_orders['Route_ID'].isin(route_no_distance)]
            # result_1["visit_station_or_not"]=np.where(result_1['Route_ID'].isin(route_no_distance), result_1['visit_station_or_not'].replace(0.0), result_1["visit_station_or_not"])
            scheduled_orders = convert_eta(scheduled_orders, self.date) # convert eta and pldtime into h:m:s
            
            def change_time_format(column):
                scheduled_orders[column] = scheduled_orders[column].astype(float)
                scheduled_orders[column] = scheduled_orders[column].map(frmt)
                scheduled_orders[column] = pd.to_datetime(self.date)+ pd.to_timedelta(scheduled_orders[column])
                scheduled_orders[column] = scheduled_orders[column] + pd.DateOffset(days=1)
                return scheduled_orders
            
            scheduled_orders = scheduled_orders.rename(columns={"Order_Quantity": "Quantity_carried", "Tanker_Cap": "Tanker_Capacity"})
            scheduled_orders['Total_Quantity'] = scheduled_orders['Tanker_Capacity']
            scheduled_orders = change_time_format('Time complete route')
            scheduled_orders = change_time_format('Time start route')

            scheduled_orders = scheduled_orders.drop(['No_Compartment','Min_Runout'], axis = 1) 
            scheduled_orders['Min_Retain_Hour'] = scheduled_orders['Min_Retain_Hour'].astype(int)
            scheduled_orders['Min_Retain_Hour'] = pd.to_datetime(scheduled_orders['Min_Retain_Hour'], format='%H')
            scheduled_orders['Min_Retain_Hour'] = scheduled_orders['Min_Retain_Hour'].dt.strftime("%Y-%m-%d %H:%M:%S")
            scheduled_orders['Min_Retain_Hour'] = pd.to_datetime(scheduled_orders['Min_Retain_Hour'], format='%Y-%m-%d %H:%M:%S')
            scheduled_orders['Min_Retain_Hour'] = scheduled_orders['Min_Retain_Hour'] + pd.offsets.DateOffset(year=self.date.year, day=self.date.day, month=self.date.month)
            scheduled_orders['Min_Retain_Hour'] = scheduled_orders['Min_Retain_Hour'] + pd.DateOffset(days=1)
            scheduled_orders['Region'] = self.region

            # if 'CO' in scheduled_orders['Order_Type'].to_list():
            #     scheduled_orders = scheduled_orders[['Order_Type','Route_ID','Terminal_97','Terminal_Default','Tanker_Capacity','Vehicle Number','Trip','Time start route','PldTime','ETA','Time complete route',
            #                                         'Order_ID','Destinations','Product','Quantity_carried','Total_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day',
            #                                         'Travel_Time','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Min_Retain_Hour',
            #                                         'DMR_Date','Loop_Counter','Job_StartTime','Txn_ID','Region']]
            # else:
            scheduled_orders = scheduled_orders[['Order_Type','Route_ID','Terminal_97','Terminal_Default','Tanker_Capacity','Vehicle Number','Trip','Time start route','PldTime','ETA','Time complete route',
                                                'Order_ID','Destinations','Product','Quantity_carried','Total_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day',
                                                'Travel_Time','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Min_Retain_Hour',
                                                'DMR_Date','Loop_Counter','Job_StartTime','Txn_ID','Region']]

            scheduled_orders['Strategy'] = self.rts_strategy
            scheduled_orders['Filter'] = scheduled_orders['Order_Type'].astype(str) + '_' + scheduled_orders['DMR_Date'].astype(str) + '_' + scheduled_orders['Txn_ID'].astype(str) + '_' + scheduled_orders['Region'].astype(str) + '_' + scheduled_orders['Strategy'].astype(str)

            if self.loop_status.split('_')[1] == "First" and self.opo2_first == False:
                if len(retail_data) > 0 :
                    check_routes = retail_data.merge(result_1[['Route_ID','visit_station_or_not']], 'left', on=['Route_ID'])
                    check_routes = check_routes.query('visit_station_or_not == 1')

                    check_routes['identifier'] = check_routes['Destinations'].astype(str) + '_' + check_routes['Product'].astype(str) + '_' + check_routes['condition'] + '_' + check_routes['grouping'] 
                    retail_data['identifier'] = retail_data['Destinations'].astype(str) + '_' + retail_data['Product'].astype(str) + '_' + retail_data['condition'] + '_' + retail_data['grouping'] 
                    remove_route = check_routes[['Route_ID','identifier']]['Route_ID'].unique()
                    order_wo_route = (retail_data[~retail_data['Route_ID'].isin(remove_route)])
                    
                    remove_identifier = check_routes[['Route_ID','identifier']]['identifier'].unique()
                    remove_more_route = order_wo_route[~order_wo_route['identifier'].isin(remove_identifier)]

                    unscheduled_order = remove_more_route[~remove_more_route['identifier'].isin(check_routes['identifier'])].reset_index(drop=True)
                    unscheduled_order = unscheduled_order.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                else :
                    unscheduled_order = pd.DataFrame()

                if len(commercial_data) > 0 :
                    check_commercial = commercial_data.merge(result_1[['Route_ID','visit_station_or_not']], 'left', on=['Route_ID'])
                    unscheduled_commercial = check_commercial.query('visit_station_or_not != 1')
                    unscheduled_commercial = unscheduled_commercial.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                else :
                   unscheduled_commercial = pd.DataFrame() 

                if len(unscheduled_commercial) > 0:
                    #unscheduled_commercial[["End_Stock_Day", "Cloud"]] = np.nan
                    #unscheduled_commercial[["Tank_Status", "Multiload_Status", "condition", "grouping", "order_status"]] = 'None'
                    # unscheduled_commercial[['Opening_Time','Closing_Time']] = unscheduled_commercial[['Opening_Time','Closing_Time']].astype(float)
                    # unscheduled_commercial['Opening_Time'] = unscheduled_commercial['Opening_Time'].map(frmt)
                    # unscheduled_commercial['Closing_Time'] = unscheduled_commercial['Closing_Time'].map(frmt)
                    unscheduled_order = unscheduled_order.append(unscheduled_commercial)
                
                try:
                    unscheduled_order = unscheduled_order.drop(['visit_station_or_not','identifier','No_Compartment','Tanker_Cap','Route_ID','Utilization','Min_Retain_Hour', 'Min_Runout', 'no_delivery_start_1', 'no_delivery_end_1', 'no_delivery_start_2', 'no_delivery_end_2', 'delta', 'Travel_Time'], axis=1)
                except:
                    unscheduled_order = unscheduled_order.drop(['identifier','No_Compartment','Tanker_Cap','Route_ID','Utilization','Min_Retain_Hour', 'Min_Runout', 'no_delivery_start_1', 'no_delivery_end_1', 'no_delivery_start_2', 'no_delivery_end_2', 'delta', 'Travel_Time'], axis=1)
                
                unscheduled_order['Region'] = self.region
                unscheduled_order['DMR_Date'] = self.date
                unscheduled_order['Loop_Counter'] = self.loop_status
                unscheduled_order['Job_StartTime'] = start_time
                unscheduled_order['Txn_ID'] = self.txn_id

                unscheduled_order['Strategy'] = self.rts_strategy
                unscheduled_order['Filter'] = unscheduled_order['Order_Type'].astype(str) + '_' + unscheduled_order['DMR_Date'].astype(str) + '_' + unscheduled_order['Txn_ID'].astype(str) + '_' + unscheduled_order['Region'].astype(str) + '_' + unscheduled_order['Strategy'].astype(str)

                unscheduled_order = unscheduled_order[['Destinations', 'Terminal_97', 'Terminal_Default', 'Product', 'Order_Quantity', 'Opening_Time', 'Closing_Time', 'Priority', 'Retain_Hour', 'Runout', 'End_Stock_Day', 'Accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Txn_ID', 'Region', 'Order_Type', 'Strategy', 'Filter']]

            elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True:
                if len(retail_data) > 0 :
                    check_routes2 = retail_data.merge(result_1[['Route_ID','visit_station_or_not']], 'left', on=['Route_ID'])
                    check_routes2 = check_routes2.query('visit_station_or_not == 1')
                    #check_routes2['condition'] = check_routes2['condition'].str.replace(r'_[0-9]+','')
                    #retail_data['condition'] = retail_data['condition'].str.replace(r'_[0-9]+','')

                    check_routes2['identifier'] = check_routes2['Destinations'].astype(str) + '_' + check_routes2['Product'].astype(str) + '_' + check_routes2['condition'].astype(str)  + '_' + check_routes2['grouping'].astype(str) + '_' + check_routes2['order_status'].astype(str)
                    retail_data['identifier'] = retail_data['Destinations'].astype(str) + '_' + retail_data['Product'].astype(str) + '_' + retail_data['condition'].astype(str)  + '_' + retail_data['grouping'].astype(str)  + '_' + retail_data['order_status'].astype(str)
                    check_routes2['identifier'] = check_routes2['identifier'].astype('category')
                    retail_data['identifier'] = retail_data['identifier'].astype('category')

                    remove_route = check_routes2[['Route_ID','identifier']]['Route_ID'].unique()
                    order_wo_route = (retail_data[~retail_data['Route_ID'].isin(remove_route)])
                    
                    remove_identifier = check_routes2[['Route_ID','identifier']]['identifier'].unique()
                    remove_more_route = order_wo_route[~order_wo_route['identifier'].isin(remove_identifier)]
                    
                    check_routes2['grouping'] = check_routes2['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_multiload': 'single', 'single_multiload_early':'single', 'single_multiload_late':'single'})
                    remove_more_route['grouping'] = remove_more_route['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_multiload': 'single', 'single_multiload_early':'single', 'single_multiload_late':'single'})
                    remove_more_route = remove_more_route.drop_duplicates(subset=['Destinations', 'Product','Multiload_Status','condition','grouping'], keep="first")
                    remove_more_route['identifier'] = remove_more_route['Destinations'].astype(str) + '_' + remove_more_route['Product'].astype(str) + '_' + remove_more_route['condition'].astype(str)  + '_' + remove_more_route['grouping'].astype(str)  + '_' + remove_more_route['order_status'].astype(str)
                    remove_more_route['identifier'] = remove_more_route['identifier'].astype('category')
                    check_routes2['identifier'] = check_routes2['Destinations'].astype(str) + '_' + check_routes2['Product'].astype(str) + '_' + check_routes2['condition'].astype(str)  + '_' + check_routes2['grouping'].astype(str) + '_' + check_routes2['order_status'].astype(str)
                    check_routes2['identifier'] = check_routes2['identifier'].astype('category')

                    unscheduled_order = remove_more_route[~remove_more_route['identifier'].isin(check_routes2['identifier'])].reset_index(drop=True)   
                    #unscheduled_order = unscheduled_order.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                    unscheduled_order = unscheduled_order.drop(['identifier','No_Compartment','Tanker_Cap','Route_ID','Utilization','Min_Retain_Hour', 'Min_Runout', 'no_delivery_start_1', 'no_delivery_end_1', 'no_delivery_start_2', 'no_delivery_end_2'], axis=1)
                    order_data_ori = InputHandler.get_processed_ori_orders(self.txn_id)                    
                    unscheduled_order = unscheduled_order.merge(order_data_ori[['Ship-to','Material','retain_hour', 'Runout', 'Qty', 'Multiload_Status', 'condition', 'grouping', 'order_status']], left_on=['Destinations','Product','Multiload_Status', 'condition', 'grouping', 'order_status'], right_on=['Ship-to', 'Material', 'Multiload_Status', 'condition', 'grouping', 'order_status'], how='inner')
                    unscheduled_order = unscheduled_order.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                    unscheduled_order = unscheduled_order.drop(['Order_Quantity','Ship-to','Material','Retain_Hour','Runout_x'], axis=1)
                    unscheduled_order = unscheduled_order.rename(columns={"Qty":"Order_Quantity", "retain_hour": "Retain_Hour","Runout_y":"Runout"})
                else :
                    unscheduled_order = pd.DataFrame()
                                
                if len(commercial_data) > 0 :
                    check_commercial = commercial_data.merge(result_1[['Route_ID','visit_station_or_not']], 'left', on=['Route_ID'])
                    unscheduled_commercial = check_commercial.query('visit_station_or_not != 1')
                    unscheduled_commercial = unscheduled_commercial.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                else :
                   unscheduled_commercial = pd.DataFrame() 
                
                if len(unscheduled_commercial) > 0:
                    #unscheduled_commercial[["End_Stock_Day", "Cloud"]] = np.nan
                    #unscheduled_commercial[["Tank_Status", "Multiload_Status", "condition", "grouping", "order_status"]] = 'None'
                    # unscheduled_commercial[['Opening_Time','Closing_Time']] = unscheduled_commercial[['Opening_Time','Closing_Time']].astype(float)
                    # unscheduled_commercial['Opening_Time'] = unscheduled_commercial['Opening_Time'].map(frmt)
                    # unscheduled_commercial['Closing_Time'] = unscheduled_commercial['Closing_Time'].map(frmt)
                    unscheduled_order = unscheduled_order.append(unscheduled_commercial)  
                
                unscheduled_order['Region'] = self.region
                unscheduled_order['DMR_Date'] = self.date
                unscheduled_order['Loop_Counter'] = self.loop_status
                unscheduled_order['Job_StartTime'] = start_time
                unscheduled_order['Txn_ID'] = self.txn_id 

                unscheduled_order['Strategy'] = self.rts_strategy
                unscheduled_order['Filter'] = unscheduled_order['Order_Type'].astype(str) + '_' + unscheduled_order['DMR_Date'].astype(str) + '_' + unscheduled_order['Txn_ID'].astype(str) + '_' + unscheduled_order['Region'].astype(str) + '_' + unscheduled_order['Strategy'].astype(str)

                unscheduled_order = unscheduled_order[['Destinations', 'Terminal_97', 'Terminal_Default', 'Product', 'Order_Quantity', 'Opening_Time', 'Closing_Time', 'Priority', 'Retain_Hour', 'Runout', 'End_Stock_Day', 'Accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Txn_ID', 'Region', 'Order_Type', 'Strategy', 'Filter']]
            else :
                pass
            
            print("[SchedulingModule] [{}] [{}] Postprocess for scheduled & unscheduled orders are completed...".format(self.loop_status, self.txn_id))
            print("[SchedulingModule] [{}] [{}] Total number of orders need to be scheduled : {}".format(self.loop_status, self.txn_id, order_data_display.groupby(['Destinations', 'Product','condition','grouping']).ngroups))
            print("[SchedulingModule] [{}] [{}] Total number of scheduled orders : {}".format(self.loop_status, self.txn_id, len(scheduled_orders)))
            print("[SchedulingModule] [{}] [{}] Total number of unscheduled orders : {}".format(self.loop_status, self.txn_id, len(unscheduled_order)))

        else:
            scheduled_orders = pd.DataFrame()
            
            if self.loop_status.split('_')[1] == "First" and self.multiload_all_tanker == True and self.capacity != self.largest_capacity:
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift'] 
            elif self.loop_status.split('_')[1] == "First" and self.normal_all_tanker == True and self.capacity != self.largest_capacity:
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift']                 
            elif self.loop_status.split('_')[1] == "First" and self.all_cap_toggle == False and self.multiload_all_tanker == False and self.normal_all_tanker == False and self.capacity == self.largest_capacity and self.post_process == False:
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift'] 
            elif self.loop_status.split('_')[1] == "First" and self.all_cap_toggle == False and self.multiload_all_tanker == False and self.normal_all_tanker == False and self.capacity != self.largest_capacity and self.post_process == False:
                truck_data['Time start route'], truck_data['Time complete route'] = truck_data['start_shift'], truck_data['end_shift'] 
            else :
                pass      
                    
            new_truck_data = update_truck_order(result_1, truck_data)
            new_truck_data = new_truck_data.drop_duplicates(subset=['Vehicle Number','Time start route','Time complete route'], keep="first")

            if not order_data.empty :
                print("[SchedulingModule] [{}] [{}] There are NO SOLUTION. All orders are unscheduled.".format(self.loop_status, self.txn_id))
                
                if self.loop_status.split('_')[1] == "First" and self.opo2_first == False :
                    unscheduled_order = order_data.copy()
                    unscheduled_order = unscheduled_order.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                    unscheduled_order = unscheduled_order.drop(['No_Compartment','Tanker_Cap','Route_ID','Utilization','Travel_Time','Min_Retain_Hour', 'Min_Runout', 'no_delivery_start_1', 'no_delivery_end_1', 'no_delivery_start_2', 'no_delivery_end_2'], axis=1)

                elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                    unscheduled_order = order_data.copy()
                    unscheduled_order['grouping'] = unscheduled_order['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_multiload': 'single', 'single_multiload_early':'single', 'single_multiload_late':'single'})
                    unscheduled_order = unscheduled_order.drop(['No_Compartment','Tanker_Cap','Route_ID','Utilization','Travel_Time','Min_Retain_Hour', 'Min_Runout', 'no_delivery_start_1', 'no_delivery_end_1', 'no_delivery_start_2', 'no_delivery_end_2'], axis=1)
                    
                    if len(retail_data) > 0 :
                        unscheduled_order = retail_data.copy()
                        unscheduled_order['grouping'] = unscheduled_order['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_multiload': 'single', 'single_multiload_early':'single', 'single_multiload_late':'single'})
                        unscheduled_order = unscheduled_order.drop(['No_Compartment','Tanker_Cap','Route_ID','Utilization','Travel_Time','Min_Retain_Hour', 'Min_Runout', 'no_delivery_start_1', 'no_delivery_end_1', 'no_delivery_start_2', 'no_delivery_end_2'], axis=1)

                        if self.input_source == "dmr" :                            
                            order_data_ori = InputHandler.get_processed_ori_orders(self.txn_id)
                            unscheduled_order = unscheduled_order.merge(order_data_ori[['Ship-to','Material','retain_hour', 'Runout', 'Qty', 'Multiload_Status', 'condition', 'grouping', 'order_status']], left_on=['Destinations','Product','Multiload_Status', 'condition', 'grouping', 'order_status'], right_on=['Ship-to', 'Material', 'Multiload_Status', 'condition', 'grouping', 'order_status'], how='inner')
                            unscheduled_order = unscheduled_order.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                            unscheduled_order = unscheduled_order.drop(['Order_Quantity','Ship-to','Material','Retain_Hour','Runout_x'], axis=1)
                            unscheduled_order = unscheduled_order.rename(columns={"Qty":"Order_Quantity", "retain_hour": "Retain_Hour","Runout_y":"Runout"})

                        if self.input_source == "opo":
                            order_data_ori = InputHandler.get_opo_output_data(self.region, self.date)
                            order_data_ori['Remark_imputed'] = np.where(order_data_ori['Remark_imputed'] < 24, order_data_ori['Remark'], 23)
                            order_data_ori['Remark_imputed'] = order_data_ori['Remark_imputed'].astype(int)
                            order_data_ori['Remark_imputed'] = pd.to_datetime(order_data_ori['Remark_imputed'], format='%H')
                            order_data_ori['Remark_imputed'] = order_data_ori['Remark_imputed'].dt.strftime("%Y-%m-%d %H:%M:%S")
                            order_data_ori['Remark_imputed'] = pd.to_datetime(order_data_ori['Remark'], format='%Y-%m-%d %H:%M:%S')
                            order_data_ori['Remark_imputed'] = order_data_ori['Remark_imputed'] + pd.offsets.DateOffset(year=self.date.year, day=self.date.day, month=self.date.month)
                            order_data_ori['Remark_imputed'] = order_data_ori['Remark_imputed'] + pd.DateOffset(days=1)
                            
                            unscheduled_orders_multiload = unscheduled_order[unscheduled_order['Multiload_Status']=='Multiload'].merge(order_data_ori[['Ship To','Material','Remark_imputed','Runout','Quantity']], left_on=['Destinations','Product','Retain_Hour'], right_on=['Ship To', 'Material','Remark_imputed'], how='inner')
                            unscheduled_order = unscheduled_order[unscheduled_order['Multiload_Status']=='Normal'].merge(order_data_ori[['Ship To','Material','Remark_imputed','Runout','Quantity']], left_on=['Destinations','Product'], right_on=['Ship To', 'Material'], how='inner')
                            unscheduled_order = pd.concat([unscheduled_order,unscheduled_orders_multiload],ignore_index=False)
                            unscheduled_order = unscheduled_order.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                            unscheduled_order = unscheduled_order.drop(['Order_Quantity','Ship To','Material','Retain_Hour','Runout_x'], axis=1)
                            unscheduled_order = unscheduled_order.rename(columns={"Quantity":"Order_Quantity", "Remark_imputed": "Retain_Hour","Runout_y":"Runout"})
                    else :
                        unscheduled_order = pd.DataFrame()

                    if len(commercial_data) > 0 :
                        unscheduled_commercial = commercial_data
                    else :
                        unscheduled_commercial = pd.DataFrame() 
                
                    if len(unscheduled_commercial) > 0:
                        unscheduled_order = unscheduled_order.append(unscheduled_commercial)

                else :
                    pass 

                unscheduled_order['Region'] = self.region
                unscheduled_order['DMR_Date'] = self.date
                unscheduled_order['Loop_Counter'] = self.loop_status
                unscheduled_order['Job_StartTime'] = start_time
                unscheduled_order['Txn_ID'] = self.txn_id
                unscheduled_order['Strategy'] = self.rts_strategy
                unscheduled_order['Filter'] = unscheduled_order['Order_Type'].astype(str) + '_' + unscheduled_order['DMR_Date'].astype(str) + '_' + unscheduled_order['Txn_ID'].astype(str) + '_' + unscheduled_order['Region'].astype(str) + '_' + unscheduled_order['Strategy'].astype(str)

                unscheduled_order = unscheduled_order[['Destinations', 'Terminal_97', 'Terminal_Default', 'Product', 'Order_Quantity', 'Opening_Time', 'Closing_Time', 'Priority', 'Retain_Hour', 'Runout', 'End_Stock_Day', 'Accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Txn_ID', 'Region', 'Order_Type', 'Strategy', 'Filter']]
            
            else:
                # TODO 
                print("[SchedulingModule] [{}] [{}] There are NO ROUTES. SchedulingModule is stopped.".format(self.loop_status, self.txn_id))
                unscheduled_order = pd.DataFrame()

        new_truck_data['DMR_Date'] = self.date
        new_truck_data['Loop_Counter'] = self.loop_status
        new_truck_data['Job_StartTime'] = start_time
        new_truck_data['Txn_ID'] = self.txn_id
        
        try:
            new_truck_data = new_truck_data.drop(['Route_ID','Trip','Tanker_Capacity','visit_station_or_not','depot_start_time','depot_close_time'], axis = 1)
        except KeyError:
            try :
                new_truck_data = new_truck_data.drop(['depot_start_time','depot_close_time'], axis = 1)
            except KeyError:
                pass

        try:
            unscheduled_order = unscheduled_order.drop(['start_1', 'end_1', 'start_2', 'end_2'], axis=1)
        except KeyError:
            pass

        try:
            unscheduled_order = unscheduled_order.drop(["Total_Quantity", "delta",'Default terminal'], axis=1)
        except KeyError:
            pass

        
        print("[SchedulingModule] Saving out output dataframes...")   

        # Save updated truck data in csv and database
        if db:
            DBConnector.save(new_truck_data, Config.TABLES['updated_truck']["tablename"], if_exists='append')
            print("[SchedulingModule] All rts_updated_truck_data_MILPOutput are loaded succesfully to SQL database.")
            DBConnector.save(scheduled_orders, Config.TABLES['scheduled_data']["tablename"], if_exists='append')
            print("[SchedulingModule] All rts_scheduled_order_MILPOutput are loaded succesfully to SQL database.")
            DBConnector.save(unscheduled_order, Config.TABLES['unscheduled_data']["tablename"], if_exists='append')
            print("[SchedulingModule] All rts_unscheduled_order_MILPOutput are loaded succesfully to SQL database.")
        else:
            new_truck_data.to_csv('../../data/Output_Data/INT_rts_updated_truck_data_MILPOutput.csv')
            print("\nAll rts_updated_truck_data_MILPOutput are loaded successfully to csv.")
            try:  
                unscheduled_order.to_csv('../../data/Output_Data/INT_rts_unscheduled_order_MILPOutput.csv')
                print("\nAll rts_unscheduled_order_MILP Output are saved succesfully to CSV files.")
            except:
                pass            
            try:
                scheduled_orders.to_csv('../../data/Output_DataINT_rts_scheduled_orders_MILPOutput.csv')
                print('\nAll rts_scheduled_orders_MILP Output are saved succesfully to CSV files.')
            except:
                pass
        
        end_time = datetime.now()
        diff = end_time - start_time
        print("[SchedulingModule] Total runtime for SchedulingModule is {}".format(diff))
        print("[SchedulingModule] Scheduling module for {} in {} ends successfully...\n".format(self.loop_status, self.txn_id))
        return scheduled_orders, unscheduled_order
  
        
if __name__ == "__main__":
    #txn_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    txn_id = 'FMTLA'
    date_time = datetime(2020, 9, 22)
    region = 'Central'  
    start_time = datetime.now()
    loop_status = '43680_First'
    capacity = 43680
    largest_capacity = 43680
    # scheduling_loop(date_time, loop_status, txn_id, region, capacity, largest_capacity)
