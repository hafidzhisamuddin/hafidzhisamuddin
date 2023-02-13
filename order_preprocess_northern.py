import os
import pandas as pd
import numpy as np
import itertools
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")
from src.input_gen_modules.rts_input_handler import InputHandler
from src.rts_routing_modules.rts_routing_utilities import order_permutation_central, order_permutation_eastern, order_permutation_northern, end_day_stock_1, change_datetimetofloat_format, change_retain_format
from conf import Config, Logger
from database_connector import DatabaseConnector, YAMLFileConnector
from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

class Preprocessing_Northern(object):  

    def __init__(self, end_day, order_data, master_all, forecast_data, inventory_data_tomorrow, sales_time_slot, date, region, loop_status, txn_id, start_time, capacity, largest_capacity, commercial_data, unmatched_commercial, post_process = False, multiload_all_tanker = None, normal_all_tanker = None, master_terminal_data = None, order_id_df = None, opo2_first = False):
        self.order_data = order_data
        self.master_all = master_all
        self.forecast_data = forecast_data
        self.inventory_data_tomorrow = inventory_data_tomorrow
        self.date = date 
        self.region = region 
        self.loop_status = loop_status
        self.txn_id = txn_id
        self.start_time = start_time
        self.capacity = capacity
        self.largest_capacity = largest_capacity
        self.post_process = post_process
        self.multiload_all_tanker = multiload_all_tanker
        self.normal_all_tanker = normal_all_tanker
        self.sales_time_slot = sales_time_slot
        self.end_day = end_day
        self.master_terminal_data = master_terminal_data
        self.opo2_first = opo2_first
        self.order_id_df = order_id_df
        self.commercial_data = commercial_data
        self.unmatched_commercial = unmatched_commercial
        
        self.activate_opo2_log = False # turn true/false to record OPO2 modification log 

    
        
    def preprocess_order(self):
        print("[PreprocessNorthern] Preprocessing multiload orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
        ori_order_multiload = None
        processed_order_multiload = None
        full_list = None
        iteration = None


        # # Appending site_id to order data for for further preprocessing of data (Ulu Bernam station)
        # self.order_data = InputHandler.get_site_id(order_data=self.order_data).copy()


        # preprocess data to prepare order tagging and grouping for multiload and normal order in beginning first loop
        if self.loop_status.split('_')[1] == 'First' and self.opo2_first == False :
            if self.capacity == self.largest_capacity and self.post_process == False and self.multiload_all_tanker == False and self.normal_all_tanker == False:
                iteration = "sequential_first"
            elif self.multiload_all_tanker == True and self.capacity != self.largest_capacity:
                iteration = "multiload_first"
            elif self.normal_all_tanker == True and self.capacity != self.largest_capacity:
                iteration = "all_cap_first"
            else :
                iteration = "sequential_loop"
            
            if iteration == "multiload_first" or iteration == "sequential_first" or iteration == "all_cap_first" :
                
                
                ori_order = self.order_data.copy()
                self.order_data = self.order_data[self.order_data['Order Type'] != "SMP"].copy()
                self.order_data = self.order_data.drop(['Order Type'], axis=1)
            
                for idx, order in self.order_data.iterrows():
                    End_Stock_Day = end_day_stock_1(order, self.inventory_data_tomorrow, self.forecast_data)
                    self.order_data.at[idx,'End_Stock_Day'] = End_Stock_Day
                self.order_data = self.order_data[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_pre']]
                
                print("[PreprocessNorthern] Subset and pre-process SMP orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                smp_order = ori_order[ori_order['Order Type'] == "SMP"].copy()
                smp_order = smp_order.drop(['Order Type'], axis=1)  

                if len(smp_order) > 0 :                
                    for idx, order in smp_order.iterrows():
                        try:
                            End_Stock_Day = end_day_stock_1(order, self.inventory_data_tomorrow, self.forecast_data)
                            smp_order.at[idx,'End_Stock_Day'] = End_Stock_Day
                        except Exception as e:
                            print("[PreprocessNorthern] Error in End Stock Day calculation for {}. {}".format(idx,e))
                            smp_order.at[idx,'End_Stock_Day'] = 0         
                    smp_order = smp_order[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_pre']]
                    
                    print("[PreprocessNorthern] Preprocessing SMP orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    group_count_smp = (smp_order.groupby('Ship-to').agg({"Ship-to": "count", "Material":"nunique"})).rename(columns={"Ship-to":"number_rows", "Material":"number_material"}).reset_index() 
        
                    try :
                        print("[PreprocessNorthern] Preprocessing single SMP orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                        scenario1_smp = group_count_smp[(group_count_smp['number_rows']<2)]
                        normal_order_smp = smp_order[smp_order['Ship-to'].isin(scenario1_smp['Ship-to'])] 
                        if normal_order_smp.empty == True:
                            raise RuntimeError('Empty dataframe')
                        normal_order_smp['condition'] = 'smp_normal'
                        normal_order_smp['grouping'] = 'single'
                        normal_order_smp['order_status'] = 'smp'
                        smp_list = normal_order_smp['Ship-to'].astype(str).unique()
                    except Exception as e:
                        print("[PreprocessNorthern] NO single SMP orders for this DMR due to : {}.".format(e))
                        normal_order_smp = pd.DataFrame()
                        smp_list = []
                    
                    try :
                        print("[PreprocessNorthern] Preprocessing two_rows_one_prods SMP orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                        scenario2_smp = group_count_smp[(group_count_smp['number_rows']==2) & (group_count_smp['number_material']==2)]
                        smp_order_2 = smp_order[smp_order['Ship-to'].isin(scenario2_smp['Ship-to'])]
                        if smp_order_2.empty == True:
                            raise RuntimeError('Empty dataframe') 
                        smp_group = smp_order_2.groupby('Ship-to', as_index=False).agg(Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'])
                        smp_group['condition'] = 'smp_two_rows_two_prods'
                        smp_group['grouping'] = 'combined'
                        smp_group['order_status'] = 'smp'
                        smp_duplicate2 = smp_order_2.copy()
                        smp_duplicate2['condition'] = 'smp_two_rows_two_prods'
                        smp_duplicate2['grouping'] = 'combined'
                        smp_duplicate2['order_status'] = 'smp'
                        smp_list2 = smp_duplicate2['Ship-to'].astype(str).unique()
                        print("[PreprocessNorthern] Preprocessing two_rows_one_prods SMP orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                    except Exception as e:
                        print("[PreprocessNorthern] NO two_rows_one_prods SMP orders for this DMR due to : {}.".format(e))
                        smp_group = pd.DataFrame() 
                        smp_duplicate2 = pd.DataFrame()  
                        smp_list2 = []

                    try :
                        print("[PreprocessNorthern] Preprocessing multi_rows_multi_prods SMP orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                        scenario3_smp = group_count_smp[(group_count_smp['number_rows']>1) & (group_count_smp['number_material']>=1)]
                        smp_order_3 = smp_order[smp_order['Ship-to'].isin(scenario3_smp['Ship-to'])]
                        valid_smp_list = [int(y) for x in [smp_list, smp_list2] for y in x]
                        smp_order_3 = smp_order_3[~smp_order_3['Ship-to'].isin(valid_smp_list)]
                        if smp_order_3.empty == True:
                            raise RuntimeError('Empty dataframe') 
                        smp_group_multi = smp_order_3.groupby('Ship-to', as_index=False).agg(Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'])
                        smp_group_multi['condition'] = 'smp_multi_rows_multi_prods'
                        smp_group_multi['grouping'] = 'combined'
                        smp_group_multi['order_status'] = 'smp'
                        smp_duplicate_multi = smp_order_3.copy()
                        smp_duplicate_multi['condition'] = 'smp_multi_rows_multi_prods'
                        smp_duplicate_multi['grouping'] = 'combined'
                        smp_duplicate_multi['order_status'] = 'smp'
                        print("[PreprocessNorthern] Preprocessing multi_rows_multi_prods SMP orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                    except Exception as e:
                        print("[PreprocessNorthern] NO multi_rows_multi_prods SMP orders for this DMR due to : {}.".format(e))
                        smp_group_multi = pd.DataFrame() 
                        smp_duplicate_multi = pd.DataFrame()
                else :
                    print("[PreprocessNorthern] NO any SMP orders for this DMR.")
                    normal_order_smp = pd.DataFrame()
                    smp_group = pd.DataFrame() 
                    smp_duplicate2 = pd.DataFrame() 
                    smp_group_multi = pd.DataFrame() 
                    smp_duplicate_multi = pd.DataFrame()     

                print("[PreprocessNorthern] Preprocessing SMP orders for {} in {} has completed...".format(self.loop_status,self.txn_id))

                # Pre-process ASR orders to identify underlying order scenarios
                print("[PreprocessNorthern] Subset and pre-process ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))

                def custom_agg(x):
                    names = {'number_rows': x['Ship-to'].count(),
                             'number_material': x['Material'].nunique(),
                             'material_type': x['Material'].min(),
                             }
                    return pd.Series(names, index=['number_rows', 'number_material', 'material_type'])

                # Grouping by each station and which terminal it's getting orders from
                group_count = self.order_data.groupby(
                    ['Ship-to', 'terminal', 'long_distance']).apply(custom_agg).sort_values('Ship-to').reset_index()

                # Subset Long Distance orders to cater for LDN973 and LDFIRST delivery orders
                group_count_LD = group_count.query("long_distance != 'non_LD'").copy()
                # Subset non Long Distance orders
                group_count_nonLD = group_count[~group_count.apply(tuple,1).isin(group_count_LD.apply(tuple,1))]


                # Subset orders for Ulu Bernam station, to cater for first trip later in route_generate_northern
                # group_count_ulu_bernam = group_count.query("site_id == 'RYB0424'").copy() 
                # group_count = group_count[~group_count.apply(tuple,1).isin(group_count_ulu_bernam.apply(tuple,1))]
                

                # ------------------------------------ LONG DISTANCE ORDERS -------------------------------------------
                # Pre-process station with only 1 single LD Normal order
                try:
                    print("[PreprocessNorthern] Preprocessing (LD) single ASR Normal orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario0_LD = group_count_LD[(group_count_LD['number_rows'] == 1)][['Ship-to', 'terminal']]
                    normal_order_LD = self.order_data.merge(scenario0_LD)
                    # Remove same station for non_LD orders 
                    normal_order_LD = normal_order_LD[normal_order_LD['long_distance'] != 'non_LD']
                    normal_order_LD['Multiload_Status'] = 'Normal'  ## fixed for LDN973
                    normal_order_LD['condition'] = 'normal'
                    normal_order_LD['grouping'] = 'single_LD'
                    normal_order_LD['order_status'] = 'normal'
                    if normal_order_LD.empty == True:
                        raise RuntimeError('Empty dataframe')
                    print("[PreprocessNorthern] Preprocessing (LD) single ASR orders for {} in {} has completed...".format(self.loop_status,self.txn_id))                    
                except Exception as e:
                    print("[PreprocessNorthern] NO (LD) single ASR orders for this DMR due to : {}.".format(e))
                    normal_order_LD = pd.DataFrame() 
                
                # TESTING REQUIRED!!!
                # Pre-process station with only LD 2 rows 1 product (Multiload)
                try :
                    print("[PreprocessNorthern] Preprocessing (LD) ASR two_rows_one_prods orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario1_LD = group_count_LD[
                        (group_count_LD['number_rows'] == 2) & (group_count_LD['number_material'] == 1)][['Ship-to', 'terminal']]
                    self.order_data_2_1_LD = self.order_data.merge(scenario1_LD)
                    self.order_data_2_1_LD['Multiload_Status'] = 'Multiload'
                    self.order_data_2_1_LD['condition'] = 'two_rows_one_prods'
                    #self.order_data_2_1['retain_hour'] = pd.to_numeric(self.order_data_2_1['retain_hour'])
                    multiload_order_min_retain = self.order_data_2_1_LD.loc[self.order_data_2_1_LD.groupby(['Cloud2','Ship-to']).retain_hour.idxmin()].reset_index(drop=True)
                    multiload_order_max_retain = self.order_data_2_1_LD.loc[self.order_data_2_1_LD.groupby(['Cloud2','Ship-to']).retain_hour.idxmax()].reset_index(drop=True)
                    multiload_order_min_retain['grouping'] = 'single_1_LD'
                    multiload_order_max_retain['grouping'] = 'single_2_LD'
                    self.order_data_2_1_LD = pd.concat([multiload_order_min_retain, multiload_order_max_retain], axis=0)
                    self.order_data_2_1_LD['order_status'] = 'multiload'
                    if self.order_data_2_1_LD.empty == True:
                        raise RuntimeError('Empty dataframe')
                    print("[PreprocessNorthern] Preprocessing (LD) ASR two_rows_one_prods orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                except Exception as e:
                    print("[PreprocessSouthern] NO (LD) ASR two_rows_one_prods orders for this DMR due to : {}.".format(e))
                    self.order_data_2_1_LD = pd.DataFrame()   

                # Pre-process station with only LD 2 rows 2 product (Multiproduct)
                try :                 
                    print("[PreprocessNorthern] Preprocessing (LD) ASR two_rows_two_prods orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario2_LD = group_count_LD[
                        (group_count_LD['number_rows'] == 2) & (group_count_LD['number_material'] == 2)][['Ship-to', 'terminal']]
                    self.order_data_2_2_LD = self.order_data.merge(scenario2_LD)
                    self.order_data_2_2_LD = self.order_data_2_2_LD.groupby('Ship-to', as_index=False).agg(Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'])
                    self.order_data_2_2_LD['condition'] = 'two_rows_two_prods'
                    self.order_data_2_2_LD['grouping'] = 'combined_LD'
                    self.order_data_2_2_LD['order_status'] = 'normal'
                    # ori_data_full
                    original_duplicate2_2_LD = pd.merge(self.order_data, scenario2_LD, how='inner') # ori data 2               
                    original_duplicate2_2_LD['condition'] = 'two_rows_two_prods'
                    original_duplicate2_2_LD['grouping'] = 'combined_LD'
                    original_duplicate2_2_LD['order_status'] = 'normal'
                    if original_duplicate2_2_LD.empty == True:
                        raise RuntimeError('Empty dataframe')                    
                    print("[PreprocessNorthern] Preprocessing (LD) ASR two_rows_two_prods orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                except Exception as e:
                    print("[PreprocessNorthern] NO (LD) ASR two_rows_two_prods orders for this DMR due to : {}.".format(e))
                    self.order_data_2_2_LD = pd.DataFrame()   
                    original_duplicate2_2_LD = pd.DataFrame()



                # ------------------------------------ NORMAL ORDERS --------------------------------------------------
                # Preprocess for single ASR orders ( 1 row 1 product )
                try:
                    print("[PreprocessNorthern] Preprocessing single ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario0 = group_count_nonLD[(group_count_nonLD['number_rows']==1)][['Ship-to', 'terminal']]
                    normal_order = self.order_data.merge(scenario0)
                    normal_order['condition'] = 'normal'
                    normal_order['grouping'] = 'single'
                    normal_order['order_status'] = 'normal'
                    if normal_order.empty == True:
                        raise RuntimeError('Empty dataframe')
                    print("[PreprocessNorthern] Preprocessing single ASR orders for {} in {} has completed...".format(self.loop_status,self.txn_id))                    
                except Exception as e:
                    print("[PreprocessNorthern] NO single ASR orders for this DMR due to : {}.".format(e))
                    normal_order = pd.DataFrame()                                     

                # Preprocess for multiload ASR orders ( 2 rows 1 products )    
                try :
                    print("[PreprocessNorthern] Preprocessing two_rows_one_prods ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario1 = group_count_nonLD[(group_count_nonLD['number_rows']==2) & (group_count_nonLD['number_material']==1)][['Ship-to', 'terminal']]
                    self.order_data_2_1 = self.order_data.merge(scenario1)
                    self.order_data_2_1['Multiload_Status'] = 'Multiload'
                    self.order_data_2_1['condition'] = 'two_rows_one_prods'
                    #self.order_data_2_1['retain_hour'] = pd.to_numeric(self.order_data_2_1['retain_hour'])
                    multiload_order_min_retain = self.order_data_2_1.loc[self.order_data_2_1.groupby(['Cloud2','Ship-to']).retain_hour.idxmin()].reset_index(drop=True)
                    multiload_order_max_retain = self.order_data_2_1.loc[self.order_data_2_1.groupby(['Cloud2','Ship-to']).retain_hour.idxmax()].reset_index(drop=True)
                    multiload_order_min_retain['grouping'] = 'single_1'
                    multiload_order_max_retain['grouping'] = 'single_2'
                    self.order_data_2_1 = pd.concat([multiload_order_min_retain, multiload_order_max_retain], axis=0)
                    self.order_data_2_1['order_status'] = 'multiload'
                    if self.order_data_2_1.empty == True:
                        raise RuntimeError('Empty dataframe')
                    print("[PreprocessNorthern] Preprocessing two_rows_one_prods ASR orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                except Exception as e:
                    print("[PreprocessNorthern] NO two_rows_one_prods ASR orders for this DMR due to : {}.".format(e))
                    self.order_data_2_1 = pd.DataFrame()   

                # Preprocess for multiproduct ASR orders ( 2 rows 2 products )    
                try :                 
                    print("[PreprocessNorthern] Preprocessing two_rows_two_prods ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario2 = group_count_nonLD[(group_count_nonLD['number_rows']==2) & (group_count_nonLD['number_material']==2)][['Ship-to', 'terminal']]
                    self.order_data_2_2 = self.order_data.merge(scenario2)
                    self.order_data_2_2 = self.order_data_2_2.groupby(['Ship-to'], as_index=False).agg(Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'])
                    self.order_data_2_2['condition'] = 'two_rows_two_prods'
                    self.order_data_2_2['grouping'] = 'combined'
                    self.order_data_2_2['order_status'] = 'normal'
                    original_duplicate2_2 = self.order_data.merge(scenario2) # ori data 2, need to check on different terminal 
                    original_duplicate2_2['condition'] = 'two_rows_two_prods'
                    original_duplicate2_2['grouping'] = 'combined'
                    original_duplicate2_2['order_status'] = 'normal'
                    if original_duplicate2_2.empty == True:
                        raise RuntimeError('Empty dataframe')                    
                    print("[PreprocessNorthern] Preprocessing two_rows_two_prods ASR orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                except Exception as e:
                    print("[PreprocessNorthern] NO two_rows_two_prods ASR orders for this DMR due to : {}.".format(e))
                    self.order_data_2_2 = pd.DataFrame()   
                    original_duplicate2_2 = pd.DataFrame()

                # Preprocess for 3 rows 3 products with (1 U97 , 1 U95 and 1 ADO orders) all combined
                try:
                    print("[PreprocessNorthern] Preprocessing three_rows_three_prods ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    processed_order_data_3_97 = pd.DataFrame()
                    original_duplicate3_97 = pd.DataFrame()
                    scenario3_97 = group_count_nonLD[(group_count_nonLD['number_rows'] == 3) & (group_count_nonLD['number_material'] == 3)][['Ship-to', 'terminal']]
                    self.order_data_3_97_all = self.order_data.merge(scenario3_97)
                    order_data_3_grouped = self.order_data_3_97_all.groupby('Ship-to', as_index=False).agg(
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'])
                    order_data_3_grouped['condition'] = 'three_rows_three_prods'
                    order_data_3_grouped['grouping'] = 'combined'
                    order_data_3_grouped['order_status'] = 'normal'
                    processed_order_data_3_97 = order_data_3_grouped
                    original_duplicate3_97 = self.order_data_3_97_all[
                        self.order_data_3_97_all['Ship-to'].isin(scenario3_97['Ship-to'])]  # ori data 3
                    original_duplicate3_97['condition'] = 'three_rows_three_prods'
                    original_duplicate3_97['grouping'] = 'combined'
                    original_duplicate3_97['order_status'] = 'normal'
                    if processed_order_data_3_97.empty == True:
                        raise RuntimeError('Empty dataframe')
                    print(
                        "[PreprocessNorthern] Preprocessing three_rows_three_prods ASR orders for {} in {} has completed...".format(
                            self.loop_status, self.txn_id))
                except Exception as e:
                    print(
                        "[PreprocessNorthern] NO three_rows_three_prods ASR orders for this DMR due to : {}.".format(
                            e))
                    processed_order_data_3_97 = pd.DataFrame()
                    original_duplicate3_97 = pd.DataFrame() 

                # # Testing 
                # # Preprocess for 3 rows 3 products with (1 U97 , 1 U95 and 1 ADO orders) but separated for Single 97
                # try:
                #     print("[PreprocessNorthern] Preprocessing three_rows_three_prods ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                #     processed_order_data_3_97 = pd.DataFrame()
                #     original_duplicate3_97 = pd.DataFrame()
                #     scenario3_97 = group_count_nonLD[(group_count_nonLD['number_rows'] == 3) & (group_count_nonLD['number_material'] == 3)][['Ship-to', 'terminal']]
                #     self.order_data_3_97_all = self.order_data.merge(scenario3_97)
                #     order_data_3_97 = self.order_data_3_97_all[self.order_data_3_97_all['Material'] == 70000011]
                #     order_data_3_97['condition'] = 'three_rows_three_prods'
                #     order_data_3_97['grouping'] = 'single_97' # need to check for 97 to be single or can combine with other prod
                #     order_data_3_97['order_status'] = 'normal'
                #     order_data_3 = self.order_data_3_97_all[self.order_data_3_97_all['Material'] != 70000011]
                #     order_data_3_grouped = order_data_3.groupby('Ship-to', as_index=False).agg(
                #         Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'])
                #     order_data_3_grouped['condition'] = 'three_rows_three_prods'
                #     order_data_3_grouped['grouping'] = 'combined'
                #     order_data_3_grouped['order_status'] = 'normal'
                #     processed_order_data_3_97 = pd.concat([order_data_3_97, order_data_3_grouped], axis=0)
                #     original_duplicate3_97 = self.order_data_3_97_all[
                #         self.order_data_3_97_all['Ship-to'].isin(scenario3_97['Ship-to']) & (
                #             ~self.order_data_3_97_all['Material'].isin([70000011]))]  # ori data 3
                #     original_duplicate3_97['condition'] = 'three_rows_three_prods'
                #     original_duplicate3_97['grouping'] = 'combined'
                #     original_duplicate3_97['order_status'] = 'normal'
                #     original_duplicate3_97 = pd.concat([original_duplicate3_97, order_data_3_97], axis=0)
                #     if processed_order_data_3_97.empty == True:
                #         raise RuntimeError('Empty dataframe')
                #     print(
                #         "[PreprocessNorthern] Preprocessing three_rows_three_prods ASR orders for {} in {} has completed...".format(
                #             self.loop_status, self.txn_id))
                # except Exception as e:
                #     print(
                #         "[PreprocessNorthern] NO three_rows_three_prods ASR orders for this DMR due to : {}.".format(
                #             e))
                #     processed_order_data_3_97 = pd.DataFrame()
                #     original_duplicate3_97 = pd.DataFrame()
                
                # Preprocess for 3 rows 2 products 
                try :     
                    print("[PreprocessNorthern] Preprocessing three_rows_two_prods ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario3 = group_count_nonLD[
                        (group_count_nonLD['number_rows'] == 3) & (group_count_nonLD['number_material'] == 2)][['Ship-to', 'terminal']]
                    self.order_data_3 = self.order_data.merge(scenario3) # ori data 3
                    if self.order_data_3.empty == True:
                        raise RuntimeError('Empty dataframe')                     
                    self.order_data_3['condition'] = 'three_rows_two_prods'
                    processed_order_data3 = pd.DataFrame()
                    self.order_data_3_grouping = pd.DataFrame()
                    for customer, data in self.order_data_3.groupby(['Ship-to']):
                        df = self.order_data_3[self.order_data_3['Ship-to']==customer]
                        if len(data[data['Material']==70020771]) > len(data[data['Material']==70100346]):
                            next_retain = df['retain_hour'][df['Material']==70100346].values
                            # df['retain_hour'] = pd.to_numeric(df['retain_hour'])
                            first_trip = df.loc[[(df['retain_hour'][df['Material']==70020771]).idxmin()]]
                            first_trip['order_status'] = 'multiload'
                            second_trip = df[df['Material']==70100346]
                            second_trip['order_status'] = 'normal'
                            if (next_retain <= (min(df['retain_hour'][df['Material']==70020771]) + min(df['runout_duration'][df['Material']==70020771]))):
                                combine_trip = pd.concat([first_trip, second_trip], axis=0)
                                group_trip = combine_trip.groupby('Ship-to', as_index=False).agg({**Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'], **Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_add_agg'][0]})
                                # df['retain_hour'] = pd.to_numeric(df['retain_hour'])
                                third_trip = df.loc[[(df['retain_hour'][df['Material']==70020771]).idxmax()]]
                                combine_trip['grouping'] = 'combined'
                                group_trip['condition'] = 'three_rows_two_prods'
                                group_trip['grouping'] = 'combined'
                                third_trip['grouping'] = 'single'
                                third_trip['order_status'] = 'multiload'
                                combine_trip = combine_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                group_trip = group_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                third_trip = third_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                self.order_data_3_grouping = pd.concat([self.order_data_3_grouping, combine_trip, third_trip], axis=0)
                                processed_order_data3 = pd.concat([processed_order_data3, group_trip, third_trip], axis=0)
                            else:
                                # df['retain_hour'] = pd.to_numeric(df['retain_hour'])
                                third_trip = df.loc[[(df['retain_hour'][df['Material']==70020771]).idxmax()]]
                                third_trip['order_status'] = 'multiload'
                                combine_trip = pd.concat([third_trip, second_trip], axis=0)
                                group_trip = combine_trip.groupby('Ship-to', as_index=False).agg({**Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'], **Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_add_agg'][0]})
                                combine_trip['grouping'] = 'combined'
                                group_trip['condition'] = 'three_rows_two_prods'
                                group_trip['grouping'] = 'combined'
                                first_trip['grouping'] = 'single'
                                combine_trip = combine_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                group_trip = group_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                first_trip = first_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                self.order_data_3_grouping = pd.concat([self.order_data_3_grouping, combine_trip, first_trip], axis=0)
                                processed_order_data3 = pd.concat([processed_order_data3, group_trip, first_trip], axis=0)       
                        else:
                            next_retain = df['retain_hour'][df['Material']==70020771].values
                            # df['retain_hour'] = pd.to_numeric(df['retain_hour'])
                            first_trip = df.loc[[(df['retain_hour'][df['Material']==70100346]).idxmin()]]
                            first_trip['order_status'] = 'multiload'
                            second_trip = df[df['Material']==70020771]
                            second_trip['order_status'] = 'normal'
                            if (next_retain <= (min(df['retain_hour'][df['Material']==70100346]) + (min(df['runout_duration'][df['Material']==70100346])))):
                                combine_trip = pd.concat([first_trip, second_trip], axis=0)
                                group_trip = combine_trip.groupby('Ship-to', as_index=False).agg({**Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'], **Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_add_agg'][0]})
                                # df['retain_hour'] = pd.to_numeric(df['retain_hour'])
                                third_trip = df.loc[[(df['retain_hour'][df['Material']==70100346]).idxmax()]]
                                combine_trip['grouping'] = 'combined'
                                group_trip['condition'] = 'three_rows_two_prods'
                                group_trip['grouping'] = 'combined'
                                third_trip['grouping'] = 'single'
                                third_trip['order_status'] = 'multiload'
                                combine_trip = combine_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                group_trip = group_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                third_trip = third_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                self.order_data_3_grouping = pd.concat([self.order_data_3_grouping, combine_trip, third_trip], axis=0)
                                processed_order_data3 = pd.concat([processed_order_data3, group_trip, third_trip], axis=0)
                            else:
                                # df['retain_hour'] = pd.to_numeric(df['retain_hour'])
                                third_trip = df.loc[[(df['retain_hour'][df['Material']==70100346]).idxmax()]]
                                third_trip['order_status'] = 'multiload'
                                combine_trip = pd.concat([third_trip, second_trip], axis=0)
                                group_trip = combine_trip.groupby('Ship-to', as_index=False).agg({**Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'], **Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_add_agg'][0]})
                                combine_trip['grouping'] = 'combined'
                                group_trip['condition'] = 'three_rows_two_prods'
                                group_trip['grouping'] = 'combined'
                                first_trip['grouping'] = 'single'
                                combine_trip = combine_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                group_trip = group_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                first_trip = first_trip[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                                self.order_data_3_grouping = pd.concat([self.order_data_3_grouping, combine_trip, first_trip], axis=0)
                                processed_order_data3 = pd.concat([processed_order_data3, group_trip, first_trip], axis=0)
                    print("[PreprocessNorthern] Preprocessing three_rows_two_prods ASR orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                except Exception as e:
                    print("[PreprocessNorthern] NO three_rows_two_prods ASR orders for this DMR due to : {}.".format(e))
                    self.order_data_3_grouping = pd.DataFrame()   
                    processed_order_data3 = pd.DataFrame()


                # Preprocess for 4 rows 2 products 
                try :    
                    print("[PreprocessNorthern] Preprocessing four_rows_two_prods ASR orders for {} in {} is being initiated...".format(self.loop_status,self.txn_id))
                    scenario4 = group_count_nonLD[
                        (group_count_nonLD['number_rows'] == 4) & (group_count_nonLD['number_material'] == 2)][['Ship-to', 'terminal']]
                    self.order_data_4 = self.order_data.merge(scenario4) # ori data 4
                    if self.order_data_4.empty == True:
                        raise RuntimeError('Empty dataframe')                     
                    self.order_data_4['condition'] = 'four_rows_two_prods'
                    self.order_data_4['grouping'] = 'combined'
                    processed_order_data4 = pd.DataFrame()
                    self.order_data_4_grouping = pd.DataFrame()
                    for customer, data in self.order_data_4.groupby(['Ship-to']):
                        df = self.order_data_4[self.order_data_4['Ship-to']==customer]
                        df = df.sort_values(['Ship-to', 'Material', 'retain_hour'])
                        df = df.assign(sequence=np.arange(len(df)) % 2 + 1)
                        df['identifier'] = df['Ship-to'].astype(str) + "_" + df['sequence'].astype(str)
                        df['grouping'] = df['grouping'] + '_' + df['sequence'].astype(str)
                    
                        group_trip = df.groupby('identifier', as_index=False).agg({**Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_basic_agg'], **Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_add_agg'][1]})
                        group_trip['condition'] = 'four_rows_two_prods'
                        group_trip['grouping'] = 'combined'
                        group_trip['grouping'] = group_trip.grouping + '_' + group_trip.groupby('Ship-to').cumcount().add(1).astype(str)
                        group_trip = group_trip.drop(['identifier'], axis=1)
                        df = df.drop(['identifier','sequence'], axis=1)
                        self.order_data_4_grouping = pd.concat([self.order_data_4_grouping, df], axis=0)
                        processed_order_data4 = pd.concat([processed_order_data4, group_trip], axis=0)
                        self.order_data_4_grouping['order_status'] = 'multiload'
                        processed_order_data4['order_status'] = 'multiload'
                        self.order_data_4_grouping = self.order_data_4_grouping[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                        processed_order_data4 = processed_order_data4[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_post']]
                    print("[PreprocessNorthern] Preprocessing four_rows_two_prods ASR orders for {} in {} has completed...".format(self.loop_status,self.txn_id))
                except Exception as e:
                    print("[PreprocessNorthern] NO four_rows_two_prods ASR orders for this DMR due to : {}.".format(e))
                    self.order_data_4_grouping = pd.DataFrame()   
                    processed_order_data4 = pd.DataFrame()  
                                
                print("[PreprocessNorthern] Preprocessing ASR orders for {} in {} has completed...".format(self.loop_status,self.txn_id))  

                # ori_data_multiload
                ori_order_multiload = pd.concat([smp_duplicate2, original_duplicate2_2, original_duplicate2_2_LD, original_duplicate3_97, self.order_data_3_grouping, self.order_data_4_grouping], axis=0).reset_index(drop=True)               

                # processed_order_multiload
                processed_order_multiload = pd.concat([normal_order_smp, smp_group, normal_order, normal_order_LD, self.order_data_2_1, self.order_data_2_1_LD, self.order_data_2_2, self.order_data_2_2_LD, processed_order_data_3_97, processed_order_data3, processed_order_data4], axis=0).reset_index(drop=True) 

                # ori_data_full
                full_list = pd.concat([normal_order_smp, ori_order_multiload, self.order_data_2_1, self.order_data_2_1_LD , normal_order, normal_order_LD], axis=0).reset_index(drop=True) 

                # Check if full list has the same number of rows with self.order_data
                if len(full_list) == len(self.order_data):
                    print(f"[PreprocessNorthern] Order preprocessing check done. full_list is the same with order_data.")
                else:
                    print(f"[PreprocessNorthern] Check order compilation steps. Different number of rows between full_list :  {len(full_list)} rows and order_data: {len(self.order_data)} rows.")

                # Merging ori_order_multiload, processed_order_multiload and full list with master_terminal_data
                dfs = [ori_order_multiload, processed_order_multiload, full_list]
                renames_dfs = []
                for df in dfs:
                    if df.empty != True:
                        df = pd.merge(df.drop(['acc'], axis=1), self.master_terminal_data, 'left', left_on=['Ship-to'], right_on=['acc'])
                        # terminal values correction
                        df = self.orderdata_terminal_correction(df)
                        df.rename(columns = {'terminal_PRIMAX_97':'terminal_97', 'terminal':'terminal_95'}, inplace=True)
                        df = df.drop(columns=['terminal_BIODIESEL_B10', 'terminal_PRIMAX_95'], errors='ignore')
                    renames_dfs.append(df)

                ori_order_multiload, processed_order_multiload, full_list = renames_dfs[0].copy(), renames_dfs[1].copy(), renames_dfs[2].copy()


                full_list_export = full_list.copy()
                full_list_export['Txn_ID'] = self.txn_id
                full_list_export = full_list_export.drop(['runout_duration'],axis = 1)

                full_list_export['DMR_Date'] = self.date
                full_list_export['Loop_Counter'] = self.loop_status
                full_list_export['Job_StartTime'] = self.start_time
                full_list_export['Txn_ID'] = self.txn_id  
                full_list_export['Order_Type'] = 'RE'
                # full_list_export['long_distance'] = "non_LD"
                time_list = ['Opening_Time','Closing_Time']
                # for col in time_list :
                #     full_list_export[col] = change_datetimetofloat_format(full_list_export[col])   
                full_list_export.rename(columns = {'terminal_97':'Terminal_97', 'terminal_95':'Terminal_Default'}, inplace=True) 
                full_list_export = full_list_export[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['first_loop_export']]

                full_list_export['id_merge'] = full_list_export['Ship-to'].astype(str) + '_' + full_list_export['Material'].astype(str) + '_' + full_list_export['Qty'].astype(str) + '_' + full_list_export['Multiload_Status'].astype(str) + '_' + full_list_export['retain_ori'].astype(str) 
                self.order_id_df['id_merge'] = self.order_id_df['retail'].astype(str) + '_' + self.order_id_df['product'].astype(str) + '_' + self.order_id_df['volume'].astype(str) + '_' + self.order_id_df['multi_load_id'].astype(str) + '_' + self.order_id_df['retain'].astype(str) 
                id_merge = self.order_id_df[['id','id_merge']]
                                                              
                full_list_export = pd.merge(full_list_export, id_merge, 'left', on=['id_merge'])
                full_list_export = full_list_export.drop(['id_merge'], axis=1)
                
                if len(self.unmatched_commercial) > 0:
                    self.unmatched_commercial = self.unmatched_commercial[['Route_ID','Terminal_97','Terminal_Default','No_Compartment','Tanker_Cap','Order_ID','Destinations','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Utilization','Retain_Hour','Runout','End_Stock_Day','Travel_Time','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Min_Retain_Hour','Min_Runout','DMR_Date','Loop_Counter','Job_StartTime','Txn_ID','Region','Order_Type','delta','id']]                        
                else :
                    self.unmatched_commercial = pd.DataFrame()
                
                if len(self.commercial_data) > 0:
                    comm_list_export = self.commercial_data.append(self.unmatched_commercial)
                    comm_list_export['retain_ori'] = comm_list_export['Retain_Hour']
                    comm_list_export['Total_weight'] = 0
                    comm_list_export['distance_terminal_u95'] = 0
                    comm_list_export = comm_list_export.rename(columns = {'Destinations':'Ship-to', 'Product':'Material', 'Order_Quantity':'Qty', 'Retain_Hour':'retain_hour', 'Cloud':'Cloud2', 'Accessibility':'accessibility'})
                    comm_list_export = comm_list_export[['Ship-to', 'Priority', 'Terminal_97', 'Terminal_Default', 'Material', 'Qty', 'Priority', 'retain_hour', 'retain_ori', 'Runout', 'Opening_Time', 'Closing_Time', 'Total_weight', 'End_Stock_Day', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status', 'long_distance', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'Txn_ID', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Order_Type', 'id']]
                    full_list_export = full_list_export.append(comm_list_export)

                DBConnector.save(full_list_export, Config.TABLES['processed_ori_orders']["tablename"], if_exists='append')

            #######  First loop second run #####################
            
            else :
                ori_order = self.order_data.copy()

                try:
                    print(
                        "[PreprocessNorthern] Subset and pre-process Retail orders for {} in {} is being initiated...".format(
                            self.loop_status, self.txn_id))
                    self.order_data = self.order_data[self.order_data['order_status'] != "smp"].copy()
                    for idx, order in self.order_data.iterrows():
                        try:
                            End_Stock_Day = end_day_stock_1(order, self.inventory_data_tomorrow, self.forecast_data)
                            self.order_data.at[idx, 'End_Stock_Day'] = End_Stock_Day
                        except Exception as e:
                            print(
                                "[PreprocessNorthern] Error in End Stock Day calculation for {}. {}".format(idx, e))
                            self.order_data.at[idx, 'End_Stock_Day'] = 0
                    self.order_data = self.order_data[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                except Exception as e:
                    print(
                        "[PreprocessNorthern] NO Retail orders for {}. {} rows in data : {}".format(self.loop_status,
                                                                                                    len(self.order_data),
                                                                                                    e))
                    self.order_data = pd.DataFrame()

                try:
                    print(
                        "[PreprocessNorthern] Subset and pre-process SMP orders for {} in {} is being initiated...".format(
                            self.loop_status, self.txn_id))
                    smp_order = ori_order[ori_order['order_status'] == "smp"].copy()
                    for idx, order in smp_order.iterrows():
                        try:
                            End_Stock_Day = end_day_stock_1(order, self.inventory_data_tomorrow, self.forecast_data)
                            smp_order.at[idx, 'End_Stock_Day'] = End_Stock_Day
                        except Exception as e:
                            print(
                                "[PreprocessNorthern] Error in End Stock Day calculation for {}. {}".format(idx, e))
                            smp_order.at[idx, 'End_Stock_Day'] = 0
                    smp_order = smp_order[
                        ['Ship-to', 'Material', 'Qty', 'retain_hour', 'Runout', 'runout_duration', 'Total_weight',
                         'End_Stock_Day', 'acc', 'Cloud2', 'distance_terminal_u95', 'accessibility', 'Tank_Status',
                         'long_distance', 'Multiload_Status', 'condition', 'grouping', 'order_status']]
                except Exception as e:
                    print(
                        "[PreprocessNorthern] NO SMP orders for {}. {} rows in data : {}".format(self.loop_status,
                                                                                                 len(smp_order), e))
                    smp_order = pd.DataFrame()

                if len(self.order_data) > 0 or len(smp_order) > 0:
                    remaining_order = pd.concat([self.order_data, smp_order])
                    single_group = ['single_97_LD', 'single', 'single_1', 'single_2']
                    try:
                        remain_normal = remaining_order[remaining_order['grouping'].isin(single_group)]
                        if remain_normal.empty == True:
                            raise RuntimeError('No Single Load Orders')
                    except Exception as e:
                        print(
                            "[PreprocessNorthern] NO Single load orders {} : {}".format(self.loop_status, e))
                        remain_normal = pd.DataFrame()

                    combine_group = ['combined_1', 'combined_2', 'combined']
                    try:
                        remain_multiload = remaining_order[remaining_order['grouping'].isin(combine_group)]
                        if remain_multiload.empty == True:
                            raise RuntimeError('No Multiload Orders')
                        remain_multiload['identifier'] = remain_multiload['Ship-to'].astype(str) + "_" + \
                                                         remain_multiload['Multiload_Status'].astype(str) + "_" + \
                                                         remain_multiload['condition'].astype(str) + "_" + \
                                                         remain_multiload['grouping'].astype(str)
                        remain_group = remain_multiload.groupby(['identifier'], as_index=False).agg(
                                    Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_basic_agg'])
                        remain_group = remain_group.drop(['identifier'], axis=1)
                        remain_multiload = remain_multiload.drop(['identifier'], axis=1)
                    except Exception as e:
                        print("[PreprocessNorthern] {}".format(e))
                        remain_multiload = pd.DataFrame()
                        remain_group = pd.DataFrame()

                # ori_data_multiload
                ori_order_multiload = remain_multiload
                # processed_order_multiload
                processed_order_multiload = pd.concat([remain_normal, remain_group], axis=0).reset_index(drop=True)
                # ori_data_full
                full_list = remaining_order

                full_list_export = pd.DataFrame()

        elif self.loop_status.split('_')[1] == 'Second':
            # TEMPORARY FIX: Filling missing Opening Time and Closing Time with other orders with same terminal
            # if not (self.order_data.query("Opening_Time.isna() == True").empty):
            #     self.order_data[['Opening_Time', 'Closing_Time']] = \
            #         self.order_data.groupby('Material')[['Opening_Time', 'Closing_Time']].apply(lambda x: x.ffill().bfill()) 

            ori_order_multiload, processed_order_multiload, full_list = self.preprocess_opo2()
        
        # further preprocess data using opo2 in the first loop 
        else:
            ori_order_multiload, processed_order_multiload, full_list = self.preprocess_opo2()

        print("[PreprocessNorthern] Preprocessing multiload orders for {} in {} has completed".format(self.loop_status, self.txn_id))

        return ori_order_multiload, processed_order_multiload, full_list

    def preprocess_opo2(self) :

        ori_order = self.order_data.copy()

        # Subset SMP orders
        try:
            print("[PreprocessNorthern] Subset and pre-process SMP orders for {} in {} is being initiated...".format(
                self.loop_status, self.txn_id))
            smp_order = ori_order[ori_order['order_status'] == 'smp'].copy()
            smp_order['ori_status'] = 'original'
            # Calculate end stock day
            for idx, order in smp_order.iterrows():
                try:
                    End_Stock_Day = end_day_stock_1(order, self.inventory_data_tomorrow, self.forecast_data)
                    smp_order.at[idx, 'End_Stock_Day'] = End_Stock_Day
                except Exception as e:
                    print("[PreprocessNorthern] Error in End Stock Day calculation for {}. {}".format(idx,e))
                    smp_order.at[idx,'End_Stock_Day'] = 0
                smp_order = smp_order[
                    Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_init']]
        except Exception as e:
            print("[PreprocessNorthern] NO SMP orders in UNSCHEDULED / UNMATCHED for {}. {} rows : {}".format(self.loop_status, len(smp_order), e))
            smp_order = pd.DataFrame()

        # Subset Retail orders
        try:
            print("[PreprocessNorthern] Subset and pre-process Retail orders for {} in {} is being initiated...".format(
                    self.loop_status, self.txn_id))
            self.order_data = ori_order[ori_order['order_status'] != "smp"]
            self.order_data['ori_status'] = 'original'
            # Calculate end stock day
            for idx, order in self.order_data.iterrows():
                try:
                    End_Stock_Day = end_day_stock_1(order, self.inventory_data_tomorrow, self.forecast_data)
                    self.order_data.at[idx,'End_Stock_Day'] = End_Stock_Day
                except Exception as e:
                    print("[PreprocessNorthern] Error in End Stock Day calculation for {}. {}".format(idx,e))
                    self.order_data.at[idx,'End_Stock_Day'] = 0
            self.order_data = self.order_data[Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_init']]
        except Exception as e:
            print("[PreprocessNorthern] NO Retail orders in UNSCHEDULED / UNMATCHED for {}. {} rows : {}".format(self.loop_status,len(self.order_data),e))
            self.order_data = pd.DataFrame()

        # Perform OPO2 operations on RETAIL NON-MULTILOAD ONLY orders
        if len(self.order_data) > 0:
            remain_order = self.order_data

            # Pre-process only single LD (U97/U95/Diesel) or 1 non long distance (U97/U95/Diesel) order
            print("[PreprocessNorthern] OPO2 single LD and non LD Normal orders for {} in {} is being initiated...".format(self.loop_status, self.txn_id))
            normal_order = self.order_data[self.order_data['condition'] == 'normal']
            processed_normal_order = pd.DataFrame()
            original_order_1 = pd.DataFrame()

            if len(normal_order) > 0:
                # Removing normal orders and keeping the remaining in remain_order variable for subsequent processing
                remain_order = remain_order[~remain_order.apply(tuple,1).isin(normal_order.apply(tuple,1))].copy()
                # Subset long distance orders and those which are not
                normal_LD = normal_order.query("condition == 'normal' & grouping == 'single_LD'").copy()
                normal_nonLD = normal_order.query("condition == 'normal' & grouping == 'single'").copy()
                
                # OPO2 station with only 1 long distance order
                if len(normal_LD) > 0:
                    # Returns dataframe with original and opo2_modified order quantity
                    normal_LD = order_permutation_northern(
                        normal_LD, self.inventory_data_tomorrow, self.forecast_data, self.sales_time_slot, self.master_all, self.date, self.loop_status, self.start_time, self.txn_id, self.activate_opo2_log, self.region)
                    normal_LD = change_retain_format(normal_LD, self.date)
                    normal_LD.insert(0, 'sequence', range(1, 1 + len(normal_LD)))
                    normal_LD['condition'] = normal_LD['condition'] + '_' + normal_LD['sequence'].astype(str)
                    normal_LD = normal_LD.drop(['sequence'], axis=1)
                    normal_LD['order_status'] = 'normal'
                    normal_LD['runout_duration'] = normal_LD.apply(lambda row: row.Runout - row.retain_hour, axis=1)
                    processed_normal_order = pd.concat([processed_normal_order, normal_LD])
                else:
                    normal_LD = pd.DataFrame() # orders with original and opo2_modified

                # OPO2 station with only 1 non long distance (U97/U95/Diesel) order
                if len(normal_nonLD) > 0:
                    normal_nonLD = order_permutation_northern(
                        normal_nonLD, self.inventory_data_tomorrow, self.forecast_data, self.sales_time_slot, self.master_all, self.date, self.loop_status, self.start_time, self.txn_id, self.activate_opo2_log, self.region)
                    normal_nonLD = change_retain_format(normal_nonLD, self.date)
                    normal_nonLD.insert(0, 'sequence', range(1, 1 + len(normal_nonLD)))
                    normal_nonLD['condition'] = normal_nonLD['condition'] + '_' + normal_nonLD['sequence'].astype(str)
                    normal_nonLD = normal_nonLD.drop(['sequence'], axis=1)
                    normal_nonLD['order_status'] = 'normal'
                    normal_nonLD['runout_duration'] = normal_nonLD.apply(
                        lambda row: row.Runout - row.retain_hour, axis=1)
                    processed_normal_order = pd.concat([processed_normal_order, normal_nonLD])
                else:
                    normal_nonLD = pd.DataFrame() # orders with original and opo2_modified

                # Subsetting columns of procssed normal orders and only extracting original orders 
                if len(processed_normal_order) > 0:
                    # processed LD and non-LD orders which includes original and opo2_modified
                    processed_normal_order = processed_normal_order[
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                    # processed/full normal orders without opo2_modified
                    original_order_1 = processed_normal_order[processed_normal_order['ori_status'] == 'original']

            print("[PreprocessNorthern] OPO2 single LD and non LD Normal orders for {} in {} has completed...".format(self.loop_status, self.txn_id))

            # Pre-process station with two_rows_two_prods orders (U97/U95/Diesel) of LD or non long distance  order
            print("[PreprocessNorthern] OPO2 two_rows_two_prods LD and non LD Normal orders for {} in {} is being initiated...".format(
                    self.loop_status, self.txn_id))
            self.order_data_2 = self.order_data[self.order_data['condition'] == 'two_rows_two_prods']
            processed_order_data_2_LD = pd.DataFrame() # LD Processed orders groupby drops 
            processed_order_data_2_nonLD = pd.DataFrame() # NON-LD Processed orders groupby drops 
            original_duplicate2_LD = pd.DataFrame() # Full expanded original and opo2_modified orders
            original_duplicate2_nonLD = pd.DataFrame() # Full expanded original and opo2_modified orders
            original_order_2 = pd.DataFrame() # Full expanded original ONLY orders
            if len(self.order_data_2) > 0:
                # Further removing 2 rows 2 prods orders and keeping the remaining in remain_order variable for subsequent processing
                remain_order = remain_order[~remain_order.apply(tuple,1).isin(self.order_data_2.apply(tuple,1))].copy()
                # Subset long distance orders and those which are not
                self.order_data_2_LD = self.order_data_2.query(
                    "condition == 'two_rows_two_prods' & grouping == 'combined_LD'").copy()
                self.order_data_2_nonLD = self.order_data_2.query(
                    "condition == 'two_rows_two_prods' & grouping == 'combined'").copy()

                # OPO2 station where a single station has orders of 2 different products (U95/Diesel/U97) and with LD tagging
                if len(self.order_data_2_LD) > 0:
                    self.order_data_2_perm = order_permutation_northern(
                        self.order_data_2_LD, self.inventory_data_tomorrow, self.forecast_data, self.sales_time_slot, self.master_all, self.date, self.loop_status, self.start_time, self.txn_id, self.activate_opo2_log, self.region)
                    self.order_data_2_perm = change_retain_format(self.order_data_2_perm, self.date)
                    original_order_2_LD = self.order_data_2_perm[self.order_data_2_perm['ori_status']=='original']
                    original_order_2_LD['runout_duration'] = original_order_2_LD.apply(lambda row: row.Runout - row.retain_hour, axis=1)
                    self.order_data_2_perm = self.order_data_2_perm[self.order_data_2_perm['retain_hour'] < self.end_day]
                    self.order_data_2_perm['runout_duration'] = self.order_data_2_perm.apply(lambda row: row.Runout - row.retain_hour, axis=1)
                    self.order_data_2_perm['identifier'] = self.order_data_2_perm['Ship-to'].astype(str) + "_" + self.order_data_2_perm['Material'].astype(str) + "_" + self.order_data_2_perm['Qty'].astype(str)
                    order_com = pd.DataFrame(
                        [[d, o1, o2] for d, identifier in self.order_data_2_perm.groupby(['Ship-to']).identifier for o1, o2 in itertools.combinations(identifier, 2)], columns=['Ship-to', 'order_1', 'order_2'])                    
                    order_com = order_com[
                        order_com['order_1'].apply(lambda x: x.split('_')[1]) != order_com['order_2'].apply(lambda x: x.split('_')[1])]
                    order_com.insert(0, 'sequence', range(1, 1 + len(order_com)))
                    merge_seq = pd.melt(
                        order_com, id_vars=['sequence','Ship-to'], value_name="order")
                    merge_seq.drop(columns="variable", inplace=True)
                    original_duplicate2_LD = pd.merge(
                        merge_seq, self.order_data_2_perm[
                            Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_pre']], how='left', left_on='order', right_on='identifier')
                    # Processed orders where we groupby drops (One drop for both products, combined)
                    processed_order_data_2_LD = original_duplicate2_LD.groupby(
                        'sequence', as_index=False).agg(
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_basic_agg'])
                    processed_order_data_2_LD['condition'] = 'two_rows_two_prods'
                    processed_order_data_2_LD['grouping'] = 'combined_LD'
                    processed_order_data_2_LD['condition'] = processed_order_data_2_LD['condition'] + '_' + processed_order_data_2_LD['sequence'].astype(str)
                    processed_order_data_2_LD.drop(columns='sequence', inplace=True)
                    # Full expanded original and opo2_modified orders
                    original_duplicate2_LD['condition'] = 'two_rows_two_prods'
                    original_duplicate2_LD['grouping'] = 'combined_LD'
                    original_duplicate2_LD['condition'] = original_duplicate2_LD['condition'] + '_' + original_duplicate2_LD['sequence'].astype(str)
                    original_duplicate2_LD.drop(columns=['sequence','order','identifier'], inplace=True)
                    # Subsetting dataframe to standardize final columns
                    processed_order_data_2_LD = processed_order_data_2_LD[
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                    original_duplicate2_LD = original_duplicate2_LD[
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                    original_order_2_LD = original_order_2_LD[
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                    original_order_2 = pd.concat([original_order_2, original_order_2_LD])
                else:
                    print("[PreprocessNorthern] OPO2 - NO (LD) ASR two_rows_two_prods orders for this loop.")
                    processed_order_data_2_LD = pd.DataFrame() # Processed orders groupby drops
                    original_duplicate2_LD = pd.DataFrame() # Full expanded original and opo2_modified orders
                    original_order_2_LD = pd.DataFrame() # Full expanded original ONLY orders

                # OPO2 station where a single station has orders of 2 non long distance other product (U95/Diesel/U97) orders
                if len(self.order_data_2_nonLD) > 0:
                    self.order_data_2_perm = order_permutation_northern(
                        self.order_data_2_nonLD, self.inventory_data_tomorrow, self.forecast_data, self.sales_time_slot, self.master_all, self.date, self.loop_status, self.start_time, self.txn_id, self.activate_opo2_log, self.region)
                    self.order_data_2_perm = change_retain_format(self.order_data_2_perm, self.date)
                    original_order_2_nonLD = self.order_data_2_perm[self.order_data_2_perm['ori_status']=='original']
                    original_order_2_nonLD['runout_duration'] = original_order_2_nonLD.apply(lambda row: row.Runout - row.retain_hour, axis=1)
                    self.order_data_2_perm = self.order_data_2_perm[self.order_data_2_perm['retain_hour'] < self.end_day]
                    self.order_data_2_perm['runout_duration'] = self.order_data_2_perm.apply(lambda row: row.Runout - row.retain_hour, axis=1)
                    self.order_data_2_perm['identifier'] = self.order_data_2_perm['Ship-to'].astype(str) + "_" + self.order_data_2_perm['Material'].astype(str) + "_" + self.order_data_2_perm['Qty'].astype(str)
                    order_com = pd.DataFrame(
                        [[d, o1, o2] for d, identifier in self.order_data_2_perm.groupby(['Ship-to']).identifier for o1, o2 in itertools.combinations(identifier, 2)], columns=['Ship-to', 'order_1', 'order_2'])                    
                    order_com = order_com[
                        order_com['order_1'].apply(lambda x: x.split('_')[1]) != order_com['order_2'].apply(lambda x: x.split('_')[1])]
                    order_com.insert(0, 'sequence', range(1, 1 + len(order_com)))
                    merge_seq = pd.melt(
                        order_com, id_vars=['sequence','Ship-to'], value_name="order")
                    merge_seq.drop(columns="variable", inplace=True)
                    original_duplicate2_nonLD = pd.merge(
                        merge_seq, self.order_data_2_perm[
                            Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_pre']], how='left', left_on='order', right_on='identifier')
                    # Processed orders where we groupby drops (One drop for both products, combined)
                    processed_order_data_2_nonLD = original_duplicate2_nonLD.groupby(
                        'sequence', as_index=False).agg(
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_basic_agg'])
                    processed_order_data_2_nonLD['condition'] = 'two_rows_two_prods'
                    processed_order_data_2_nonLD['grouping'] = 'combined'
                    processed_order_data_2_nonLD['condition'] = processed_order_data_2_nonLD['condition'] + '_' + processed_order_data_2_nonLD['sequence'].astype(str)
                    processed_order_data_2_nonLD.drop(columns='sequence', inplace=True)
                    # Full expanded original and opo2_modified orders
                    original_duplicate2_nonLD['condition'] = 'two_rows_two_prods'
                    original_duplicate2_nonLD['grouping'] = 'combined'
                    original_duplicate2_nonLD['condition'] = original_duplicate2_nonLD['condition'] + '_' + original_duplicate2_nonLD['sequence'].astype(str)
                    original_duplicate2_nonLD.drop(columns=['sequence','order','identifier'], inplace=True)
                    # Subsetting dataframe to standardize final columns
                    processed_order_data_2_nonLD = processed_order_data_2_nonLD[
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                    original_duplicate2_nonLD = original_duplicate2_nonLD[
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                    original_order_2_nonLD = original_order_2_nonLD[
                        Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
                    original_order_2 = pd.concat([original_order_2, original_order_2_nonLD])
                else:
                    print("[PreprocessNorthern] OPO2 - NO ASR two_rows_two_prods orders for this loop.")
                    processed_order_data_2_nonLD = pd.DataFrame() # Processed orders groupby drops
                    original_duplicate2_nonLD = pd.DataFrame() # Full expanded original and opo2_modified orders
                    original_order_2_nonLD = pd.DataFrame() # Full expanded original ONLY orders
                
            else:
                processed_order_data_2_LD = pd.DataFrame() # LD Processed orders groupby drops 
                processed_order_data_2_nonLD = pd.DataFrame() # NON-LD Processed orders groupby drops 
                original_duplicate2_LD = pd.DataFrame() # Full expanded original and opo2_modified orders
                original_duplicate2_nonLD = pd.DataFrame() # Full expanded original and opo2_modified orders
                original_order_2 = pd.DataFrame() # Full expanded original ONLY orders
                    
            print("[PreprocessNorthern] OPO2 two_rows_two_prods LD and non LD Normal orders for {} in {} has completed...".format(
                    self.loop_status, self.txn_id))

            # Extracting out the remaining orders which did not went through the OPO2 process
            if (len(remain_order) > 0) and (len(remain_order) != len(self.order_data)) and (
                    len(normal_order) > 0 or len(self.order_data_2) > 0):
                remaining_order = remain_order.copy()
            elif len(remain_order) == 0:
                remaining_order = pd.DataFrame()
            else:
                remaining_order = self.order_data.copy()
        else:
            remaining_order = pd.DataFrame()

        # Preprocess the remaining MULTILOAD ONLY orders with no OPO2. Cannot permutate multiload orders because it will affect the Retain and Runout of the orders
        if len(remaining_order) > 0 or len(smp_order) > 0:
            remaining_order = pd.concat([remaining_order, smp_order])
            remaining_order['Total_weight'] = remaining_order['Qty'] * remaining_order['Density_values']
            remaining_order['identifier'] = remaining_order['Ship-to'].astype(str) + "_" + remaining_order['Multiload_Status'].astype(str) + "_" + remaining_order['condition'].astype(str) + "_" + remaining_order['grouping'].astype(str) 
            processed_order_multiload = remaining_order.groupby(['identifier'], as_index=False).agg(
                {**Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_basic_agg'], **Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_add_agg'][0], **Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_add_agg'][1]})
            processed_order_multiload = processed_order_multiload.drop(['identifier'], axis=1)
            remaining_order = remaining_order.drop(['identifier'], axis=1)
            remaining_order = remaining_order.drop(['Density_values'], axis=1)
            processed_order_multiload = processed_order_multiload[
                Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]
            remaining_order = remaining_order[
                Config.MODEL_INPUTOUTPUT['order_process_subset'][self.region]['second_loop_post']]


            try:
                ori_remain_multiload = remaining_order[
                    (remaining_order['order_status'] != 'smp') & (remaining_order['Multiload_Status'] != 'Normal')]
                if ori_remain_multiload.empty == True:
                    raise RuntimeError('No Retail Multiload Orders before grouped')
            except Exception as e:
                print("[PreprocessNorthern] Multiload orders : {}".format(e))
                ori_remain_multiload = pd.DataFrame()

            try:
                ori_remain_smp_group = remaining_order[
                    (remaining_order['order_status'] == 'smp') & (remaining_order['grouping'] == 'combined')]
                if ori_remain_smp_group.empty == True:
                    raise RuntimeError('No SMP Multiload Orders before grouped')
            except Exception as e:
                print("[PreprocessNorthern] NO SMP Multiload orders : {}".format(e))
                ori_remain_smp_group = pd.DataFrame()

            try:
                ori_remain_smp_single = remaining_order[
                    (remaining_order['order_status'] == 'smp') & (remaining_order['grouping'] == 'single')]
                if ori_remain_smp_single.empty == True:
                    raise RuntimeError('No SMP Normal Orders before grouped')
            except Exception as e:
                print("[PreprocessNorthern] NO SMP normal orders : {}".format(e))
                ori_remain_smp_single = pd.DataFrame()

        else:
            processed_order_multiload = pd.DataFrame()
            ori_remain_multiload = pd.DataFrame()
            ori_remain_smp_group = pd.DataFrame()
            ori_remain_smp_single = pd.DataFrame()

        # Full expanded original and opo2_modified orders
        ori_order_multiload = pd.concat(
            [ori_remain_smp_group, ori_remain_multiload, original_duplicate2_LD, original_duplicate2_nonLD], axis=0).reset_index(drop=True)
        # Processed orders groupby possible routes/drops
        processed_order_multiload = pd.concat(
            [processed_order_multiload, processed_normal_order, processed_order_data_2_LD, processed_order_data_2_nonLD],
            axis=0).reset_index(drop=True)
        # # Full expanded original ONLY orders
        full_list = pd.concat(
            [ori_remain_smp_group, original_order_1, original_order_2, ori_remain_multiload, ori_remain_smp_single], axis=0).reset_index(
            drop=True)  # add the preprocess and normal order = original order data count from db

        # final processing to processed and full list dataframe
        processed_order_multiload = processed_order_multiload.drop(['runout_duration'], axis=1)
        full_list = full_list.drop(['runout_duration'], axis=1)

        # Merging ori_order_multiload, processed_order_multiload and full list with master_terminal_data
        dfs = [ori_order_multiload, processed_order_multiload, full_list]
        renames_dfs = []
        for df in dfs:
            df = pd.merge(df.drop(['acc'], axis=1), self.master_terminal_data, 'left', left_on=['Ship-to'], right_on=['acc'])
            # terminal values correction
            df = self.orderdata_terminal_correction(df)
            df.rename(columns = {'terminal_PRIMAX_97':'terminal_97', 'terminal':'terminal_95'}, inplace=True)
            df = df.drop(columns=['terminal_BIODIESEL_B10', 'terminal_PRIMAX_95'], errors='ignore')
            renames_dfs.append(df)
        
        ori_order_multiload, processed_order_multiload, full_list = renames_dfs[0].copy(), renames_dfs[1].copy(), renames_dfs[2].copy()

        print(
            "[PreprocessNorthern] Preprocessing multiload orders for {} in {} has completed".format(self.loop_status, self.txn_id))

        return ori_order_multiload, processed_order_multiload, full_list

    def orderdata_terminal_correction(self, data_df):
        """
        Returns a corrected version of the data_df using the terminal value of the data_df as reference because the order_bank terminal should be the single source of truth.

        :param data_df: DataFrame with order details and the reference df for correct terminal value. 
        """
        data_df['terminal_PRIMAX_95'] = np.where(
            (data_df['terminal_PRIMAX_95'] != data_df['terminal']) & (data_df['Material'] == Config.DATA['PRODUCT_CODE']['PRIMAX_95']), 
            data_df['terminal'], data_df['terminal_PRIMAX_95'])

        data_df['terminal_PRIMAX_97'] = np.where(
            (data_df['terminal_PRIMAX_97'] != data_df['terminal']) & (data_df['Material'] == Config.DATA['PRODUCT_CODE']['PRIMAX_97']), 
            data_df['terminal'], data_df['terminal_PRIMAX_97'])

        data_df['terminal_BIODIESEL_B10'] = np.where(
            (data_df['terminal_BIODIESEL_B10'] != data_df['terminal']) & (data_df['Material'] == Config.DATA['PRODUCT_CODE']['BIODIESEL_B10']), 
            data_df['terminal'], data_df['terminal_BIODIESEL_B10'])

        data_df['terminal_PRIMAX_95'] = np.where(
            ((data_df['terminal_BIODIESEL_B10'] != data_df['terminal_PRIMAX_95']) & (data_df['Material'] == Config.DATA['PRODUCT_CODE']['BIODIESEL_B10'])), 
            data_df['terminal_BIODIESEL_B10'], data_df['terminal_PRIMAX_95'])
        
        return data_df