import os
import time
import copy

from src.rts_routing_modules.rts_routing_utilities import end_day_stock_1, change_retain_format, change_strtodatetime_format, change_datetimetofloat_format
from src.rts_routing_modules.rts_routing_simulate import simulating_solutions, sa_cloudwise
from src.input_gen_modules import input_generation 
from src.input_gen_modules.rts_input_handler import InputHandler

#from instance import Instance
from src.rts_routing_modules.rts_routing_baseobjects import Resource, Order
from conf import Config, Logger
import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta

from src.utility.db_connection_helper import get_db_connection

start_time = datetime.now()
from sqlalchemy import create_engine

from database_connector import DatabaseConnector, YAMLFileConnector

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)


# find multiload_status
# def find multiload_status(order_list):
#     group_count_order_data = (order_list.groupby(['ship_to_party','product']).agg({"ship_to_party": "count", "product":"count"})).rename(columns={"ship_to_party":"number_rows", "product":"number_product"}).reset_index()
#     correct_multiload_station = group_count_order_data[(group_count_order_data['number_rows']>1) & (group_count_order_data['number_product']>1)]
#     multiload_order = order_list[order_list['ship_to_party'].isin(correct_multiload_station['ship_to_party'])]
#     multiload_order['Multiload_Status'] = 'Multiload'
#     correct_non_multiload_station = group_count_order_data[(group_count_order_data['number_rows']<2) & (group_count_order_data['number_product']<2)]
#     normal_order = order_list[order_list['ship_to_party'].isin(correct_non_multiload_station['ship_to_party'])]
#     normal_order = normal_order[~normal_order['ship_to_party'].isin(multiload_order['ship_to_party'])]
#     normal_order['Multiload_Status'] = 'Normal'
#     order_data_labelled = pd.concat([multiload_order, normal_order]).reset_index(drop=True)
    
#     return order_data_labelled
class RouteCompile():
    def __init__(self, loop_status, order_id, date, region, txn_id, start_time, capacity, largest_capacity = None, post_process = None, smallest_capacity = None, input_source = None, multiload_all_tanker = None, normal_all_tanker = None, all_cap_toggle = None, rts_strategy = None, opo2_first = False, opo2_first_capacity = None, tanker_capacity_list = None, order_id_ori= None):
        self.loop_status = loop_status
        self.order_id = order_id
        self.date = date
        self.region = region
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
        self.rts_strategy = rts_strategy
        self.opo2_first = opo2_first
        self.opo2_first_capacity = opo2_first_capacity
        self.tanker_capacity_list = tanker_capacity_list
        self.order_id_ori = order_id_ori
        self.route_two_loop = False

    def route_compile(self):
        if self.loop_status.split('_')[1] == "Second":
            self.start_time = datetime.now()
        print("[RouteGenerator] Reading routing inputs for {} in {} are being initiated from Input Generation...".format(self.loop_status, self.txn_id))       
        input_gen = input_generation.InputGen(0, self.order_id, self.date, self.region, self.loop_status, self.txn_id, self.start_time, self.capacity, self.largest_capacity, self.smallest_capacity, self.post_process, self.input_source, self.multiload_all_tanker, self.normal_all_tanker, self.all_cap_toggle, self.opo2_first, self.opo2_first_capacity, self.tanker_capacity_list, self.order_id_ori)
        order_data, truck_data, cloud_ordered_list, duplicate_station_full, order_data_full, master_data, commercial_data, unmatched_commercial, master_terminal_data, depot_dates = input_gen.read_input()
        
        # Prepare dataframe for merge back opening and closing time for stations
        # TODO : To add more region specific columns
        if self.region == "CENTRAL" :
            cols = ['Ship-to','Opening_Time','Closing_Time']
        elif self.region in ['SOUTHERN','EASTERN','NORTHERN']: 
            cols = ['Ship-to','terminal_95','Opening_Time','Closing_Time']
        else :
            cols = ['Ship-to','terminal','Opening_Time','Closing_Time']       
        
        station_time = order_data_full.drop_duplicates(subset=cols, keep="first")
        station_time = station_time[cols]

        # Prepare dataframe for merge back priority for orders
        # priority_df = order_data_full[~order_data_full['Priority'].isnull()]
        # multiload_priority = priority_df[priority_df['order_status'] == "multiload"]
        # normal_priority = priority_df[priority_df['order_status'] != "multiload"]
                  
        # if len(unmatched_commercial) > 0 :
        #     master_terminal_97 = master_terminal_data[['acc','terminal_97']]
        #     unmatched_commercial = pd.merge(unmatched_commercial, master_terminal_data, 'left', left_on=['Destinations'], right_on=['acc'])
        #     unmatched_commercial = unmatched_commercial.rename(columns = {'terminal_97':'Terminal_97', 'Default terminal':'Terminal_Default'})   
        #     unmatched_commercial = unmatched_commercial[['Destinations','Terminal_97','Terminal_Default','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Order_Type']]
        # else :
        #     unmatched_commercial = pd.DataFrame()
        
        # Get opening time of depot of the model run region to incorporate as time leave depot
        time_list = ['time_from','time_to']
        for col in time_list :
            depot_dates[col] = change_strtodatetime_format(depot_dates[col], self.date + timedelta(days=1))  

        # Merging terminal info into order data
        master_terminal_data = master_terminal_data.rename(columns = {'terminal_PRIMAX_97':'terminal_97', 'terminal_PRIMAX_95':'terminal_95'})
        
        # TODO : To add more region specific conditions
        if self.region == "CENTRAL":
            order_data = pd.merge(order_data.drop(['acc'], axis=1), master_terminal_data[['acc','terminal_97','terminal_95']], 'left', left_on=['Ship-to'], right_on=['acc'])
        else:
            pass

        # Capture terminal in use in order data for terminal-cloud wise route generation        
        terminal_in_run = order_data['terminal_95'].unique()

        if self.opo2_first == True :
            self.route_two_loop = True

        count_opo2 = 1
        result_df = pd.DataFrame()
        unmatched_log = pd.DataFrame()

        # While Loop to subset data for routing with / without opo_first to form route
        while count_opo2 < 3 :
            print("[RouteGenerator] Preprocessing routing inputs for {} in {} are being initiated...".format(self.loop_status, self.txn_id))
            # Subset only data not undergone opo2 in first loop if opo2_first is True to produce route
            if self.opo2_first and self.route_two_loop :
                self.opo2_first = False # OFF to process orders from opo2 in subsequent script
                subset_order_data = order_data.query('opo2 == False')
                subset_order_data.sort_values(by=['Ship-to','Material','Qty','Total_weight','condition','grouping'], inplace=True)
                count_opo2 += 1

            # Subset only data undergone opo2 in first loop if opo2_first is True and use only opo2 first capacity specified in main.py to produce route
            elif self.opo2_first == False and self.route_two_loop :
                self.opo2_first = True # ON back to not process orders from opo2 in subsequent script
                subset_order_data = order_data.query('opo2 == True')
                subset_order_data.sort_values(by=['Ship-to','Material','Qty','Total_weight','condition','grouping'], inplace=True)
                #duplicate_station_full = duplicate_station_full.query('opo2 == True')
                order_data_full = order_data_full.query('opo2 == True')
                truck_data = truck_data[truck_data['Vehicle maximum vol.'].isin(self.opo2_first_capacity)]
                count_opo2 += 2

            # This is for opo2_first is False in any loop
            elif self.opo2_first == False :
                subset_order_data = order_data.copy()
                count_opo2 += 2

            else :
                pass
            
            # Subset normal and multiload station orders based on region, ie. need to segregate LD U97 from multiload orders for Southern
            normal_order = subset_order_data.query('Multiload_Status == "Normal"')
            normal_order['combine_tag'] = "normal"            
            if self.region == "CENTRAL":
                multiload_order = subset_order_data.query('Multiload_Status == "Multiload"')
            elif self.region == "SOUTHERN":
                if len(subset_order_data[(subset_order_data['long_distance'].isin(Config.MODEL_INPUTOUTPUT['long_distance'][self.region])) & (subset_order_data['Multiload_Status'] == "Multiload")]) > 0 :
                    multiload_order = subset_order_data.query('Multiload_Status == "Multiload" & long_distance == "non_LD"')
                    LD_multiload_order = subset_order_data.query('Multiload_Status == "Multiload" & long_distance != "non_LD"')
                    normal_order = pd.concat([normal_order, LD_multiload_order], axis=0)
                else :
                    multiload_order = subset_order_data.query('Multiload_Status == "Multiload"')
            else :
                multiload_order = subset_order_data.query('Multiload_Status == "Multiload"')


            # Subset multiload orders into 1st load and 2nd load 
            if self.loop_status.split('_')[1] == "First" and self.opo2_first == False : 
                multiload_order_min_retain = multiload_order.loc[multiload_order.groupby(['Cloud2','Ship-to']).retain_hour.idxmin()].reset_index(drop=True)
                multiload_order_min_retain['combine_tag'] = "early"
                multiload_order_max_retain = multiload_order.loc[multiload_order.groupby(['Cloud2','Ship-to']).retain_hour.idxmax()].reset_index(drop=True)
                multiload_order_max_retain['combine_tag'] = "later"

            elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                early_group = ['combined_early','combined_1','single_1', 'single_multiload_early', 'single_early', 'single_normal_early']
                late_group = ['combined_late','combined_2', 'single_2', 'single_multiload_late', 'single_late', 'single_normal_late','single_multiload']
                multiload_order_min_retain = multiload_order[multiload_order['grouping'].isin(early_group)]
                multiload_order_min_retain['combine_tag'] = "early"
                multiload_order_max_retain = multiload_order[multiload_order['grouping'].isin(late_group)]
                multiload_order_max_retain['combine_tag'] = "later"

            else :
                pass

            # Forming road tanker dictionary and class
            resource_dict = {}
            for resource in truck_data.iterrows():
                if self.region in ['CENTRAL']:
                    resource_dict[resource[1]['Vehicle Number']] = Resource(resource[1]['Vehicle Number'], resource[1]['shift_type'], int(resource[1]['Vehicle maximum vol.']), int(resource[1]['Vehicle maximum vol.'])/5460, 5460, float(resource[1]['Vehicle max.weight']), float(resource[1]['Vehicle unl.weight']), resource[1]['start_shift'], resource[1]['end_shift'], resource[1]['balance_shift'], 
                    depot_dates['time_from'].iloc[0], depot_close_time = depot_dates['time_to'].iloc[0])
                elif self.region in ['NORTHERN','SOUTHERN','EASTERN']:
                    resource_dict[resource[1]['Vehicle Number']] = Resource(resource[1]['Vehicle Number'], resource[1]['shift_type'], int(resource[1]['Vehicle maximum vol.']), int(resource[1]['Vehicle maximum vol.'])/5460, 5460, float(resource[1]['Vehicle max.weight']), float(resource[1]['Vehicle unl.weight']), resource[1]['start_shift'], resource[1]['end_shift'], resource[1]['balance_shift'],
                    depot_dates[depot_dates['terminal'] == resource[1]['default_terminal']]['time_from'].iloc[0], depot_close_time = depot_dates[depot_dates['terminal'] == resource[1]['default_terminal']]['time_to'].iloc[0], rt_terminal = resource[1]['default_terminal'])
                elif self.region in ['SABAH','SARAWAK']:
                    resource_dict[resource[1]['Vehicle Number']] = Resource(resource[1]['Vehicle Number'], resource[1]['shift_type'], int(resource[1]['Vehicle maximum vol.']), int(resource[1]['compartment_no']), int(resource[1]['compartment_max_vol']), float(resource[1]['Vehicle max.weight']), float(resource[1]['Vehicle unl.weight']), resource[1]['start_shift'], resource[1]['end_shift'], resource[1]['balance_shift'],
                    depot_dates[depot_dates['terminal'] == resource[1]['default_terminal']]['time_from'].iloc[0])
                else:
                    pass
            
            print("[RouteGenerator] Preprocessing routing inputs for {} in {} is completed...".format(self.loop_status, self.txn_id))

            count = 1

            # While loop to combine normal orders with multiload orders of 1st load and 2nd load seperately to form route
            while count < 3 :
                multiload_combi = None
                if len(multiload_order) > 0 :

                    if len(multiload_order_min_retain) > 0 and count == 1 :
                        print("Combining Multiload Orders (Early Retain) with Normal Orders")
                        subset_order_data = pd.concat([normal_order, multiload_order_min_retain], axis=0)
                        count += 1 
                        multiload_combi = 'Early'

                    else :
                        print("Combining Multiload Orders (Later Retain) with Normal Orders")
                        subset_order_data = pd.concat([normal_order, multiload_order_max_retain], axis=0)
                        count += 2
                        multiload_combi = 'Later'
                        
                else:
                    subset_order_data = normal_order.copy()
                    count += 2

                # Forming order class and dictionary
                orders_dict = {}
                if self.loop_status.split('_')[1] == "First" and self.opo2_first == False :
                    for order in subset_order_data.iterrows():
                        unique_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
                        orders_dict[unique_id] = Order(unique_id, order[1]['Qty'], order[1]['Priority'],
                                                    order[1]['Material'], int(order[1]['Ship-to']),
                                                    order[1]['Cloud2'], order[1]['retain_hour'], order[1]['Runout'], order[1]['Closing_Time'], order[1]['accessibility'], order[1]['distance_terminal_u95'], order[1]['Total_weight'], order[1]['End_Stock_Day'],
                                                    order[1]['Tank_Status'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], order[1]['combine_tag'], order[1]['terminal_97'], order[1]['terminal_95'], order[1]['long_distance'], 
                                                    identifier = str(order[1]['Ship-to']) + '_' + str(order[1]['Material']) + '_' + str(order[1]['Qty']) + '_' + str(int(order[1]['Total_weight'])) + '_' + str(order[1]['condition']) + '_' + str(order[1]['grouping']))

                elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                    for order in subset_order_data.iterrows():
                        unique_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
                        orders_dict[unique_id] = Order(unique_id, order[1]['Qty'], order[1]['Priority'],
                                                    order[1]['Material'], int(order[1]['Ship-to']),
                                                    order[1]['Cloud2'], order[1]['retain_hour'], order[1]['Runout'], order[1]['Closing_Time'], order[1]['accessibility'], order[1]['distance_terminal_u95'], order[1]['Total_weight'], order[1]['End_Stock_Day'],
                                                    order[1]['Tank_Status'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], order[1]['combine_tag'], order[1]['terminal_97'], order[1]['terminal_95'], order[1]['long_distance'], ori_status = order[1]['ori_status'],
                                                    identifier = str(order[1]['Ship-to']) + '_' + str(order[1]['Material']) + '_' + str(order[1]['Qty']) + '_' + str(int(order[1]['Total_weight'])) + '_' + str(order[1]['condition']) + '_' + str(order[1]['grouping']))
                
                else :
                    pass

                print("[RouteGenerator] Generating route cloudwise for {} in {} is being initiated...".format(self.loop_status, self.txn_id))
                simulation_list = []
                unmatched_list = []
                cloud_current_list = subset_order_data['Cloud2'].unique()
                for i in range(1):
                    t = time.time()
                    a = copy.deepcopy(resource_dict)
                    b = copy.deepcopy(orders_dict)
                    cloud_wise_route, cloud_wise_unmatched = simulating_solutions(orders_dict, resource_dict, "Current", cloud_current_list, self.date, multiload_combi, self.multiload_all_tanker, self.normal_all_tanker, self.region, self.opo2_first, terminal_in_run)
                    simulation_list.append((sum([len(cloud_wise_route[i]) for i in cloud_current_list]), cloud_wise_route))
                    unmatched_list.append((sum([len(cloud_wise_unmatched[i]) for i in cloud_current_list]), cloud_wise_unmatched))
                    resource_dict, orders_dict = a, b

                cloud_wise_route = min(simulation_list, key = lambda t: t[1])[1]
                cloud_wise_unmatched = min(unmatched_list, key = lambda t: t[1])[1]

                final_solution, final_unmatched = sa_cloudwise(cloud_wise_route, cloud_wise_unmatched, self.loop_status, self.opo2_first)

                # Let's compile the solution for all the clouds
                for cloud in final_solution:
                    df = pd.DataFrame(final_solution[cloud].output())
                    df['Cloud'] = cloud
                    result_df = result_df.append(df)
                
                for cloud in final_unmatched:
                    df = pd.DataFrame(final_unmatched[cloud].output())
                    df['Cloud'] = cloud
                    unmatched_log = unmatched_log.append(df)
                if len(unmatched_log) > 0 :
                    unmatched_log = unmatched_log.drop_duplicates(subset='Route_ID', keep="first")

            print("[RouteGenerator] Generating route cloudwise for {} in {} is completed...".format(self.loop_status, self.txn_id))

        if len(result_df) > 0:
            print("[RouteGenerator] Postprocessing route output for {} in {} are being initiated...".format(self.loop_status, self.txn_id))
            result_df = result_df.reset_index(drop=True)        
            result_df['Destinations'] = result_df['Destinations'].astype(int)
            result_df['Product'] = result_df['Product'].astype(int)
            #result_df.drop(['Process'], axis = 1, inplace=True)

            if len(duplicate_station_full) > 0 :
                if self.loop_status.split('_')[1] == "First" and self.opo2_first == False : 
                    grouping = ['combined','combined_1','combined_2', 'combined_LD']
                    duplicate_station_full = duplicate_station_full[duplicate_station_full['grouping'].isin(grouping)]
                elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                    grouping = ['combined','combined_1','combined_2','combined_early','combined_late', 'combined_LD']
                    duplicate_station_full = duplicate_station_full[duplicate_station_full['grouping'].isin(grouping)]
                else :
                    pass

                duplicate_station_full['Station_ID_Product'] = duplicate_station_full['Ship To'].astype(str) + '_' +  duplicate_station_full['Multiload_Status'].astype(str) + '_' + duplicate_station_full['condition'] + '_' + duplicate_station_full['grouping'] 
                duplicate_station_full['Station_ID_Product'] = duplicate_station_full['Station_ID_Product'].astype('category')
                duplicate_station_full = duplicate_station_full[duplicate_station_full.columns.difference(['Tank_Status', 'Multiload_Status'])]
                
                result_df['Station_ID_Product'] = result_df['Destinations'].astype(str) + '_' + result_df['Multiload_Status'].astype(str) + '_' + result_df['condition'] + '_' + result_df['grouping']
                result_df['Station_ID_Product'] = result_df['Station_ID_Product'].astype('category')
                result_df_filter = result_df[~result_df['Station_ID_Product'].isin(duplicate_station_full['Station_ID_Product'])].reset_index(drop=True)
                result_df = result_df[result_df['Station_ID_Product'].isin(duplicate_station_full['Station_ID_Product'])].reset_index(drop=True)
                result_df = result_df.drop(['Station_ID_Product'], axis=1)
                result_df_filter = result_df_filter.drop(['Station_ID_Product'], axis=1)
                
                if len(unmatched_log) > 0 : 
                    unmatched_log['Station_ID_Product'] = unmatched_log['Destinations'].astype(int).astype(str) + '_' + unmatched_log['Multiload_Status'].astype(str) + '_' + unmatched_log['condition'] + '_' + unmatched_log['grouping']
                    unmatched_log['Station_ID_Product'] = unmatched_log['Station_ID_Product'].astype('category')
                    unmatched_log_filter = unmatched_log[~unmatched_log['Station_ID_Product'].isin(duplicate_station_full['Station_ID_Product'])].reset_index(drop=True)
                    unmatched_log = unmatched_log[unmatched_log['Station_ID_Product'].isin(duplicate_station_full['Station_ID_Product'])].reset_index(drop=True)
                    unmatched_log = unmatched_log.drop(['Station_ID_Product'], axis=1)
                    unmatched_log_filter = unmatched_log_filter.drop(['Station_ID_Product'], axis=1)
                
                if self.loop_status.split('_')[1] == "First" and self.opo2_first == False : 
                    merged_heuristic = pd.merge(duplicate_station_full, result_df, left_on=['Ship To','condition','grouping'], right_on=['Destinations','condition','grouping'])
                    merged_heuristic = merged_heuristic[['Ship To', 'Priority_x', 'Terminal_97', 'Terminal_Default', 'Material', 'Quantity', 'Remark', 'Runout_x', 'End_Stock_Day_x', 'Route_ID',
                        'No_Compartment', 'Tanker_Cap', 'Order_ID', 'Utilization', 'Travel_Time','Accessibility','Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status_x', 'combine_tag','long_distance']]
                    merged_heuristic = merged_heuristic.rename(columns = {'Ship To':'Destinations', 'Priority_x':'Priority', 'Material':'Product', 'Remark':'Retain_Hour', 'Runout_x':'Runout', 'Quantity':'Order_Quantity','order_status_x':'order_status','End_Stock_Day_x':'End_Stock_Day'})
                    merged_heuristic = merged_heuristic[['Route_ID', 'Priority', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID',
                        'Destinations', 'Product', 'Order_Quantity', 'Utilization', 'Retain_Hour', 'Runout','End_Stock_Day',
                        'Travel_Time', 'Accessibility', 'Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status', 'combine_tag', 'long_distance']]

                    if len(unmatched_log) > 0 : 
                        merged_unmatched = pd.merge(duplicate_station_full, unmatched_log, left_on=['Ship To','condition','grouping'], right_on=['Destinations','condition','grouping'])
                        merged_unmatched = merged_unmatched[['Ship To', 'Priority_x', 'Terminal_97', 'Terminal_Default', 'Material', 'Quantity', 'Total_weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Remark','Runout_x','End_Stock_Day_x','Route_ID',
                            'No_Compartment', 'Tanker_Cap', 'Order_ID','Accessibility','Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status_x', 'combine_tag', 'long_distance', 'violation_log']]
                        merged_unmatched = merged_unmatched.rename(columns = {'Ship To':'Destinations', 'Priority_x':'Priority', 'Material':'Product', 'Remark':'Retain_Hour', 'Runout_x':'Runout', 'Quantity':'Order_Quantity','order_status_x':'order_status','Total_weight':'Order_Weight','End_Stock_Day_x':'End_Stock_Day'})
                        merged_unmatched['ori_status'] = 'original'
                        merged_unmatched = merged_unmatched[['Route_ID', 'Priority', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID', 'Destinations',
                                                        'Product', 'Order_Quantity', 'Order_Weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Retain_Hour', 'Runout', 'End_Stock_Day','Accessibility',
                                                        'Tank_Status', 'Multiload_Status','Cloud','condition', 'grouping',
                                                        'order_status', 'combine_tag', 'long_distance', 'ori_status','violation_log']]

                elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                    # Expanding orders from result_df back to the full number of orders instead of the processed one
                    merged_heuristic = pd.merge(duplicate_station_full, result_df, left_on=['Ship To','condition','grouping'], right_on=['Destinations','condition','grouping'])
                    merged_heuristic = merged_heuristic[['Ship To', 'Priority_x', 'Terminal_97', 'Terminal_Default', 'Material', 'Quantity', 'Remark', 'Runout_x','End_Stock_Day_x', 'Route_ID',
                        'No_Compartment', 'Tanker_Cap', 'Order_ID', 'Utilization', 'Travel_Time','Accessibility','Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status_x', 'combine_tag', 'ori_status_x', 'long_distance']]
                    merged_heuristic = merged_heuristic.rename(columns = {'Ship To':'Destinations', 'Priority_x':'Priority', 'Material':'Product', 'Remark':'Retain_Hour', 'Runout_x':'Runout','Quantity':'Order_Quantity','order_status_x':'order_status', 'ori_status_x':'ori_status','End_Stock_Day_x':'End_Stock_Day'})
                    merged_heuristic = merged_heuristic[['Route_ID', 'Priority', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID',
                        'Destinations', 'Product', 'Order_Quantity', 'Utilization', 'Retain_Hour', 'Runout', 'End_Stock_Day',
                        'Travel_Time', 'Accessibility', 'Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status', 'combine_tag', 'ori_status', 'long_distance']]
                    
                    if len(unmatched_log) > 0 : 
                        merged_unmatched = pd.merge(duplicate_station_full, unmatched_log, left_on=['Ship To','condition','grouping'], right_on=['Destinations','condition','grouping'])
                        merged_unmatched = merged_unmatched[['Ship To', 'Priority_x', 'Terminal_97', 'Terminal_Default', 'Material', 'Quantity', 'Total_weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Remark', 'Runout_x', 'End_Stock_Day_x', 'Route_ID',
                            'No_Compartment', 'Tanker_Cap', 'Order_ID','Accessibility','Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status_x','ori_status_x', 'combine_tag', 'long_distance', 'violation_log']]
                        merged_unmatched = merged_unmatched.rename(columns = {'Ship To':'Destinations', 'Priority_x':'Priority', 'Material':'Product', 'Remark':'Retain_Hour', 'Runout_x':'Runout','Quantity':'Order_Quantity','order_status_x':'order_status','Total_weight':'Order_Weight','ori_status_x':'ori_status','End_Stock_Day_x':'End_Stock_Day'})
                        merged_unmatched = merged_unmatched[['Route_ID', 'Priority', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID', 'Destinations',
                                                        'Product', 'Order_Quantity', 'Order_Weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Retain_Hour', 'Runout', 'End_Stock_Day', 'Accessibility',
                                                        'Tank_Status', 'Multiload_Status','Cloud','condition', 'grouping',
                                                        'order_status', 'combine_tag', 'long_distance', 'ori_status', 'violation_log']] 
                else :
                    pass
                
                merged_heuristic['Sequence'] = merged_heuristic.groupby(['Route_ID','Order_ID']).cumcount()
                cols = ['Order_ID','Sequence']
                merged_heuristic['Order_ID'] = merged_heuristic[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
                merged_heuristic = merged_heuristic.drop(['Sequence'], axis=1)
                
                if len(unmatched_log) > 0 : 
                    merged_unmatched['Sequence'] = merged_unmatched.groupby(['Route_ID','Order_ID']).cumcount()
                    cols = ['Order_ID','Sequence']
                    merged_unmatched['Order_ID'] = merged_unmatched[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
                    merged_unmatched = merged_unmatched.drop(['Sequence'], axis=1)

            else :
               result_df_filter = result_df.copy()
               unmatched_log_filter = unmatched_log.copy()
               merged_heuristic = pd.DataFrame()
               merged_unmatched = pd.DataFrame()

            full_result_df = pd.concat([result_df_filter, merged_heuristic], axis = 0, sort=False).reset_index(drop=True)
            mapper_1 = full_result_df.groupby('Route_ID')['Retain_Hour'].max()
            mapper_2 = full_result_df.groupby('Route_ID')['Runout'].min()
            full_result_df['Min_Retain_Hour'] = full_result_df['Route_ID'].map(mapper_1)
            full_result_df['Min_Runout'] = full_result_df['Route_ID'].map(mapper_2)
                            
            if len(unmatched_log) > 0 : 
                full_unmatched_log = pd.concat([unmatched_log_filter, merged_unmatched], axis = 0, sort=False).reset_index(drop=True)
                full_unmatched_log = full_unmatched_log.drop(['combine_tag'], axis=1)

            check_multiload_route = full_result_df[full_result_df['Multiload_Status'] == 'Multiload']
            
            if len(check_multiload_route) > 0 :
                check_multiload_route = check_multiload_route.drop_duplicates(subset=['Destinations','Product','Order_Quantity','Multiload_Status','condition','grouping','order_status'], keep="first")
                
                # TO REVISIT : Allow either 1st load or 2nd load to retain in routes about instead of both needs to be there to increase chances of to be scheduled
                if self.loop_status.split('_')[1] == "First" and self.opo2_first == False : 
                #     check_multiload_route_group = check_multiload_route.groupby(['Destinations','Multiload_Status','condition']).size().reset_index(name='counts')
                #     check_multiload_route_group_2 = check_multiload_route.groupby(['Destinations','Multiload_Status','condition','grouping']).size().reset_index(name='counts')
  
                #     try:
                #         two_rows_one_prods = check_multiload_route_group[(check_multiload_route_group['condition'] == "two_rows_one_prods") & (check_multiload_route_group['counts']<2)]
                #     except Exception as e:
                #         two_rows_one_prods = pd.DataFrame()
                #     try:
                #         three_rows_two_prods = check_multiload_route_group[(check_multiload_route_group['condition'] == "three_rows_two_prods") & (check_multiload_route_group['counts']<2)]
                #     except Exception as e:
                #         three_rows_two_prods = pd.DataFrame()
                #     try :                 
                #         four_rows_two_prods = check_multiload_route_group[(check_multiload_route_group['condition'] == "four_rows_two_prods") & (check_multiload_route_group['counts']<3)]
                #     except Exception as e:
                #         four_rows_two_prods = pd.DataFrame()
                #     try :
                #         four_rows_three_prods = check_multiload_route_group[(check_multiload_route_group['condition'] == "four_rows_three_prods") & (check_multiload_route_group['counts']<3)]
                #     except Exception as e:
                #         four_rows_three_prods = pd.DataFrame()                    
                #     try :
                #         five_rows_three_prods = check_multiload_route_group[(check_multiload_route_group['condition'] == "five_rows_three_prods") & (check_multiload_route_group['counts']<4)]
                #     except Exception as e:
                #         five_rows_three_prods = pd.DataFrame()                   
                    
                #     remove_multiload_dest = pd.concat([two_rows_one_prods, three_rows_two_prods, four_rows_two_prods, four_rows_three_prods, five_rows_three_prods], axis = 0, sort=False)['Destinations'].unique()
                #     remove_multiload_route = full_result_df[(full_result_df['Destinations'].isin(remove_multiload_dest))][full_result_df['combine_tag'] != 'early']['Route_ID'].unique()
                #     full_result_df = full_result_df[~full_result_df['Route_ID'].isin(remove_multiload_route)]
                    full_result_df = full_result_df.drop(['combine_tag'], axis=1)

                elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                    # check_multiload_route_group = check_multiload_route.groupby(['Destinations','Multiload_Status','condition']).size().reset_index(name='counts')

                    # check_multiload_route_group['identifier'] = check_multiload_route_group['Destinations'].astype(str) + '_' + check_multiload_route_group['Multiload_Status'].astype(str) + '_' + check_multiload_route_group['condition'].astype(str)
                    # try:
                    #     two_rows_one_prods = check_multiload_route_group[(check_multiload_route_group['condition'].str.contains("two_rows_one_prods")) & (check_multiload_route_group['counts']<2)]
                    # except Exception as e:
                    #     two_rows_one_prods = pd.DataFrame()
                    # other_multiload = check_multiload_route_group[~(check_multiload_route_group['condition'].str.contains("two_rows_one_prods")) & (check_multiload_route_group['counts']<3)]

                    # remove_multiload_identifier = pd.concat([two_rows_one_prods, other_multiload], axis = 0, sort=False)['identifier'].unique()
                    # full_result_df['identifier'] = full_result_df['Destinations'].astype(str) + '_' + full_result_df['Multiload_Status'].astype(str) + '_' + full_result_df['condition'].astype(str)
                    # remove_multiload_route = full_result_df[full_result_df['identifier'].isin(remove_multiload_identifier)][full_result_df['combine_tag'] != 'early']['Route_ID'].unique()
                    # full_result_df = full_result_df[~full_result_df['Route_ID'].isin(remove_multiload_route)]
                    # full_result_df = full_result_df.drop(['identifier','combine_tag'], axis=1)
                    full_result_df = full_result_df.drop(['combine_tag'], axis=1)
                
                else :
                    pass
            
            else :
                full_result_df = full_result_df.drop(['combine_tag'], axis=1)

            if len(full_result_df) > 0 :
                if self.multiload_all_tanker == True and self.capacity != self.largest_capacity:
                    print("[RouteGenerator] Subset ONLY multiload route output for {} in {} is started...".format(self.loop_status, self.txn_id))  
                    multiload_route = full_result_df[full_result_df['Multiload_Status']=="Multiload"]['Route_ID'].unique()
                    full_result_df = (full_result_df[full_result_df['Route_ID'].isin(multiload_route)]) 
                    print("[RouteGenerator] Subset ONLY multiload route output for {} in {} is completed...".format(self.loop_status, self.txn_id))  

                if self.loop_status.split('_')[1] == "First" and len(unmatched_log) > 0 :  
                    full_unmatched_log['ori_status'] = 'original'
                
                if self.loop_status.split('_')[1] == "First" and self.opo2_first == False : 
                    order_data_full['Station_ID_Product'] = order_data_full['Ship-to'].astype(str) + '_' + order_data_full['Material'].astype(str) + '_' + order_data_full['condition'].astype(str) + '_' + order_data_full['grouping'].astype(str) 
                    order_data_full['Station_ID_Product'] = order_data_full['Station_ID_Product'].astype('category')
                    full_result_df['Station_ID_Product'] = full_result_df['Destinations'].astype(str) + '_' + full_result_df['Product'].astype(str) + '_' + full_result_df['condition'].astype(str) + '_' + full_result_df['grouping'].astype(str)
                    full_result_df['Station_ID_Product'] = full_result_df['Station_ID_Product'].astype('category')
                    unmatched_df = order_data_full[~order_data_full['Station_ID_Product'].isin(full_result_df['Station_ID_Product'])].reset_index(drop=True)
                    unmatched_df = unmatched_df.drop(['Station_ID_Product'], axis=1)
                    full_result_df = full_result_df.drop(['Station_ID_Product'], axis=1)

                    if self.region == "CENTRAL":
                        unmatched_df = pd.merge(unmatched_df, master_terminal_data, 'left', left_on=['Ship-to'], right_on=['acc'])
                    else:
                        pass

                    unmatched_df['Order_Type'] = "RE"
                    unmatched_df = unmatched_df[['Ship-to','Priority','terminal_97','terminal_95','Material', 'Qty','retain_hour','Runout','End_Stock_Day','accessibility','Tank_Status','Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud2','Order_Type']]
                    unmatched_df = unmatched_df.rename(columns = {'Ship-to':'Destinations', 'terminal_97':'Terminal_97', 'terminal_95':'Terminal_Default', 'Material':'Product', 'retain_hour':'Retain_Hour', 'Qty':'Order_Quantity', 'Cloud2':'Cloud','accessibility':'Accessibility'}).reset_index(drop=True)
                            
                elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                    ori_data_full = order_data_full[order_data_full['ori_status'] == 'original']
                    ori_data_full['condition'] = ori_data_full['condition'].str.replace(r'_[0-9]+','')
                    ori_data_full['grouping'] = ori_data_full['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_multiload_early': 'combined', 'single_multiload_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_normal_early': 'combined', 'single_normal_late': 'combined', 'single_multiload': 'single'})
                    ori_data_full = ori_data_full.drop_duplicates(subset=['Ship-to','Material','Qty','Multiload_Status','condition','grouping','ori_status'], keep="first")
                    ori_data_full['Station_ID_Product'] = ori_data_full['Ship-to'].astype(str) + '_' + ori_data_full['Material'].astype(str) + '_' + ori_data_full['condition'].astype(str) + '_' + ori_data_full['grouping'].astype(str) + '_' + ori_data_full['order_status'].astype(str)
                    ori_data_full['Station_ID_Product'] = ori_data_full['Station_ID_Product'].astype('category')
            
                    check_full_result_df = full_result_df.copy()
                    check_full_result_df['condition'] = check_full_result_df['condition'].str.replace(r'_[0-9]+','')
                    check_full_result_df['grouping'] = check_full_result_df['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_multiload_early': 'combined', 'single_multiload_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_normal_early': 'combined', 'single_normal_late': 'combined', 'single_multiload': 'single'})
                    check_full_result_df = check_full_result_df.drop_duplicates(subset=['Destinations','Product','Multiload_Status','condition','grouping'], keep="first")
                    check_full_result_df['Station_ID_Product'] = check_full_result_df['Destinations'].astype(str) + '_' + check_full_result_df['Product'].astype(str) + '_' + check_full_result_df['condition'].astype(str) + '_' + check_full_result_df['grouping'].astype(str) + '_' + check_full_result_df['order_status'].astype(str) 
                    check_full_result_df['Station_ID_Product'] = check_full_result_df['Station_ID_Product'].astype('category')
                    unmatched_df = ori_data_full[~ori_data_full['Station_ID_Product'].isin(check_full_result_df['Station_ID_Product'])].reset_index(drop=True)
                    unmatched_df = unmatched_df.drop(['Station_ID_Product'], axis=1)
                    check_full_result_df = check_full_result_df.drop(['Station_ID_Product'], axis=1)
                  
                    if self.region == "CENTRAL":
                        unmatched_df = pd.merge(unmatched_df, master_terminal_data, 'left', left_on=['Ship-to'], right_on=['acc'])
                    else:
                        pass

                    unmatched_df['Order_Type'] = "RE"
                    unmatched_df = unmatched_df[['Ship-to','Priority','terminal_97','terminal_95','Material','Qty','retain_hour','Runout','End_Stock_Day','accessibility','Tank_Status','Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud2','Order_Type']]
                    unmatched_df = unmatched_df.rename(columns = {'Ship-to':'Destinations', 'terminal_97':'Terminal_97', 'terminal_95':'Terminal_Default', 'Material':'Product', 'retain_hour':'Retain_Hour', 'Qty':'Order_Quantity', 'Cloud2':'Cloud','accessibility':'Accessibility'}).reset_index(drop=True)
                    full_result_df = full_result_df[['Route_ID', 'Priority', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID',
                        'Destinations', 'Product', 'Order_Quantity', 'Utilization', 'Retain_Hour', 'Runout','End_Stock_Day', 
                        'Travel_Time', 'Accessibility', 'Tank_Status','Min_Retain_Hour','Min_Runout','Multiload_Status','Cloud','condition','grouping', 'order_status', 'long_distance']]
                    full_result_df['condition'] = full_result_df['condition'].str.replace(r'_[0-9]+','')
                
                else :
                    pass
                
                if len(unmatched_log) > 0 : 
                    full_unmatched_reshape = full_unmatched_log.copy()
                    full_unmatched_reshape['idx'] = full_unmatched_reshape.groupby('Route_ID').cumcount()
                    tmp = []
                    for var in ['Destinations', 'Priority', 'Terminal_97', 'Terminal_Default', 'Order_ID', 'Product', 'Order_Quantity', 'Order_Weight', 'Retain_Hour', 'Runout','End_Stock_Day', 'Accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'ori_status']:
                        column_list = []
                        full_unmatched_reshape['tmp_idx'] = var + '_' + full_unmatched_reshape.idx.astype(str)
                        if self.region == "CENTRAL" :
                            column_list.extend("{}_{}".format(var, num) for num in range(4))
                        if self.region in ["SOUTHERN", "EASTERN"] :
                            column_list.extend("{}_{}".format(var, num) for num in range(6))
                        if self.region == "NORTHERN" :
                            column_list.extend("{}_{}".format(var, num) for num in range(6))
                        original_pivoted_df = full_unmatched_reshape.pivot(index='Route_ID',columns='tmp_idx',values=var)
                        standard_pivoted_df = original_pivoted_df.reindex(original_pivoted_df.columns.union(column_list, sort=False), axis=1, fill_value=0)
                        standard_pivoted_df[column_list] = standard_pivoted_df[column_list].replace({'0':np.nan, 0:np.nan})
                        tmp.append(standard_pivoted_df)
                        #tmp.append(full_unmatched_reshape.pivot(index='Route_ID',columns='tmp_idx',values=var))
                    reshape = pd.concat(tmp,axis=1).reset_index(inplace=False)

                    N = 1
                    c = sorted(reshape.columns[N:], key=lambda x: int(x.split('_')[-1]))
                    reshape_new = reshape.reindex(reshape.columns[:N].tolist() + c, axis=1)
                    reshape_new = reshape_new.merge(full_unmatched_log[['Route_ID', 'Cloud', 'No_Compartment', 'Tanker_Cap', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'violation_log']], on='Route_ID', how='left').drop_duplicates()

                    if self.region == "CENTRAL" :    
                        col = ['Destinations_0','Destinations_1','Destinations_2','Destinations_3']
                    if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                        col = ['Destinations_0', 'Destinations_1', 'Destinations_2', 'Destinations_3', 'Destinations_4', 'Destinations_5']
                    reshape_new[col] = reshape_new[col].replace('nan', np.nan).fillna(0)
                    new_col = reshape_new['Destinations_0'].astype(int).astype(str) + '_' + reshape_new['Destinations_1'].astype(int).astype(str) + '_' + reshape_new['Destinations_2'].astype(int).astype(str) + '_' + reshape_new['Destinations_3'].astype(int).astype(str)
                    if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                        new_col += '_' + reshape_new['Destinations_4'].astype(int).astype(str) + '_' + reshape_new['Destinations_5'].astype(int).astype(str)
                    reshape_new.insert(loc=1, column='Destinations', value=new_col)
                    full_unmatched_log = reshape_new.copy()

                    if self.region == "CENTRAL" : 
                        full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3']] = full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3']].astype(str)
                    if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                        full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3','Order_ID_4','Order_ID_5']] = full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3','Order_ID_4','Order_ID_5']].astype(str)
                    #column_toconvert = ['Retain_Hour_0','Retain_Hour_1','Retain_Hour_2','Retain_Hour_3']
                    #full_unmatched_log[column_toconvert] = pd.to_datetime(full_unmatched_log[column_toconvert].stack(), errors='coerce').unstack()

                # Merging full_result_df with station_time according to the respective terminal
                if len(full_result_df) > 0 :
                    if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                        full_result_df = InputHandler.multi_terminal_station_time_merge(
                            full_result_df, station_time, stn_data_terminal_col='terminal_95', terminal_95_col='Terminal_Default', terminal_97_col='Terminal_97', product_col='Product')
                    else:
                        full_result_df = pd.merge(full_result_df, station_time, 'left', left_on=['Destinations'], right_on=['Ship-to'])
                    
                    time_list = ['Opening_Time','Closing_Time']
                    for col in time_list :
                        full_result_df[col] = change_datetimetofloat_format(full_result_df[col])
                    full_result_df['DMR_Date'] = self.date
                    full_result_df['Loop_Counter'] = self.loop_status
                    full_result_df['Job_StartTime'] = self.start_time
                    full_result_df['Txn_ID'] = self.txn_id
                    full_result_df['Region'] = self.region
                    full_result_df['Order_Type'] = 'RE'
                    full_result_df['delta'] = 0
                    full_result_df = full_result_df[['Route_ID','Terminal_97','Terminal_Default','No_Compartment','Tanker_Cap','Order_ID','Destinations','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Utilization','Retain_Hour','Runout','End_Stock_Day','Travel_Time','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Min_Retain_Hour','Min_Runout','DMR_Date','Loop_Counter','Job_StartTime','Txn_ID','Region','Order_Type','delta']]
                else :
                    full_result_df = pd.DataFrame()

                if len(commercial_data) > 0 :
                    full_result_df = full_result_df.append(commercial_data)
                
                if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                    unmatched_df = InputHandler.multi_terminal_station_time_merge(
                        unmatched_df, station_time, stn_data_terminal_col='terminal_95', terminal_95_col='Terminal_Default', terminal_97_col='Terminal_97', product_col='Product')
                else:
                    unmatched_df = pd.merge(unmatched_df, station_time, 'left', left_on=['Destinations'], right_on=['Ship-to'])

                time_list = ['Opening_Time','Closing_Time']
                for col in time_list :
                    unmatched_df[col] = change_datetimetofloat_format(unmatched_df[col])                
                unmatched_df = unmatched_df[['Destinations','Terminal_97','Terminal_Default','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Order_Type']]
                if len(unmatched_commercial) > 0 :
                    unmatched_commercial = unmatched_commercial[['Destinations','Terminal_97','Terminal_Default','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Order_Type']]
                    unmatched_df = unmatched_df.append(unmatched_commercial)
                    
                unmatched_df['DMR_Date'] = self.date
                unmatched_df['Loop_Counter'] = self.loop_status
                unmatched_df['Job_StartTime'] = self.start_time
                unmatched_df['Txn_ID'] = self.txn_id
                unmatched_df['Region'] = self.region
                unmatched_df = unmatched_df[['Destinations','Terminal_97','Terminal_Default','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','DMR_Date','Loop_Counter','Job_StartTime','Txn_ID','Region','Order_Type']]

                if len(unmatched_log) > 0 : 
                    full_unmatched_log['DMR_Date'] = self.date
                    full_unmatched_log['Loop_Counter'] = self.loop_status
                    full_unmatched_log['Job_StartTime'] = self.start_time
                    full_unmatched_log['Txn_ID'] = self.txn_id
                    full_unmatched_log['Region'] = self.region
                else :
                    full_unmatched_log = pd.DataFrame()

                full_result_df['Strategy'] = self.rts_strategy
                unmatched_df['Strategy'] = self.rts_strategy

                full_result_df['Filter'] = full_result_df['Order_Type'].astype(str) + '_' + full_result_df['DMR_Date'].astype(str) + '_' + full_result_df['Txn_ID'].astype(str) + '_' + full_result_df['Region'].astype(str) + '_' + full_result_df['Strategy'].astype(str)
                unmatched_df['Filter'] = unmatched_df['Order_Type'].astype(str) + '_' + unmatched_df['DMR_Date'].astype(str) + '_' + unmatched_df['Txn_ID'].astype(str) + '_' + unmatched_df['Region'].astype(str) + '_' + unmatched_df['Strategy'].astype(str)

                print("[RouteGenerator] Postprocessing route output for {} in {} is completed...".format(self.loop_status, self.txn_id))

                # Ensure data types are correct for important column
                convert_dict = {
                'Product': int,
                'Order_Quantity': float
                }
                full_result_df = full_result_df.astype(convert_dict)

                db = True
                if db:
                    DBConnector.save(full_result_df, Config.TABLES['route_output']["tablename"], if_exists='append')
                    DBConnector.save(unmatched_df, Config.TABLES['unmatched_data']["tablename"], if_exists='append')
                    try:
                        DBConnector.save(full_unmatched_log, Config.TABLES['unmatched_log']["tablename"], if_exists='append')
                        print("[RouteGenerator] Unmatched logs for {} in {} successfully pushed to DB.".format(self.loop_status, self.txn_id))
                    except Exception as err :
                        print("[RouteGenerator] Unmatched logs for {} in {} failed to push to DB due to {}.".format(self.loop_status, self.txn_id, err))
                    print("[RouteGenerator] All routes and unmatched orders for {} in {} are loaded successfully--.".format(self.loop_status, self.txn_id))
                else:
                    full_result_df.to_csv('INT_rts_route_output.csv', index=False)
                    unmatched_df.to_csv('INT_rts_route_unmatched_orders.csv',index=False)
                    full_unmatched_log.to_csv('INT_rts_route_unmatched_log', index=False)
                    print("All Routes and Unmatched Orders are loaded successfully to csv.")

                end_time = datetime.now()
                diff = end_time - self.start_time
                print("[RouteGenerator] Total runtime for route generator for {} in {} : {}".format(self.loop_status, self.txn_id, diff))
                if len(full_result_df) > 0 :
                    full_result_df_display = full_result_df.copy()
                    full_result_df_display['grouping'] = full_result_df_display['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_multiload_early': 'combined', 'single_multiload_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_normal_early': 'combined', 'single_normal_late': 'combined', 'single_multiload': 'single'})
                    print("[RouteGenerator] Total number of orders need to be scheduled for {} in {} : {}".format(self.loop_status, self.txn_id, full_result_df_display.groupby(['Destinations','Product','condition','grouping']).ngroups))
                    print("[RouteGenerator] Total number of orders are unmatched for {} in {} : {}".format(self.loop_status, self.txn_id, len(unmatched_df)))
                    print("[RouteGenerator] Total number of route for {} in {} : {}".format(self.loop_status, self.txn_id, full_result_df.groupby(['Route_ID']).ngroups))
                print("[RouteGenerator] Route Generator module for {} in {} is completed".format(self.loop_status, self.txn_id))
            else : 
                result_df = pd.DataFrame()
        
        elif len(result_df) < 1 :
            if self.loop_status.split('_')[1] == "First" and self.opo2_first == False :  
                grouping = ['combined','combined_1','combined_2']

            elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                grouping = ['combined','combined_1','combined_2','combined_early','combined_late']

            else :
                pass
        
            if len(duplicate_station_full) > 0 :
                duplicate_station_full = duplicate_station_full[duplicate_station_full['grouping'].isin(grouping)]
                duplicate_station_full['Station_ID_Product'] = duplicate_station_full['Ship To'].astype(str) + '_' +  duplicate_station_full['Multiload_Status'].astype(str) + '_' + duplicate_station_full['condition'] + '_' + duplicate_station_full['grouping'] 
                duplicate_station_full['Station_ID_Product'] = duplicate_station_full['Station_ID_Product'].astype('category')
                duplicate_station_full = duplicate_station_full[duplicate_station_full.columns.difference(['Tank_Status', 'Multiload_Status'])]

                unmatched_log['Station_ID_Product'] = unmatched_log['Destinations'].astype(str) + '_' + unmatched_log['Multiload_Status'].astype(str) + '_' + unmatched_log['condition'] + '_' + unmatched_log['grouping']
                unmatched_log['Station_ID_Product'] = unmatched_log['Station_ID_Product'].astype('category')
                unmatched_log_filter = unmatched_log[~unmatched_log['Station_ID_Product'].isin(duplicate_station_full['Station_ID_Product'])].reset_index(drop=True)
                unmatched_log = unmatched_log[unmatched_log['Station_ID_Product'].isin(duplicate_station_full['Station_ID_Product'])].reset_index(drop=True)
                unmatched_log = unmatched_log.drop(['Station_ID_Product'], axis=1)
                unmatched_log_filter = unmatched_log_filter.drop(['Station_ID_Product'], axis=1)
            
                if self.loop_status.split('_')[1] == "First" and self.opo2_first == False :  
                    merged_unmatched = pd.merge(duplicate_station_full, unmatched_log, left_on=['Ship To','condition','grouping'], right_on=['Destinations','condition','grouping'])
                    merged_unmatched = merged_unmatched[['Ship To', 'Priority_x', 'Terminal_97', 'Terminal_Default', 'Material', 'Quantity', 'Total_weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Remark','Runout_x','End_Stock_Day_x','Route_ID',
                        'No_Compartment', 'Tanker_Cap', 'Order_ID','Accessibility','Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status_x', 'combine_tag', 'long_distance', 'violation_log']]
                    merged_unmatched = merged_unmatched.rename(columns = {'Ship To':'Destinations', 'Priority_x':'Priority', 'Material':'Product', 'Remark':'Retain_Hour', 'Runout_x':'Runout', 'Quantity':'Order_Quantity','order_status_x':'order_status','Total_weight':'Order_Weight','End_Stock_Day_x':'End_Stock_Day'})
                    merged_unmatched['ori_status'] = 'original'
                    merged_unmatched = merged_unmatched[['Route_ID', 'Priority', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID', 'Destinations',
                                                    'Product', 'Order_Quantity', 'Order_Weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Retain_Hour', 'Runout', 'End_Stock_Day','Accessibility',
                                                    'Tank_Status', 'Multiload_Status','Cloud','condition', 'grouping',
                                                    'order_status', 'combine_tag', 'long_distance', 'ori_status','violation_log']]
                
                elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                    merged_unmatched = pd.merge(duplicate_station_full, unmatched_log, left_on=['Ship To','condition','grouping'], right_on=['Destinations','condition','grouping'])
                    merged_unmatched = merged_unmatched[['Ship To', 'Priority_x', 'Terminal_97', 'Terminal_Default', 'Material', 'Quantity', 'Total_weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Remark', 'Runout_x', 'End_Stock_Day_x', 'Route_ID',
                        'No_Compartment', 'Tanker_Cap', 'Order_ID','Accessibility','Tank_Status', 'Multiload_Status','Cloud','condition','grouping', 'order_status_x','ori_status_x', 'combine_tag', 'long_distance', 'violation_log']]
                    merged_unmatched = merged_unmatched.rename(columns = {'Ship To':'Destinations', 'Priority_x':'Priority', 'Material':'Product', 'Remark':'Retain_Hour', 'Runout_x':'Runout','Quantity':'Order_Quantity','order_status_x':'order_status','Total_weight':'Order_Weight','ori_status_x':'ori_status','End_Stock_Day_x':'End_Stock_Day'})
                    merged_unmatched = merged_unmatched[['Route_ID', 'Priority', 'Terminal_97', 'Terminal_Default', 'No_Compartment', 'Tanker_Cap', 'Order_ID', 'Destinations',
                                                    'Product', 'Order_Quantity', 'Order_Weight', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'Retain_Hour', 'Runout', 'End_Stock_Day', 'Accessibility',
                                                    'Tank_Status', 'Multiload_Status','Cloud','condition', 'grouping',
                                                    'order_status', 'combine_tag', 'long_distance', 'ori_status','violation_log']]   

                else :
                    pass          
            
            else :
                merged_unmatched = unmatched_log.copy()
                unmatched_log_filter = pd.DataFrame()

            merged_unmatched['Sequence'] = merged_unmatched.groupby(['Route_ID','Order_ID']).cumcount()
            cols = ['Order_ID','Sequence']
            merged_unmatched['Order_ID'] = merged_unmatched[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
            merged_unmatched = merged_unmatched.drop(['Sequence'], axis=1)

            full_unmatched_log = pd.concat([unmatched_log_filter, merged_unmatched], axis = 0, sort=False).reset_index(drop=True)
            full_unmatched_log = full_unmatched_log.drop(['combine_tag'], axis=1)
            
            if self.loop_status.split('_')[1] == "First" and self.opo2_first == False : 
                full_unmatched_log['ori_status'] = 'original'

            print("[RouteGenerator] No Route output. Postprocessing UNMATCHED orders for {} in {} is being initiated...".format(self.loop_status, self.txn_id))
            if self.loop_status.split('_')[1] == "First" and self.opo2_first == False :  
                unmatched_df = order_data_full

            elif self.loop_status.split('_')[1] == "Second" or self.opo2_first == True :
                ori_data_full = order_data_full[order_data_full['ori_status'] == 'original']
                ori_data_full['condition'] = ori_data_full['condition'].str.replace(r'_[0-9]+','')
                ori_data_full = ori_data_full.drop_duplicates(subset=['Ship-to','Material','Qty','Multiload_Status','condition','grouping','ori_status'], keep="first")
                unmatched_df = ori_data_full
            
            else :
                pass

            forecast_data = InputHandler.get_forecast_data(self.order_id, self.date)
            inventory_data_tomorrow = InputHandler.get_inventory_data(self.order_id, self.date)
            
            for idx, order in unmatched_df.iterrows():
                End_Stock_Day = end_day_stock_1(order, inventory_data_tomorrow, forecast_data)
                unmatched_df.at[idx,'End_Stock_Day'] = End_Stock_Day
           
            if self.region == "CENTRAL":
                unmatched_df = pd.merge(unmatched_df, master_terminal_data, 'left', left_on=['Ship-to'], right_on=['acc'])
            else:
                pass

            unmatched_df['Order_Type'] = "RE"
            unmatched_df = unmatched_df[['Ship-to','Priority','terminal_97','terminal_95','Material','Qty','retain_hour','Runout','End_Stock_Day','accessibility','Tank_Status','Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud2','Order_Type']]
            unmatched_df = unmatched_df.rename(columns = {'Ship-to':'Destinations', 'terminal_97':'Terminal_97', 'terminal_95':'Terminal_Default', 'Material':'Product', 'retain_hour':'Retain_Hour', 'Qty':'Order_Quantity', 'Cloud2':'Cloud','accessibility':'Accessibility'}).reset_index(drop=True)
            
            # Merging unmatched_df with station_time according to the respective terminal
            if self.region == 'EASTERN':
                unmatched_df = InputHandler.multi_terminal_station_time_merge(
                    unmatched_df, station_time, stn_data_terminal_col='terminal', terminal_95_col='Terminal_Default', terminal_97_col='Terminal_97', product_col='Product')
            else:
                unmatched_df = pd.merge(unmatched_df, station_time, 'left', left_on=['Destinations'], right_on=['Ship-to'])

            time_list = ['Opening_Time','Closing_Time']
            for col in time_list :
                unmatched_df[col] = change_datetimetofloat_format(unmatched_df[col])   
            unmatched_df = unmatched_df[['Destinations','Terminal_97','Terminal_Default','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Order_Type']]
            if len(unmatched_commercial) > 0 :
                unmatched_commercial = unmatched_commercial[['Destinations','Terminal_97','Terminal_Default','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','Order_Type']]
                unmatched_df = unmatched_df.append(unmatched_commercial)
            
            unmatched_df['DMR_Date'] = self.date
            unmatched_df['Loop_Counter'] = self.loop_status
            unmatched_df['Job_StartTime'] = self.start_time
            unmatched_df['Txn_ID'] = self.txn_id
            unmatched_df['Region'] = self.region
            unmatched_df = unmatched_df[['Destinations','Terminal_97','Terminal_Default','Product','Order_Quantity','Opening_Time','Closing_Time','Priority','Retain_Hour','Runout','End_Stock_Day','Accessibility','Tank_Status','Multiload_Status','condition','grouping','order_status','long_distance','Cloud','DMR_Date','Loop_Counter','Job_StartTime','Txn_ID','Region','Order_Type']]

            full_unmatched_reshape = full_unmatched_log.copy()
            full_unmatched_reshape['idx'] = full_unmatched_reshape.groupby('Route_ID').cumcount()
            tmp = []
            for var in ['Destinations', 'Priority', 'Terminal_97', 'Terminal_Default', 'Order_ID', 'Product', 'Order_Quantity', 'Order_Weight', 'Retain_Hour', 'Runout','End_Stock_Day', 'Accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'ori_status']:
                column_list = []
                full_unmatched_reshape['tmp_idx'] = var + '_' + full_unmatched_reshape.idx.astype(str)
                if self.region == "CENTRAL" :
                    column_list.extend("{}_{}".format(var, num) for num in range(4))
                if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                    column_list.extend("{}_{}".format(var, num) for num in range(6))                
                original_pivoted_df = full_unmatched_reshape.pivot(index='Route_ID',columns='tmp_idx',values=var)
                standard_pivoted_df = original_pivoted_df.reindex(original_pivoted_df.columns.union(column_list, sort=False), axis=1, fill_value=0)
                standard_pivoted_df[column_list] = standard_pivoted_df[column_list].replace({'0':np.nan, 0:np.nan})
                tmp.append(standard_pivoted_df)
                #tmp.append(full_unmatched_reshape.pivot(index='Route_ID',columns='tmp_idx',values=var))
            reshape = pd.concat(tmp,axis=1).reset_index(inplace=False)

            N = 1
            c = sorted(reshape.columns[N:], key=lambda x: int(x.split('_')[-1]))
            reshape_new = reshape.reindex(reshape.columns[:N].tolist() + c, axis=1)
            reshape_new = reshape_new.merge(full_unmatched_log[['Route_ID', 'Cloud', 'No_Compartment', 'Tanker_Cap', 'Tanker_DryWeight', 'Tanker_WeightLimit', 'violation_log']], on='Route_ID', how='left').drop_duplicates()
            
            if self.region == "CENTRAL" :    
                col = ['Destinations_0','Destinations_1','Destinations_2','Destinations_3']
            if self.region in ["SOUTHERN", "EASTERN", " NORTHERN"] :
                col = ['Destinations_0', 'Destinations_1', 'Destinations_2', 'Destinations_3', 'Destinations_4', 'Destinations_5']
            reshape_new[col] = reshape_new[col].replace('nan', np.nan).fillna(0)
            new_col = reshape_new['Destinations_0'].astype(int).astype(str) + '_' + reshape_new['Destinations_1'].astype(int).astype(str) + '_' + reshape_new['Destinations_2'].astype(int).astype(str) + '_' + reshape_new['Destinations_3'].astype(int).astype(str)
            if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                new_col += '_' + reshape_new['Destinations_4'].astype(int).astype(str) + '_' + reshape_new['Destinations_5'].astype(int).astype(str)
            reshape_new.insert(loc=1, column='Destinations', value=new_col)
            full_unmatched_log = reshape_new.copy()

            if self.region == "CENTRAL" : 
                full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3']] = full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3']].astype(str)
            if self.region in ["SOUTHERN", "EASTERN", "NORTHERN"] :
                full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3','Order_ID_4','Order_ID_5']] = full_unmatched_log[['Order_ID_0','Order_ID_1','Order_ID_2','Order_ID_3','Order_ID_4','Order_ID_5']].astype(str)
            full_unmatched_log['DMR_Date'] = self.date
            full_unmatched_log['Loop_Counter'] = self.loop_status
            full_unmatched_log['Job_StartTime'] = self.start_time
            full_unmatched_log['Txn_ID'] = self.txn_id
            full_unmatched_log['Region'] = self.region

            if len(commercial_data) > 0 :
                full_result_df = commercial_data.copy()
                full_result_df['Strategy'] = self.rts_strategy
                full_result_df['Filter'] = full_result_df['Order_Type'].astype(str) + '_' + full_result_df['DMR_Date'].astype(str) + '_' + full_result_df['Txn_ID'].astype(str) + '_' + full_result_df['Region'].astype(str) + '_' + full_result_df['Strategy'].astype(str)
            else :
                full_result_df = pd.DataFrame()

            unmatched_df['Strategy'] = self.rts_strategy
            unmatched_df['Filter'] = unmatched_df['Order_Type'].astype(str) + '_' + unmatched_df['DMR_Date'].astype(str) + '_' + unmatched_df['Txn_ID'].astype(str) + '_' + unmatched_df['Region'].astype(str) + '_' + unmatched_df['Strategy'].astype(str)

            print("[RouteGenerator] Postprocessing UNMATCHED orders for {} in {} is completed...".format(self.loop_status, self.txn_id))

            # Ensure data types are correct for important column
            convert_dict = {
            'Product': int,
            'Order_Quantity': float
            }
            full_result_df = full_result_df.astype(convert_dict)

            db = True
            if db:
                DBConnector.save(full_result_df, Config.TABLES['route_output']["tablename"], if_exists='append')
                DBConnector.save(unmatched_df, Config.TABLES['unmatched_data']["tablename"], if_exists='append')                
                try:
                    DBConnector.save(full_unmatched_log, Config.TABLES['unmatched_log']["tablename"], if_exists='append')
                    print("[RouteGenerator] Unmatched logs for {} in {} successfully pushed to DB.".format(self.loop_status, self.txn_id))
                except Exception as err :
                    print("[RouteGenerator] Unmatched logs for {} in {} failed to push to DB due to {}.".format(self.loop_status, self.txn_id, err))
                print("[RouteGenerator] No Routes can be generated for {} in {}".format(self.loop_status, self.txn_id))
                print("[RouteGenerator] All unmatched orders : {} are loaded successfully to SQL database.".format(len(unmatched_df)))                
            else:
                full_result_df.to_csv('INT_rts_route_output.csv', index=False)
                unmatched_df.to_csv('INT_rts_route_unmatched_orders.csv',index=False)
                full_unmatched_log.to_csv('INT_rts_route_unmatched_log', index=False)
                print("All Routes and Unmatched Orders are loaded successfully to csv.")
            
            # unmatched_df['Runout'] = unmatched_df['Runout'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # unmatched_df['DMR_Date'] = unmatched_df['DMR_Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # unmatched_df['Job_StartTime'] = unmatched_df['Job_StartTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

            end_time = datetime.now()
            diff = end_time - self.start_time
            print("[RouteGenerator] Total runtime for route generator for {} in {} : {}".format(self.loop_status, self.txn_id, diff))
            print("[RouteGenerator] Route Generator module for {} in {} is completed".format(self.loop_status, self.txn_id))
        else :
            raise Exception("[RouteGenerator] Routing has no results, please check. System existing ...") 

if __name__ == "__main__" :
    #txn_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    date = datetime(2022, 2, 25)
    region = 'Northern'
    loop_1 = '43680_First'
    loop_2 = '43680_Second'
    module_1 = 'Route'
    module_2 = 'OPO 2'
    txn_id = '8PTQEU' # random id just to trigger the class to run
    start_time_loop = datetime.now()
    capacity = 43680
    largest_capacity = 43680
    RouteCompile(loop_1, date , region, txn_id, start_time, capacity, largest_capacity) 
