import os
import math 
import pandas as pd
import sys
import random
import string
import time
import sys
import numpy as np
import json
from datetime import datetime, timedelta
import src.rts_routing_modules.rts_route_main as route_generator
import src.rts_scheduling_modules.rts_scheduling_main as milp_scheduling
from src.input_gen_modules.rts_input_handler import InputHandler
from database_connector import DatabaseConnector, YAMLFileConnector, PandasFileConnector
from conf import Logger
from conf import Config
from sqlalchemy import create_engine
import requests
from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

def push_result_orderbank(txn_id) :
    
    # Gather final result and push back into order_bank
    print("Begin data post processing output to order bank...")
    scheduled_order = InputHandler.get_scheduled_data(txn_id)
    order_data = InputHandler.get_processed_ori_orders(txn_id)
    
    # Merge order id from AWSM into scheduled orders
    scheduled_order['grouping'] = scheduled_order['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_multiload': 'single', 'single_multiload_early':'single', 'single_multiload_late':'single'})
    scheduled_order['Station_ID_Product'] = scheduled_order['Destinations'].astype(str) + '_' + scheduled_order['Product'].astype(str) + '_' + scheduled_order['Multiload_Status'].astype(str) + '_' + scheduled_order['condition'].astype(str)  + '_' + scheduled_order['grouping'].astype(str)  + '_' + scheduled_order['order_status'].astype(str)
    order_data['Station_ID_Product'] = order_data['Ship-to'].astype(str) + '_' + order_data['Material'].astype(str) + '_' + order_data['Multiload_Status'].astype(str) + '_' + order_data['condition'].astype(str)  + '_' + order_data['grouping'].astype(str)  + '_' + order_data['order_status'].astype(str)
    scheduled_order = pd.merge(scheduled_order, order_data[['Station_ID_Product','id']], 'left', on = ['Station_ID_Product'])

    # Tag Scheduled and Unscheduled order id for AWSM based on scheduled orders
    order_data_status =  order_data[['Station_ID_Product','id']]
    order_data_status['scheduled_status'] = np.where(order_data_status['id'].isin(scheduled_order['id']), 'Scheduled', 'Unscheduled')

    # unscheduled_order_status = order_data_status[~order_data_status['id'].isin(scheduled_order['id'])]

    # Rename required columns to be pushed into order_bank
    scheduled_order = scheduled_order.rename(columns={'Route_ID':'route_id','Vehicle Number':'vehicle','Trip':'trip_no','Time start route':'planned_load_time','ETA':'eta','Quantity_carried':'ds_quantity','Time complete route':'planned_end_time'})
    scheduled_order = scheduled_order[~scheduled_order['id'].isnull()]

    scheduled_order['scheduled_status'] = "Scheduled"

    # Add 1 second to planned_load_time to avoid overlapping in different routes due to milliseconds different rounding in DB
    scheduled_order['planned_load_time'] = scheduled_order['planned_load_time'] + timedelta(seconds=1)

    # Subset scheduled orders by odd compartment tankers
    tanker_compartment = InputHandler.get_truck_compartment(scheduled_order['vehicle'].unique())
    tanker_compartment['min_comp'] = tanker_compartment.groupby('road_tanker')['max_volume'].transform('min')
    tanker_odd_comp = tanker_compartment[tanker_compartment['min_comp'] != 5460]
    scheduled_order_normal = scheduled_order[~scheduled_order['vehicle'].isin(tanker_odd_comp['road_tanker'].unique())]
    scheduled_order_odd = scheduled_order[scheduled_order['vehicle'].isin(tanker_odd_comp['road_tanker'].unique())]

    # Create order sequence for normal compartment tanker route in Central
    def createList(r1, r2):
        return [item for item in range(r1, r2+1)]

    group_id = scheduled_order_normal.groupby(['route_id'])    
    df_bygroup = [group_id.get_group(x) for x in group_id.groups]

    append_df = []
    sequenced_scheduled_order_normal = pd.DataFrame()

    for i in df_bygroup :
        if len(i['Destinations'].unique()) > 1 :
            i.sort_values('eta', inplace=True)
            route_quantity = int(i['ds_quantity'].sum())
            route_compartment = int(route_quantity/5460)
            route_mid_compartment = int(math.floor(route_compartment/2))

            station_id = i.groupby(['Destinations'])
            df_bystation = [station_id.get_group(x) for x in station_id.groups]
            
            tanker_mid_comp = 0
            next_station_comp = 0
            quantity_left = route_quantity
            full_1stload = False

            for j in df_bystation :
                j['compartment'] = 0
                j['order_sequence'] = None
                first_prod = []
                second_prod = []
                third_prod = []
                station_quantity = int(j['ds_quantity'].sum())
                station_compartment = int(station_quantity/5460)
                quantity_left -= station_quantity
                
                if tanker_mid_comp == 0 and quantity_left > 0:
                    if station_compartment >= route_mid_compartment :
                        # full_1stload = True
                        tanker_mid_comp = route_mid_compartment - (int(station_compartment) - route_mid_compartment)
                        if len(j) > 2 :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp, tanker_mid_comp - 1 + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            j['compartment'].iloc[2] = int(j['ds_quantity'].iloc[1]/5460)
                            third_prod = createList((max(second_prod) + 1), (max(second_prod) + j['compartment'].iloc[2]))
                            j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                            # next_station_comp = min(first_prod + second_prod + third_prod)
                            min_station_comp = min(first_prod + second_prod + third_prod)
                            max_station_comp = max(first_prod + second_prod + third_prod)                              
                        
                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp, tanker_mid_comp - 1 + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            # next_station_comp = min(first_prod + second_prod)
                            min_station_comp = min(first_prod + second_prod)
                            max_station_comp = max(first_prod + second_prod)                           

                        else :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp, tanker_mid_comp - 1 + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            # next_station_comp = min(first_prod)
                            min_station_comp = min(first_prod)
                            max_station_comp = max(first_prod)

                    else :
                        tanker_mid_comp = int(route_mid_compartment)
                        if len(j) > 2 :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp + 1, tanker_mid_comp + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            j['compartment'].iloc[2] = int(j['ds_quantity'].iloc[1]/5460)
                            third_prod = createList((max(second_prod) + 1), (max(second_prod) + j['compartment'].iloc[2]))
                            j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                            min_station_comp = min(first_prod + second_prod + third_prod)
                            max_station_comp = max(first_prod + second_prod + third_prod)                   

                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp + 1, tanker_mid_comp + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            min_station_comp = min(first_prod + second_prod)
                            max_station_comp = max(first_prod + second_prod)

                        else :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp + 1, tanker_mid_comp + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            min_station_comp = min(first_prod)
                            max_station_comp = max(first_prod)                                                

                else :
                    if full_1stload :
                        if len(j) > 2 :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList((next_station_comp - j['compartment'].iloc[0]), (next_station_comp - 1))
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                            second_prod = createList((min(first_prod) - j['compartment'].iloc[1]), (min(first_prod) - 1))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            j['compartment'].iloc[2] = int(j['ds_quantity'].iloc[1]/5460)
                            third_prod = createList((min(second_prod) - j['compartment'].iloc[2]), (min(second_prod) - 1))
                            j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                        
                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList((next_station_comp - j['compartment'].iloc[0]), (next_station_comp - 1))
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                            second_prod = createList((min(first_prod) - j['compartment'].iloc[1]), (min(first_prod) - 1))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

                        else :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            first_prod = createList((next_station_comp - j['compartment'].iloc[0]), (next_station_comp - 1))
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])

                    else :
                        if len(j) > 2 :
                            
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            if j['compartment'].iloc[0] < (route_compartment - max_station_comp) :
                                first_prod = createList(max_station_comp + 1, max_station_comp + j['compartment'].iloc[0])
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            elif j['compartment'].iloc[0] == (route_compartment - max_station_comp) :
                                first_prod = createList(max_station_comp + 1, route_compartment)
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            else :
                                balance_comp = j['compartment'].iloc[0] - (route_compartment - max_station_comp)
                                first_prod_1 = createList(max_station_comp + 1, route_compartment)
                                first_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                first_prod = first_prod_1 + first_prod_2
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])

                            max_station_comp = max(first_prod)
                            balance_comp = station_compartment - (route_compartment - max_station_comp)
                            
                            j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                            if j['compartment'].iloc[1] < (route_compartment - max_station_comp) :
                                second_prod = createList(max_station_comp + 1, max_station_comp + j['compartment'].iloc[1])
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                                j['compartment'].iloc[2] = int(j['ds_quantity'].iloc[2]/5460)
                                max_station_comp = max(second_prod)
                                balance_comp = j['compartment'].iloc[1] - (route_compartment - max_station_comp)
                                third_prod_1 = createList(max_station_comp + 1, route_compartment)
                                third_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                third_prod = third_prod_1 + third_prod_2
                                j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                            
                            elif j['compartment'].iloc[1] == (route_compartment - max_station_comp) :
                                second_prod = createList(max_station_comp + 1, route_compartment)
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                                j['compartment'].iloc[2] = int(j['ds_quantity'].iloc[2]/5460)
                                third_prod = createList((min_station_comp - j['compartment'].iloc[2]), (min_station_comp - 1))
                                j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])

                            else :
                                balance_comp = j['compartment'].iloc[1] - (route_compartment - max_station_comp)
                                second_prod_1 = createList(max_station_comp + 1, route_compartment)
                                second_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                second_prod = second_prod_1 + second_prod_2
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                                j['compartment'].iloc[2] = int(j['ds_quantity'].iloc[2]/5460)
                                third_prod = createList((min(second_prod) - j['compartment'].iloc[2]), (min(second_prod) - 1))
                                j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])

                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            if j['compartment'].iloc[0] < (route_compartment - max_station_comp) :
                                # start_comp = route_compartment - (route_compartment - max_station_comp)
                                # balance_comp = (route_compartment - max_station_comp) - j['compartment'].iloc[0]
                                first_prod = createList(max_station_comp + 1, max_station_comp + j['compartment'].iloc[0])
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                                j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                                max_station_comp = max(first_prod)
                                balance_comp = j['compartment'].iloc[1] - (route_compartment - max_station_comp)
                                second_prod_1 = createList(max_station_comp + 1, route_compartment)
                                second_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                second_prod = second_prod_1 + second_prod_2
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

                            elif j['compartment'].iloc[0] == (route_compartment - max_station_comp) :
                                first_prod = createList(max_station_comp + 1, route_compartment)
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                                j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                                second_prod = createList((min_station_comp - j['compartment'].iloc[1]), (min_station_comp - 1))
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

                            else :
                                balance_comp = j['compartment'].iloc[0] - (route_compartment - max_station_comp)
                                first_prod_1 = createList(max_station_comp + 1, route_compartment)
                                first_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                first_prod = first_prod_1 + first_prod_2
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                                j['compartment'].iloc[1] = int(j['ds_quantity'].iloc[1]/5460)
                                second_prod = createList((min(first_prod) - j['compartment'].iloc[1]), (min(first_prod) - 1))
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

                        else :
                            j['compartment'].iloc[0] = int(j['ds_quantity'].iloc[0]/5460)
                            balance_comp = j['compartment'].iloc[0] - (route_compartment - max_station_comp)
                            first_prod_1 = createList(max_station_comp + 1, route_compartment)
                            first_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                            first_prod = first_prod_1 + first_prod_2
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
            
                append_df.append(j)    
        
        else :
            i['compartment'] = 0
            i['order_sequence'] = None
            first_prod = []
            second_prod = []
            third_prod = []
            if len(i) > 2 :
                i['compartment'].iloc[0] = int(i['ds_quantity'].iloc[0]/5460)
                first_prod = createList(1, i['compartment'].iloc[0])
                i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                i['compartment'].iloc[1] = int(i['ds_quantity'].iloc[1]/5460)
                second_prod = createList((max(first_prod) + 1), (max(first_prod) + i['compartment'].iloc[1]))
                i['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                i['compartment'].iloc[2] = int(i['ds_quantity'].iloc[2]/5460)
                third_prod = createList((max(second_prod) + 1), (max(second_prod) + i['compartment'].iloc[2]))
                # third_prod_2 = createList(1,1)
                # third_prod = third_prod_1 + third_prod_2
                i['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
            
            elif len(i) > 1 :
                i['compartment'].iloc[0] = int(i['ds_quantity'].iloc[0]/5460)
                first_prod = createList(1, i['compartment'].iloc[0])
                i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                i['compartment'].iloc[1] = int(i['ds_quantity'].iloc[1]/5460)
                second_prod = createList((max(first_prod) + 1), (max(first_prod) + i['compartment'].iloc[1]))
                # second_prod_2 = createList(1,1)
                # second_prod = second_prod_1 + second_prod_2
                i['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

            else :
                i['compartment'].iloc[0] = int(i['ds_quantity'].iloc[0]/5460)
                first_prod = createList(1, i['compartment'].iloc[0])
                i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
    
            append_df.append(i)
    
    sequenced_scheduled_order_normal = pd.concat(append_df)
    sequenced_scheduled_order_normal['order_sequence'] = sequenced_scheduled_order_normal['order_sequence'].astype(str)


    # Create order sequence for odd compartment tanker (10920) route in Central 
    # TODO : To make it more robust for other region, currently hard coded based on 10920 odd compartment for Central
    if len(scheduled_order_odd) > 0 :
        group_id = scheduled_order_odd.groupby(['route_id'])    
        df_bygroup = [group_id.get_group(x) for x in group_id.groups]

        append_df = []
        sequenced_scheduled_order_odd = pd.DataFrame()

        for i in df_bygroup :
            if len(i['Destinations'].unique()) > 1 :
                i.sort_values('eta', inplace=True)

                station_id = i.groupby(['Destinations'])
                df_bystation = [station_id.get_group(x) for x in station_id.groups]

                tanker_mid_comp = 0
                quantity_left = route_quantity

                for j in df_bystation :
                    j['compartment'] = 0
                    j['order_sequence'] = None
                    first_prod = []
                    station_quantity = int(j['ds_quantity'].sum())
                    quantity_left -= station_quantity
                    
                    if tanker_mid_comp == 0 and quantity_left > 0:
                        first_prod = createList(2, 2)
                        j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                    
                    else :
                        first_prod_1 = createList(3, 3)
                        first_prod_2 = createList(1, 1)
                        first_prod = first_prod_1 + first_prod_2
                        j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                    
                    append_df.append(j)

            else :
                first_prod = []
                second_prod = []
                i['order_sequence'] = None
                if len(i) > 1 :
                    first_prod = createList(2, 2)
                    i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                    second_prod_1 = createList(3, 3)
                    second_prod_2 = createList(1, 1)
                    second_prod = second_prod_1 + second_prod_2 
                    i['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod]) 
                else :
                    first_prod = createList(1, 3)
                    i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])              

                append_df.append(i)           

        sequenced_scheduled_order_odd = pd.concat(append_df)
        sequenced_scheduled_order_odd['order_sequence'] = sequenced_scheduled_order_odd['order_sequence'].astype(str)

        sequenced_scheduled_order_odd['ds_quantity_per_compartment'] = np.where(sequenced_scheduled_order_odd['order_sequence'] == "1,2,3", "2730,5460,2730", np.where(sequenced_scheduled_order_odd['order_sequence'] == "2", "5460", "2730,2730"))
        sequenced_scheduled_order_odd = sequenced_scheduled_order_odd[['id','route_id','vehicle','trip_no','planned_load_time','eta','ds_quantity','planned_end_time','scheduled_status','ds_quantity_per_compartment','order_sequence']]

    else :
        sequenced_scheduled_order_odd = pd.DataFrame()

    # Create multiple quantity for compartment
    def repeat(string, n, delim):
        return delim.join(string for _ in range(n))

    sequenced_scheduled_order_normal['compartment_division'] = sequenced_scheduled_order_normal['ds_quantity']/5460
    sequenced_scheduled_order_normal['compartment_division'] = sequenced_scheduled_order_normal['compartment_division'].astype(int)
    sequenced_scheduled_order_normal['ds_quantity_per_compartment'] = '5460'    

    sequenced_scheduled_order_normal['ds_quantity_per_compartment']  = sequenced_scheduled_order_normal.apply(lambda x: repeat(x.ds_quantity_per_compartment, x.compartment_division, ",") ,axis=1)
    sequenced_scheduled_order_normal = sequenced_scheduled_order_normal.drop(['compartment_division','compartment'], axis=1)

    sequenced_scheduled_order_normal = sequenced_scheduled_order_normal[['id','route_id','vehicle','trip_no','planned_load_time','eta','ds_quantity','planned_end_time','scheduled_status','ds_quantity_per_compartment','order_sequence']]

    df = [sequenced_scheduled_order_normal, sequenced_scheduled_order_odd]
    scheduled_order_upload = pd.concat(df)

    scheduled_order_upload['trip_start_time'] = scheduled_order_upload['planned_load_time']

    awsm_output = scheduled_order_upload.copy()
    awsm_output['id'] = awsm_output['id'].astype(int)

    # check_id = awsm_output[awsm_output.duplicated('id')]['id'].unique()
    # check_route_list = awsm_output[awsm_output['id'].isin(check_id)]['route_id'].unique()
    # awsm_output = awsm_output[~awsm_output['route_id'].isin([check_route_list[0]])]

    print("Data post processing output to order bank complete, initiating db update...")
    DBConnector.update_table(awsm_output,'order_bank','id')
    print("update db complete")

    order_status_dict = {}
    for index, row in order_data_status.iterrows():
        row['id'] = str(row['id'])
        order_status_dict[row['id']] = row['scheduled_status']

    print("returning final result")
    
    return order_status_dict