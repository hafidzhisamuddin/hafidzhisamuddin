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

from regex import I
import src.rts_routing_modules.rts_route_main as route_generator
import src.rts_scheduling_modules.rts_scheduling_main as milp_scheduling
from src.input_gen_modules.rts_input_handler import InputHandler
from src.rts_routing_modules.rts_routing_utilities import renaming_product_id, forecast_tomorrow_not_opo, inv_tomorrow, get_alert, convert_time_interval, change_timedelta_format, change_strtodatetime_format, change_datetimetofloat_format, texify_numeric_station_id
from database_connector import DatabaseConnector, YAMLFileConnector, PandasFileConnector
from conf import Logger
from conf import Config
from sqlalchemy import create_engine
import requests
import itertools
from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

def scheduled_order_analysis(txn_id, date, region, opo2_first, rts_strategy, truck_data) :
    
    # Gather final result and push back into order_bank
    print("Begin data post processing output to analytic dataset")
    scheduled_order = InputHandler.get_scheduled_data(txn_id)
    order_data = InputHandler.get_processed_ori_orders(txn_id)

    scheduled_order['Station_Leaving_Time'] = scheduled_order['ETA'] + timedelta(minutes=30)
    scheduled_order['ETA'] = scheduled_order['ETA'] + timedelta(seconds=1)

    def cal_overlapped_time(df) :
        df = df.drop_duplicates(subset=['Destinations','ETA','Station_Leaving_Time'], keep="first")
        df = df[['ETA','Station_Leaving_Time']]
        df = df.stack().to_frame()
        df = df.reset_index(level=1)
        df.columns = ['status', 'time']
        df = df.reset_index().drop('index', axis=1)
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time')

        new_df = pd.melt(df,id_vars="time",value_name="status")
        new_df.drop(columns=["variable"],inplace=True)
        # new_df['counter'] = np.where(new_df['status'].eq('ETA'),1,-1).cumsum()
        new_df['Overlap_Time'] = new_df['status'].map({'ETA':1,'Station_Leaving_Time':-1}).cumsum()
        return new_df

    overlap_grouped = scheduled_order.groupby("Destinations").apply(cal_overlapped_time).reset_index()
    overlap_grouped['Overlap_Time'] = np.where(overlap_grouped['Overlap_Time']==1,0, overlap_grouped['Overlap_Time'])
    overlap_grouped = overlap_grouped.drop_duplicates(subset=['Destinations','Overlap_Time'], keep="first")
    
    if len(overlap_grouped[overlap_grouped['Overlap_Time']>1]) > 0 :
        print("Found overlapped time !!!")
        print(overlap_grouped[overlap_grouped['Overlap_Time']>1])
        pass

    scheduled_order = pd.merge(scheduled_order, overlap_grouped[['Destinations','Overlap_Time']], 'left', on = ['Destinations'])

    scheduled_order['grouping'] = scheduled_order['grouping'].replace({'combined_early': 'combined', 'combined_late': 'combined', 'single_early': 'single', 'single_late': 'single', 'single_multiload': 'single', 'single_multiload_early':'single', 'single_multiload_late':'single'})
    scheduled_order['Station_ID_Product'] = scheduled_order['Destinations'].astype(str) + '_' + scheduled_order['Product'].astype(str) + '_' + scheduled_order['Multiload_Status'].astype(str) + '_' + scheduled_order['condition'].astype(str)  + '_' + scheduled_order['grouping'].astype(str)  + '_' + scheduled_order['order_status'].astype(str)
    order_data['Station_ID_Product'] = order_data['Ship-to'].astype(str) + '_' + order_data['Material'].astype(str) + '_' + order_data['Multiload_Status'].astype(str) + '_' + order_data['condition'].astype(str)  + '_' + order_data['grouping'].astype(str)  + '_' + order_data['order_status'].astype(str)
    scheduled_order = pd.merge(scheduled_order, order_data[['Station_ID_Product','id','Qty','retain_hour','Runout']], 'left', on = ['Station_ID_Product'])
    scheduled_order = scheduled_order.rename(columns={'Qty':'Quantity_Ori','id':'order_id','retain_hour':'Retain_Ori','Runout_x':'Runout','Runout_y':'Runout_Ori'})
    order_id = scheduled_order['order_id'].unique()

    scheduled_order['Quantity_diff'] = scheduled_order['Quantity_carried'] - scheduled_order['Quantity_Ori']
    scheduled_order['OPO2'] = np.where(scheduled_order['Quantity_diff'] == 0, 0, 1)

    # Get retail master for scheduled orders
    order_retail_list = scheduled_order['Destinations'].tolist()
    
    # Change numeric station id into "00XXX..."
    modified_retail_list = texify_numeric_station_id(order_retail_list)

    retail_master = InputHandler.get_retail_master(region, modified_retail_list, date)

    # Replace NaN sold_to_party with ship_to_party and change numeric into "00XXX..."
    retail_master['sold_to_party'] = retail_master['sold_to_party'].fillna(retail_master['ship_to_party'].apply(lambda x: x))
    retail_master['sold_to_party'] = texify_numeric_station_id(retail_master['sold_to_party'])

    # TODO : To add more if else based on region specific long distance condition
    # Set value for long distance based on region
    if Config.MODEL_INPUTOUTPUT['model_region'][region] == "CENTRAL" :
        retail_master['ld_tag'] = Config.MODEL_INPUTOUTPUT['long_distance'][region]
    else :
        retail_master.ld_tag = retail_master.ld_tag.fillna('non_LD') 

    # Rename to old column schema from AWSM schema      
    retail_master = retail_master.rename(columns={'ship_to_party':'acc','cloud': 'Cloud2', 'distance':'distance_terminal_u95','road_tanker_accessibility':'accessibility','ld_tag':'long_distance'})
                       
    # Standardize product id based on config
    retail_master = renaming_product_id(retail_master,"product")

    # Get forecast and tomorrow opening inventory for scheduled orders
    forecast_data = InputHandler.get_forecast_data(order_id, date)
    inventory_data_tomorrow = InputHandler.get_inventory_data(order_id, date)

    # Impute missing SMP orders End_Stock_Day
    scheduled_order['End_Stock_Day'] = np.where(scheduled_order['End_Stock_Day'].isnull(), 0, scheduled_order['End_Stock_Day'])

    def end_day_stock(order_quantity, inv_tmr, forecast_tmr):
        try :
            end_stock_day = (inv_tmr + order_quantity - forecast_tmr)/forecast_tmr
        except Exception as err :
            return print("END STOCK DAY ERROR")

        return round(end_stock_day, 2)    

    # Generate End_Stock_Day, Max_Stock_Day and Alert before/after OPO2
    def opo_info(order, master_all, forecast_data, inventory_data_tomorrow) :
        if order['Product']==70020771: 
            tank_size = master_all[(master_all['acc']==order['Destinations'])&(master_all['product']==order['Product'])]['tank_capacity'].iloc[0]  
        elif order['Product']==70100346:
            tank_size = master_all[(master_all['acc']==order['Destinations'])&(master_all['product']==order['Product'])]['tank_capacity'].iloc[0]  
        elif order['Product']==70000011:
            tank_size = master_all[(master_all['acc']==order['Destinations'])&(master_all['product']==order['Product'])]['tank_capacity'].iloc[0]
        else:
            tank_size = 0

        try:
            inv_tmr = inv_tomorrow(inventory_data_tomorrow, order['Destinations'], order['Product'])
        except Exception as err :
            print(err, order['Destinations'], order['Product'])
        try:
            forecast_tmr = forecast_tomorrow_not_opo(forecast_data, order['Destinations'], order['Product'])
        except Exception as err :
            print(err, order['Destinations'], order['Product'])

        try:
            max_stock_day = round((float(tank_size) * 0.876) / forecast_tmr,2)
        except Exception as err :
            max_stock_day = order['End_Stock_Day']

        end_stock_day_ori = end_day_stock(order['Quantity_Ori'], inv_tmr, forecast_tmr)

        alert = get_alert(order['End_Stock_Day'], max_stock_day, inv_tmr)
        alert_ori = get_alert(end_stock_day_ori, max_stock_day, inv_tmr)

        return end_stock_day_ori, max_stock_day, alert, alert_ori


    for index, order in scheduled_order.iterrows(): 
        if order['Order_Type'] == "CO" or order['order_status'] == "smp":
            scheduled_order.loc[index,'End_Stock_Day_Ori'] = 0
            scheduled_order.loc[index,'Max_Stock_Day'] = 0
            scheduled_order.loc[index,'Alert'] = "COMMERCIAL/SMP"
            scheduled_order.loc[index,'Alert_Ori'] = "COMMERCIAL/SMP"
        else :
            scheduled_order.loc[index,'End_Stock_Day_Ori'], scheduled_order.loc[index,'Max_Stock_Day'], scheduled_order.loc[index,'Alert'], scheduled_order.loc[index,'Alert_Ori'] = opo_info(order, retail_master, forecast_data, inventory_data_tomorrow)
            

    # Subset scheduled orders by odd compartment tankers
    tanker_compartment = InputHandler.get_truck_compartment(scheduled_order['Vehicle Number'].unique())
    tanker_compartment['min_comp'] = tanker_compartment.groupby('road_tanker')['max_volume'].transform('min')
    tanker_odd_comp = tanker_compartment[tanker_compartment['min_comp'] != 5460]
    scheduled_order_normal = scheduled_order[~scheduled_order['Vehicle Number'].isin(tanker_odd_comp['road_tanker'].unique())]
    scheduled_order_odd = scheduled_order[scheduled_order['Vehicle Number'].isin(tanker_odd_comp['road_tanker'].unique())]

    # Create order sequence for normal compartment tanker route in Central
    def createList(r1, r2):
        return [item for item in range(r1, r2+1)]

    group_id = scheduled_order_normal.groupby(['Route_ID'])    
    df_bygroup = [group_id.get_group(x) for x in group_id.groups]

    append_df = []
    sequenced_scheduled_order_normal = pd.DataFrame()

    for i in df_bygroup :
        if len(i['Destinations'].unique()) > 1 :
            i.sort_values('ETA', inplace=True)
            route_quantity = int(i['Quantity_carried'].sum())
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
                station_quantity = int(j['Quantity_carried'].sum())
                station_compartment = int(station_quantity/5460)
                quantity_left -= station_quantity
                
                if tanker_mid_comp == 0 and quantity_left > 0:
                    if station_compartment >= route_mid_compartment :
                        # full_1stload = True
                        tanker_mid_comp = route_mid_compartment - (int(station_compartment) - route_mid_compartment)
                        if len(j) > 2 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp, tanker_mid_comp - 1 + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            j['compartment'].iloc[2] = int(j['Quantity_carried'].iloc[1]/5460)
                            third_prod = createList((max(second_prod) + 1), (max(second_prod) + j['compartment'].iloc[2]))
                            j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                            # next_station_comp = min(first_prod + second_prod + third_prod)
                            min_station_comp = min(first_prod + second_prod + third_prod)
                            max_station_comp = max(first_prod + second_prod + third_prod)                              
                        
                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp, tanker_mid_comp - 1 + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            # next_station_comp = min(first_prod + second_prod)
                            min_station_comp = min(first_prod + second_prod)
                            max_station_comp = max(first_prod + second_prod)                           

                        else :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp, tanker_mid_comp - 1 + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            # next_station_comp = min(first_prod)
                            min_station_comp = min(first_prod)
                            max_station_comp = max(first_prod)

                    else :
                        tanker_mid_comp = int(route_mid_compartment)
                        if len(j) > 2 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp + 1, tanker_mid_comp + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            j['compartment'].iloc[2] = int(j['Quantity_carried'].iloc[1]/5460)
                            third_prod = createList((max(second_prod) + 1), (max(second_prod) + j['compartment'].iloc[2]))
                            j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                            min_station_comp = min(first_prod + second_prod + third_prod)
                            max_station_comp = max(first_prod + second_prod + third_prod)                        

                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp + 1, tanker_mid_comp + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                            second_prod = createList((max(first_prod) + 1), (max(first_prod) + j['compartment'].iloc[1]))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            min_station_comp = min(first_prod + second_prod)
                            max_station_comp = max(first_prod + second_prod)

                        else :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList(tanker_mid_comp + 1, tanker_mid_comp + j['compartment'].iloc[0])
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            min_station_comp = min(first_prod)
                            max_station_comp = max(first_prod)                                                

                else :
                    if full_1stload :
                        if len(j) > 2 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList((next_station_comp - j['compartment'].iloc[0]), (next_station_comp - 1))
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                            second_prod = createList((min(first_prod) - j['compartment'].iloc[1]), (min(first_prod) - 1))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                            j['compartment'].iloc[2] = int(j['Quantity_carried'].iloc[1]/5460)
                            third_prod = createList((min(second_prod) - j['compartment'].iloc[2]), (min(second_prod) - 1))
                            j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                        
                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList((next_station_comp - j['compartment'].iloc[0]), (next_station_comp - 1))
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                            j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                            second_prod = createList((min(first_prod) - j['compartment'].iloc[1]), (min(first_prod) - 1))
                            j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

                        else :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            first_prod = createList((next_station_comp - j['compartment'].iloc[0]), (next_station_comp - 1))
                            j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])

                    else :
                        if len(j) > 2 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
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
                            
                            j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                            if j['compartment'].iloc[1] < (route_compartment - max_station_comp) :
                                second_prod = createList(max_station_comp + 1, max_station_comp + j['compartment'].iloc[1])
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                                j['compartment'].iloc[2] = int(j['Quantity_carried'].iloc[2]/5460)
                                max_station_comp = max(second_prod)
                                balance_comp = j['compartment'].iloc[1] - (route_compartment - max_station_comp)
                                third_prod_1 = createList(max_station_comp + 1, route_compartment)
                                third_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                third_prod = third_prod_1 + third_prod_2
                                j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
                            
                            elif j['compartment'].iloc[1] == (route_compartment - max_station_comp) :
                                second_prod = createList(max_station_comp + 1, route_compartment)
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                                j['compartment'].iloc[2] = int(j['Quantity_carried'].iloc[2]/5460)
                                third_prod = createList((min_station_comp - j['compartment'].iloc[2]), (min_station_comp - 1))
                                j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])

                            else :
                                balance_comp = j['compartment'].iloc[1] - (route_compartment - max_station_comp)
                                second_prod_1 = createList(max_station_comp + 1, route_compartment)
                                second_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                second_prod = second_prod_1 + second_prod_2
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                                j['compartment'].iloc[2] = int(j['Quantity_carried'].iloc[2]/5460)
                                third_prod = createList((min(second_prod) - j['compartment'].iloc[2]), (min(second_prod) - 1))
                                j['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])

                        elif len(j) > 1 :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
                            if j['compartment'].iloc[0] < (route_compartment - max_station_comp) :
                                # start_comp = route_compartment - (route_compartment - max_station_comp)
                                # balance_comp = (route_compartment - max_station_comp) - j['compartment'].iloc[0]
                                first_prod = createList(max_station_comp + 1, max_station_comp + j['compartment'].iloc[0])
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                                j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                                max_station_comp = max(first_prod)
                                balance_comp = j['compartment'].iloc[1] - (route_compartment - max_station_comp)
                                second_prod_1 = createList(max_station_comp + 1, route_compartment)
                                second_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                second_prod = second_prod_1 + second_prod_2
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod]) 

                            elif j['compartment'].iloc[0] == (route_compartment - max_station_comp) :
                                first_prod = createList(max_station_comp + 1, route_compartment)
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                                j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                                second_prod = createList((min_station_comp - j['compartment'].iloc[1]), (min_station_comp - 1))
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

                            else :
                                balance_comp = j['compartment'].iloc[0] - (route_compartment - max_station_comp)
                                first_prod_1 = createList(max_station_comp + 1, route_compartment)
                                first_prod_2 = createList((min_station_comp - balance_comp), (min_station_comp - 1))
                                first_prod = first_prod_1 + first_prod_2
                                j['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                                j['compartment'].iloc[1] = int(j['Quantity_carried'].iloc[1]/5460)
                                second_prod = createList((min(first_prod) - j['compartment'].iloc[1]), (min(first_prod) - 1))
                                j['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

                        else :
                            j['compartment'].iloc[0] = int(j['Quantity_carried'].iloc[0]/5460)
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
                i['compartment'].iloc[0] = int(i['Quantity_carried'].iloc[0]/5460)
                first_prod = createList(1, i['compartment'].iloc[0])
                i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                i['compartment'].iloc[1] = int(i['Quantity_carried'].iloc[1]/5460)
                second_prod = createList((max(first_prod) + 1), (max(first_prod) + i['compartment'].iloc[1]))
                i['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])
                i['compartment'].iloc[2] = int(i['Quantity_carried'].iloc[2]/5460)
                third_prod = createList((max(second_prod) + 1), (max(second_prod) + i['compartment'].iloc[2]))
                # third_prod_2 = createList(1,1)
                # third_prod = third_prod_1 + third_prod_2
                i['order_sequence'].iloc[2] = ','.join([str(i) for i in third_prod])
            
            elif len(i) > 1 :
                i['compartment'].iloc[0] = int(i['Quantity_carried'].iloc[0]/5460)
                first_prod = createList(1, i['compartment'].iloc[0])
                i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
                i['compartment'].iloc[1] = int(i['Quantity_carried'].iloc[1]/5460)
                second_prod = createList((max(first_prod) + 1), (max(first_prod) + i['compartment'].iloc[1]))
                # second_prod_2 = createList(1,1)
                # second_prod = second_prod_1 + second_prod_2
                i['order_sequence'].iloc[1] = ','.join([str(i) for i in second_prod])

            else :
                i['compartment'].iloc[0] = int(i['Quantity_carried'].iloc[0]/5460)
                first_prod = createList(1, i['compartment'].iloc[0])
                i['order_sequence'].iloc[0] = ','.join([str(i) for i in first_prod])
    
            append_df.append(i)
    
    sequenced_scheduled_order_normal = pd.concat(append_df)
    sequenced_scheduled_order_normal['order_sequence'] = sequenced_scheduled_order_normal['order_sequence'].astype(str)


    # Create order sequence for odd compartment tanker (10920) route in Central 
    # TODO : To make it more robust for other region, currently hard coded based on 10920 odd compartment for Central
    if len(scheduled_order_odd) > 0 :
        group_id = scheduled_order_odd.groupby(['Route_ID'])    
        df_bygroup = [group_id.get_group(x) for x in group_id.groups]

        append_df = []
        sequenced_scheduled_order_odd = pd.DataFrame()

        for i in df_bygroup :
            if len(i['Destinations'].unique()) > 1 :
                i.sort_values('ETA', inplace=True)

                station_id = i.groupby(['Destinations'])
                df_bystation = [station_id.get_group(x) for x in station_id.groups]

                tanker_mid_comp = 0
                quantity_left = route_quantity

                for j in df_bystation :
                    j['compartment'] = 0
                    j['order_sequence'] = None
                    first_prod = []
                    station_quantity = int(j['Quantity_carried'].sum())
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
    
    else :
        sequenced_scheduled_order_odd = None

    df = [sequenced_scheduled_order_normal, sequenced_scheduled_order_odd]
    scheduled_order_analysis = pd.concat(df)
    scheduled_order_analysis['Environment'] = Config.PROJECT_ENVIRONMENT
    scheduled_order_analysis['OPO2_First_Loop'] = str(opo2_first)
    # scheduled_order_analysis = scheduled_order_analysis[['Order_Type', 'Route_ID', 'Terminal_97', 'Terminal_Default', 'Tanker_Capacity', 'Vehicle Number', 'Trip', 'Time start route', 'PldTime', 'ETA', 'Station_Leaving_Time', 'Overlap_Time', 'Time complete route', 'Destinations', 'Product', 'Quantity_carried', 'Quantity_Ori', 'Quantity_diff', 'OPO2', 'Total_Quantity', 'Opening_Time', 'Closing_Time', 'Priority', 'Retain_Hour', 'Retain_Ori', 'Runout', 'End_Stock_Day', 'End_Stock_Day_Ori', 'Max_Stock_Day', 'Alert', 'Alert_Ori', 'Travel_Time', 'Accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud', 'Min_Retain_Hour', 'order_id', 'order_sequence', 'Environment', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Txn_ID', 'Region', 'Strategy', 'OPO2_First_Loop', 'Filter']]
    
    scheduled_order_analysis = convert_time_interval('Opening_Time', 'Closing_Time', scheduled_order_analysis, date)
    scheduled_order_analysis['violate_station_time'] = np.where(scheduled_order_analysis['ETA'] > scheduled_order_analysis['Closing_Time'], 1, 0)
    scheduled_order_analysis['violate_accessibility'] = np.where(scheduled_order_analysis['Tanker_Capacity'] > scheduled_order_analysis['Accessibility'], 1, 0)
    scheduled_order_analysis['empty_compartment'] = np.where(scheduled_order_analysis['Total_Quantity'] < scheduled_order_analysis['Tanker_Capacity'], 1, 0)
    scheduled_order_analysis['violate_retain'] = np.where(scheduled_order_analysis['ETA'] < scheduled_order_analysis['Retain_Hour'], 1, 0)
    scheduled_order_analysis['violate_runout'] = np.where(scheduled_order_analysis['ETA'] > scheduled_order_analysis['Runout'], 1, 0)

    scheduled_order_analysis = scheduled_order_analysis[['Order_Type', 'Route_ID', 'Terminal_97', 'Terminal_Default', 'Tanker_Capacity', 'empty_compartment', 'Vehicle Number', 'Trip', 'Time start route', 'PldTime', 'ETA', 'Retain_Hour', 'Retain_Ori', 'Runout', 'Runout_Ori', 'Min_Retain_Hour', 'Station_Leaving_Time', 'Opening_Time', 'Closing_Time', 'Overlap_Time', 'violate_station_time', 'violate_retain', 'violate_runout', 'Time complete route', 'Travel_Time', 'Destinations', 'Product', 'Quantity_carried', 'Quantity_Ori', 'Quantity_diff', 'OPO2', 'Total_Quantity', 'Priority', 'End_Stock_Day', 'End_Stock_Day_Ori', 'Max_Stock_Day', 'Alert', 'Alert_Ori', 'Accessibility', 'violate_accessibility', 'Tank_Status', 'Multiload_Status', 'condition', 'grouping', 'order_status', 'long_distance', 'Cloud', 'order_id', 'order_sequence', 'Environment', 'DMR_Date', 'Loop_Counter', 'Job_StartTime', 'Txn_ID', 'Region', 'Strategy', 'OPO2_First_Loop', 'Filter']]

    DBConnector.save(scheduled_order_analysis, Config.TABLES['awsm_analysis']["tablename"], if_exists='append')

    
    scheduled_time_analysis = scheduled_order_analysis.copy()

    def get_station_restriction(station_list, retail_or_comm, date) :
        if retail_or_comm == 'RE' :
            station_restriction = InputHandler.get_retail_dates(station_list)
        else :
            station_restriction = InputHandler.get_commercial_dates(station_list)

        time_list = ['Opening_Time','Closing_Time','no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
        for col in time_list :
            station_restriction[col] = change_timedelta_format(station_restriction[col])
                    
        time_list = ['Opening_Time','Closing_Time','no_delivery_start_2','no_delivery_start_1','no_delivery_end_2','no_delivery_end_1']
        for col in time_list :
            station_restriction[col] = change_strtodatetime_format(station_restriction[col], date + timedelta(days=1))  
                                         
        station_restriction = station_restriction.rename(columns = {"retail":"Destinations"})
        return station_restriction

    retail_active_region_list = retail_master[Config.TABLES['retail_customer_master']["columns"]["acc2"]].unique().ravel().tolist()
    retail_active_region_list = [str(station).zfill(10) for station in retail_active_region_list]
    order_retail_tag = scheduled_order_analysis[scheduled_order_analysis['Order_Type'] == 'RE']['Order_Type'].unique()

    scheduled_comm_list = scheduled_order_analysis[scheduled_order_analysis['Order_Type'] == 'CO']['Destinations'].unique().ravel().tolist()
    order_comm_tag = scheduled_order_analysis[scheduled_order_analysis['Order_Type'] == 'CO']['Order_Type'].unique() 

    if len(retail_active_region_list) > 0 :
        retail_restriction = get_station_restriction(retail_active_region_list, order_retail_tag, date)
    else :
        retail_restriction = pd.DataFrame()        
    if len(scheduled_comm_list) > 0 :
        comm_restriction = get_station_restriction(scheduled_comm_list, order_comm_tag, date)
    else :
        comm_restriction = pd.DataFrame()

    station_restriction = retail_restriction.append(comm_restriction, ignore_index=True)

    def add_restriction(df, date) :
        station = df['Destinations'].unique().ravel().tolist()
        tanker = df['Vehicle Number'].unique().ravel().tolist()
        keys = itertools.product(tanker, station)
        temp = pd.DataFrame.from_records([{'Vehicle Number': tanker, 'Destinations': station} for tanker, station in keys])
        temp = temp.merge(station_restriction[['Destinations','no_delivery_start_1','no_delivery_end_1','no_delivery_start_2','no_delivery_end_2']], how="left", on=['Destinations'])
        temp = temp.rename(columns={'no_delivery_start_1':'nodeliverystart_1','no_delivery_end_1': 'nodeliveryend_1', 'no_delivery_start_2':'nodeliverystart_2','no_delivery_end_2':'nodeliveryend_2'})
        temp = pd.wide_to_long(
            temp.reset_index(),
            i=['Vehicle Number','Destinations'],
            stubnames=['nodeliverystart','nodeliveryend'],
            j='value',
            sep='_'
            ).sort_index().reset_index()
        temp['Vehicle Number'] = temp['Destinations'].astype(str)
        temp = temp.rename(columns={'nodeliverystart':'Time start route','nodeliveryend': 'Time complete route'})
        date = pd.to_datetime(date) + timedelta(days=1)
        temp = temp[~(temp['Time start route'].isin([date]) & temp['Time complete route'].isin([date]))]
        def station_eta_tuple(data):
            data['Destinations_ETA'] = str(set(list(zip(data['Destinations'], data['ETA']))))
            return data
        df = df.groupby("Route_ID").apply(lambda x: station_eta_tuple(x)).reset_index()        
        new_df = df.merge(temp[['Vehicle Number','Destinations','Time start route','Time complete route']], how='outer', on=['Vehicle Number','Destinations'])

        cols = ['Tanker_Capacity','DMR_Date','Txn_ID','Region','Strategy','OPO2_First_Loop','Filter']
        new_df.loc[:,cols] = new_df.loc[:,cols].ffill()
        new_df['Time start route_x'] = np.where(new_df['Time start route_x'].isnull(), new_df['Time start route_y'], new_df['Time start route_x'])
        new_df['Time complete route_x'] = np.where(new_df['Time complete route_x'].isnull(), new_df['Time complete route_y'], new_df['Time complete route_x'])
        new_df.drop(columns=['Time start route_y','Time complete route_y'],inplace=True)
        new_df = new_df.rename(columns={'Vehicle Number':'Schedule_Type','Time start route_x':'Time start route','Time complete route_x': 'Time complete route'})
        return new_df

    scheduled_w_restriction = scheduled_time_analysis.groupby(['Vehicle Number']).apply(lambda x: add_restriction(x, date)).reset_index()
    DBConnector.save(scheduled_w_restriction, Config.TABLES['awsm_gann_chart']["tablename"], if_exists='append')

    
    # Enrich tanker master data with run information
    truck_data['DMR_Date'] = date
    truck_data['Txn_ID'] = txn_id
    truck_data['Region'] = region
    truck_data['Strategy'] = rts_strategy
    truck_data['RT_TxnID'] = truck_data['Vehicle Number'].astype(str) + "_" + truck_data['Txn_ID'].astype(str)

    DBConnector.save(truck_data, Config.TABLES['awsm_tanker_analysis_master']["tablename"], if_exists='append')




