# -*- coding: utf-8 -*-
from __future__ import division
import os
import sys
from datetime import *
import pyomo.environ as pyo
from pyomo.environ import *
from pyomo.core.expr.numeric_expr import LinearExpression, SumExpression
from pyomo.core.base.constraint import _GeneralConstraintData
from pyomo.core.base.numvalue import NumericConstant
import pandas as pd
import time, re
import random
import numpy as np
from pyomo.util.infeasible import log_infeasible_constraints
import time
from src.rts_scheduling_modules.rts_dictionary import Resource
from src.input_gen_modules.rts_input_handler import InputHandler
from src.rts_routing_modules.distance_calc_commercial import open_route_comm_distance_cal
from src.rts_routing_modules.rts_routing_utilities import open_route_distance_cal
from sqlalchemy import create_engine
import os
from conf import Config, Logger
#from line_profiler import LineProfiler
import dask.dataframe as dd
import requests

from database_connector import DatabaseConnector, YAMLFileConnector
from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

st = time.perf_counter()
class MILP(object):

    def __init__(self, workload_obj, order_data, truck_data, truck_shift_balance, region, loop_status, rts_strategy, opo2_first = False):
        """
        MILP model framework which comprises objective function, parameters, decision variables and constraints
        for assigning routes to road tankers and scheduling the road tankers.

        type order_data: dataframe
        param order_data: dataframe of both retails and commercial routes
        type truck_data: dataframe
        param truck_data: dataframe of road tankers
        type loop_status: string
        param loop_status: current loop iteration
        """
        self.model = pyo.ConcreteModel()

        self.order_data = order_data
        # self.order_data = self.order_data.dropna()
        self.workload_obj = workload_obj
        self.truck_data = truck_data
        self.truck_shift_balance = truck_shift_balance
        self.loop_status = loop_status
        self.region = region
        self.rts_strategy = rts_strategy
        self.opo2_first = opo2_first
        
        self.dist_matrix = InputHandler.get_dist_matrix(self.region)
        self.dist_matrix = self.dist_matrix.drop_duplicates(subset=['First_location','Next_location'], keep="first")

        group_id = self.order_data.groupby(['Destinations'])    
        df_bygroup_siteid = [group_id.get_group(x) for x in group_id.groups]

        append_df = []

        for i in df_bygroup_siteid:
            if i['Multiload_Status'].iloc[0] == "Normal":
                if i['condition'].iloc[0] == "commercial":
                    i['Order_NO'] = i['Destinations'].astype(str) + '_' + i['Product'].astype(str) + '_' + i['grouping'].astype(str)
                else : 
                    i['Order_NO'] =   i['Destinations'].astype(str) + '_' + i['Product'].astype(str)
            else :
                for index, j in i.iterrows() :
                    if j['grouping'] in ['combined_1','combined','combined_early','single_early','single_1','single_multiload_early','single_normal_early']:
                        i.loc[index,'Order_NO'] = str(j['Destinations']) + '_1'
                    elif "LD" in j['grouping'] :
                        i.loc[index,'Order_NO'] = str(j['Destinations']) + '_' + str(j['Product'])
                    else :
                        i.loc[index,'Order_NO'] = str(j['Destinations']) + '_2'
            append_df.append(i)

        self.order_data = pd.concat(append_df)

        # extract unique capacity available in tanker in used for scheduling
        self.tanker_capacity = sorted(self.truck_data['Vehicle maximum vol.'].unique().astype(str),reverse=True)

        # extract unique terminal-capacity combinations available in tanker in used for scheduling
        self.truck_data['terminal_capacity'] = self.truck_data['default_terminal'].astype(str) + '_' + self.truck_data['Vehicle maximum vol.'].astype(str)
        self.terminal_plus_capacity = sorted(self.truck_data['terminal_capacity'].unique().astype(str),reverse=True)        

        resource_dict = {}

        for resource in self.truck_data.iterrows():
            try:
                resource_dict[resource[1]['Vehicle Number']].trip_start.append(resource[1]['Time start route'])
                resource_dict[resource[1]['Vehicle Number']].trip_end.append(resource[1]['Time complete route'])
            except KeyError:
                resource_dict[resource[1]['Vehicle Number']] = Resource(resource[1]['Vehicle Number'], resource[1]['balance_shift'],
                    (resource[1]['Vehicle maximum vol.']), (resource[1]['Vehicle maximum vol.']/5460), resource[1]['chattering_type'], resource[1]['start_shift'],
                    resource[1]['end_shift'], resource[1]['Time start route'], resource[1]['Time complete route'], resource[1]['owner'], resource[1]['Score'],
                    resource[1]['depot_start_time'], resource[1]['depot_close_time'] - 1, resource[1]['terminal_capacity'])
        self.resource_dict = dict(list(resource_dict.items()))
        # self.resource_dict = resource_dict

        self.model.dict_station_visit = self.order_data[['Order_NO','Route_ID']].drop_duplicates(subset=['Order_NO','Route_ID'], keep="first")
        self.model.dict_station_visit = self.model.dict_station_visit.groupby('Order_NO')['Route_ID'].apply(list).to_dict()

        self.order_data['Accessibility'] = self.order_data['Accessibility'].fillna(43680.0)

        order_data_dict = self.order_data.copy()

        # order_data_dict['Priority'] = order_data_dict['Priority'].replace({'High Priority':'High_Priority', 'Backlog Order':'Backlog_Order', 'Future Order':'Future_Order'})
        def max_string(s):
            return max(s, key=len)
        mapper = order_data_dict.groupby('Route_ID').agg({'Priority': max_string, 'Multiload_Status': max_string, 'Accessibility': min})[['Priority','Multiload_Status','Accessibility']].reset_index()
        order_data_dict['Priority'] = order_data_dict['Route_ID'].map(mapper.set_index('Route_ID')['Priority'])
        order_data_dict['Accessibility'] = order_data_dict['Route_ID'].map(mapper.set_index('Route_ID')['Accessibility'])
        order_data_dict['Multiload_Status'] = order_data_dict['Route_ID'].map(mapper.set_index('Route_ID')['Multiload_Status'])
        self.dict_route = order_data_dict[['Route_ID','Tanker_Cap','Accessibility','Travel_Time','Priority','Multiload_Status','Min_Retain_Hour','long_distance','min_runout_duration','Terminal_Default']].drop_duplicates(subset=['Route_ID','Tanker_Cap','Accessibility','Travel_Time','Priority','Multiload_Status','Min_Retain_Hour','long_distance','min_runout_duration','Terminal_Default'], keep="first")
        self.dict_route = self.dict_route.set_index('Route_ID').T.to_dict('list')

        self.dict_multiload = self.order_data[['Order_NO','Destinations','condition','grouping','Route_ID']].drop_duplicates(subset=['Order_NO','Destinations','condition','grouping','Route_ID'], keep="first")
        self.dict_multiload_route = self.dict_multiload.groupby(['Destinations','condition','grouping'])['Route_ID'].apply(list).to_dict() 
        self.dict_multiload_order = self.dict_multiload.groupby(['Destinations','condition','grouping'])['Order_NO'].unique().apply(list).to_dict()
        
        self.truck_shift_balance = self.truck_shift_balance[['Vehicle Number','Balance','balance_shift','start_shift','end_shift']]
        self.truck_shift_balance = self.truck_shift_balance.groupby('Vehicle Number')[['Balance','balance_shift','start_shift','end_shift']].apply(lambda g: list(map(tuple, g.values.tolist()))).to_dict()

        # self.terminal_capacity_tanker = self.truck_data[['terminal_capacity','Vehicle Number']].drop_duplicates(subset=['terminal_capacity','Vehicle Number'], keep="first")
        # self.terminal_capacity_tanker = self.terminal_capacity_tanker.groupby(['terminal_capacity'])['Vehicle Number'].apply(list).to_dict()
      
        def start_shift(model, k):
            return self.truck_shift_balance.get(k)[0][2]

        def end_shift(model, k):
            return self.truck_shift_balance.get(k)[0][3]

        def shift_balance(model, k):
            return self.truck_shift_balance.get(k)[0][0]

        def travel_time(model, r):
            return round(self.dict_route.get(r)[2],2)

        def station_visit(model, i, r):
            """
            To form a binary values, 1 if the order (i) presents in the particular route (r), otherwise 0.
            i: unique id of orders, it may/can have duplications (eg. destination_product)
            r: route unique id
            """

            if r in self.model.dict_station_visit.get(i) :
                return 1
            else :
                return 0


        def station_tier(model, r):
            """
            To set reward values for high priority order / backlog order / future order to be used in Objective function.
            r: route unique id
            """
            if self.loop_status.split('_')[1] == "First" :
                if 'High Priority' in self.dict_route.get(r)[3] :
                    return 100
                elif 'Backlog Order' in self.dict_route.get(r)[3] :   
                    return 100
                elif 'Multiload' in self.dict_route.get(r)[4] :
                    return 100
                else :
                    return 1
            else :
                if 'High Priority' in self.dict_route.get(r)[3] :
                    return 10
                elif 'Backlog Order' in self.dict_route.get(r)[3] : 
                    return 10
                elif 'Multiload' in self.dict_route.get(r)[4] :
                    return 10
                else :
                    return 1                


        def pto_tanker(model, k):
            """
            To generate binary values, 1 if the road tanker is equipped with PTO, otherwise 0.
            k: vehicle number of road tanker
            """            
            if 'PTO' in list(self.truck_data.loc[(self.truck_data['Vehicle Number'] == k)]['pump_type']):
                return 1
            else:
                return 0


        def diaphragm_tanker(model, k):
            """
            To generate binary values, 1 if the road tanker is equipped with Diaphragm, otherwise 0.
            k: vehicle number of road tanker
            """            
            if 'Diaphragm' in list(self.truck_data.loc[(self.truck_data['Vehicle Number'] == k)]['pump_type']):
                return 1
            else:
                return 0


        def highland_tanker(model, k):
            """
            To generate binary values, 1 if the road tanker is belong to Cameron Highland, otherwise 0.
            k: vehicle number of road tanker
            """
            if 'MCT8326BC' in list(self.truck_data.loc[(self.truck_data['Vehicle Number'] == k)]['Vehicle Number']):
                return 1
            elif 'MCT8926BC' in list(self.truck_data.loc[(self.truck_data['Vehicle Number'] == k)]['Vehicle Number']):
                return 1
            elif 'NDD8906BC' in list(self.truck_data.loc[(self.truck_data['Vehicle Number'] == k)]['Vehicle Number']):
                return 1
            else:
                return 0


        def retain_max(model, r):
            """
            Returning the value for latest retain of the route, r from order_data dataframe.
            """            
            # if 'CO' in self.dict_route.get(r)[3] :
            #     return self.dict_route.get(r)[5]
            return self.order_data.loc[(self.order_data['Route_ID'] == r)]['no_delivery_end_1'].max()            


        def get_retain_end(model, r):
            """
            Returning the value for runout of the route, r from order_data dataframe.
            """
            return order_data.loc[(order_data['Route_ID'] == r)]['no_delivery_start_2'].min()
    
            # try:
            #     if 'Multiload' in self.dict_route.get(r)[6] :
            #         return self.dict_route.get(r)[7] + self.dict_route.get(r)[10]
            #     elif 'LD' in self.dict_route.get(r)[8] :
            #         return 23.5
            #     elif 'CO' in self.dict_route.get(r)[3] :
            #         return self.dict_route.get(r)[9]
            #     else:
            #         return order_data.loc[(order_data['Route_ID'] == r)]['no_delivery_start_2'].min()
            # except KeyError:
            #     if 'CO' in self.dict_route.get(r)[3] :
            #         return self.dict_route.get(r)[9]
            #     else:
            #         return order_data.loc[(order_data['Route_ID'] == r)]['no_delivery_start_2'].min()


        def delta_2_destination(model, r):
            """
            single drop route : Returning the travel duration from depot to 1st station + loading time, if the route contains 1 station.
            double drops route : Returning the travel duration from depot to 1st station + loading time + unloading time at 1st station + travel duration from 1st station to 2nd station.
            three drops route : Returning the travel duration from depot to 1st station + loading time + unloading time at 1st station + travel duration from 1st station to 2nd station \
                                + unloading time at 2nd station + travel duration from 2nd station to 3rd station
            This function is used to ensure the road tanker departs from depot 'delta_2_destination' earlier before the minimum runout of route,
            so that the road tanker can arrive at station by or before the minimum runout of route.
            """            
            first_location = None
            second_location = None
            i = self.order_data[(self.order_data['Route_ID']==r)].reset_index()
            
            if len(i[i.Product.isin([70000011])]) > 0 :
                depot = str(i['Terminal_97'].iloc[0])
            else :
                depot = str(i['Terminal_Default'].iloc[0])
                
            i = i.sort_values(['Retain_Hour','Destinations'], ascending=[False, True]).drop_duplicates('Destinations', keep='first')
            i = i.sort_values('Retain_Hour')
            i['Destinations'] = i['Destinations'].astype(int)

            try :
                first_location = 1 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == depot) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[0])), 'Time_taken_hour'].iloc[0]
            except Exception as e:
                if i['order_status'].iloc[0] == "commercial" :
                    _ , curr_travel_time = open_route_comm_distance_cal(depot, str(i['Destinations'].iloc[0]), self.region) 
                else :               
                    _ , curr_travel_time = open_route_distance_cal(depot, str(i['Destinations'].iloc[0]), self.region)
                first_location = curr_travel_time            
            if len(i) == 1:
                eta = first_location + 0.5
            else:
                try :
                    second_location = first_location + 0.5 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == str(i['Destinations'].iloc[0])) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[1])), 'Time_taken_hour'].iloc[0]
                except Exception as e:
                    if i['order_status'].iloc[0] == "commercial" :
                        _ , curr_travel_time = open_route_comm_distance_cal(str(i['Destinations'].iloc[0]), str(i['Destinations'].iloc[1]), self.region) 
                    else :               
                        _ , curr_travel_time = open_route_distance_cal(str(i['Destinations'].iloc[0]), str(i['Destinations'].iloc[1]), self.region)
                    second_location = curr_travel_time + first_location + 0.5 
                if len(i) == 2:
                    eta = second_location + 0.5
                else:
                    try :
                        eta = second_location + 1 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == str(i['Destinations'].iloc[1])) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[2])), 'Time_taken_hour'].iloc[0]
                    except Exception as e:
                        if i['order_status'].iloc[0] == "commercial" :
                            _ , curr_travel_time = open_route_comm_distance_cal(str(i['Destinations'].iloc[1]), str(i['Destinations'].iloc[2]), self.region) 
                        else :               
                            _ , curr_travel_time = open_route_distance_cal(str(i['Destinations'].iloc[1]), str(i['Destinations'].iloc[2]), self.region)                        
                        eta = curr_travel_time + second_location + 1                     

            return round(eta, 2)
        

        def delta(model, r):
            """
            single drop route : Returning the travel duration from depot to 1st station + loading time, if the route contains 1 station.
            double drops route : Returning the travel duration from depot to 1st station + loading time + unloading time at 1st station + travel duration from 1st station to 2nd station.
            three drops route : Returning the travel duration from depot to 1st station + loading time + unloading time at 1st station + travel duration from 1st station to 2nd station \
                                + unloading time at 2nd station + travel duration from 2nd station to 3rd station
            This function is used to ensure the road tanker departs from depot 'delta' earlier before the maximum retain of route,
            so that the road tanker can arrive at station by or after the maximum retain of route.
            """    
            first_location = None
            second_location = None  
            third_location = None      
            
            # if 'CO' in (self.order_data.loc[(self.order_data['Route_ID'] == r)]['Order_Type'].to_list()):
            #     return round(self.order_data.loc[(self.order_data['Route_ID'] == r)]['delta'].iloc[0],2)
            # else:
            i = self.order_data[(self.order_data['Route_ID']==r)].reset_index()
            
            if len(i[i.Product.isin([70000011])]) > 0 :
                depot = str(i['Terminal_97'].iloc[0])
            else :
                depot = str(i['Terminal_Default'].iloc[0])  
                        
            i = i.sort_values(['Retain_Hour','Destinations'], ascending=[False, True]).drop_duplicates('Destinations', keep='first')
            i = i.sort_values('Retain_Hour')
            i['Destinations'] = i['Destinations'].astype(int)

            try :
                first_location = 1 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == depot) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[0])), 'Time_taken_hour'].iloc[0]
            except Exception as e:
                if i['order_status'].iloc[0] == "commercial" :
                    _ , curr_travel_time = open_route_comm_distance_cal(depot, str(i['Destinations'].iloc[0]), self.region) 
                else :               
                    _ , curr_travel_time = open_route_distance_cal(depot, str(i['Destinations'].iloc[0]), self.region)
                first_location = curr_travel_time 

            if len(i) == 1:
                eta = first_location
            else:
                if len(i) == 2:
                    try :
                        second_location = first_location + 0.5 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == str(i['Destinations'].iloc[0])) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[1])), 'Time_taken_hour'].iloc[0]
                    except Exception as e:
                        if i['order_status'].iloc[0] == "commercial" :
                            _ , curr_travel_time = open_route_comm_distance_cal(str(i['Destinations'].iloc[0]), str(i['Destinations'].iloc[1]), self.region) 
                        else :               
                            _ , curr_travel_time = open_route_distance_cal(str(i['Destinations'].iloc[0]), str(i['Destinations'].iloc[1]), self.region)
                        second_location = curr_travel_time + first_location + 0.5
   
                if len(i) == 3:
                    try :
                        second_location = first_location + 0.5 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == str(i['Destinations'].iloc[0])) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[1])), 'Time_taken_hour'].iloc[0]
                        third_location = second_location + 0.5 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == str(i['Destinations'].iloc[1])) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[2])), 'Time_taken_hour'].iloc[0]
                    except Exception as e:
                        if i['order_status'].iloc[0] == "commercial" :
                            _ , curr_travel_time = open_route_comm_distance_cal(str(i['Destinations'].iloc[1]), str(i['Destinations'].iloc[2]), self.region) 
                        else :               
                            _ , curr_travel_time = open_route_distance_cal(str(i['Destinations'].iloc[1]), str(i['Destinations'].iloc[2]), self.region)                        
                        third_location = curr_travel_time + second_location + 0.5                 

                if  i['no_delivery_end_1'].iloc[0] == i['no_delivery_end_1'].iloc[1] or i['no_delivery_end_1'].iloc[0] == i['Min_Retain_Hour'].iloc[0] or i['no_delivery_start_2'].iloc[0] > i['no_delivery_start_2'].iloc[1]:
                    eta = first_location

                else:
                    if len(i) == 2:
                        eta = second_location
                    elif i['no_delivery_end_1'].iloc[1] == i['no_delivery_end_1'].iloc[2] or i['no_delivery_end_1'].iloc[1] == i['Min_Retain_Hour'].iloc[1] or i['no_delivery_start_2'].iloc[1] > i['no_delivery_start_2'].iloc[2]:
                        eta = second_location
                    else :
                        try :
                            first_location = 1 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == depot) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[0])), 'Time_taken_hour'].iloc[0]
                        except Exception as e:
                            if i['order_status'].iloc[0] == "commercial" :
                                _ , curr_travel_time = open_route_comm_distance_cal(depot, str(i['Destinations'].iloc[0]), self.region) 
                            else :               
                                _ , curr_travel_time = open_route_distance_cal(depot, str(i['Destinations'].iloc[0]), self.region)                            
                            first_location = curr_travel_time + 1     
                        try :               
                            second_location = first_location + 0.5 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == str(i['Destinations'].iloc[0])) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[1])), 'Time_taken_hour'].iloc[0]
                        except Exception as e:
                            if i['order_status'].iloc[0] == "commercial" :
                                _ , curr_travel_time = open_route_comm_distance_cal(str(i['Destinations'].iloc[0]), str(i['Destinations'].iloc[1]), self.region) 
                            else :               
                                _ , curr_travel_time = open_route_distance_cal(str(i['Destinations'].iloc[0]), str(i['Destinations'].iloc[1]), self.region)                            
                            second_location = curr_travel_time + first_location + 0.5 
                        try :               
                            third_location = second_location + 0.5 + self.dist_matrix.loc[(self.dist_matrix['First_location'] == str(i['Destinations'].iloc[0])) & (self.dist_matrix['Next_location'] == str(i['Destinations'].iloc[1])), 'Time_taken_hour'].iloc[0]
                        except Exception as e:
                            if i['order_status'].iloc[0] == "commercial" :
                                _ , curr_travel_time = open_route_comm_distance_cal(str(i['Destinations'].iloc[1]), str(i['Destinations'].iloc[2]), self.region) 
                            else :               
                                _ , curr_travel_time = open_route_distance_cal(str(i['Destinations'].iloc[1]), str(i['Destinations'].iloc[2]), self.region)   
                            third_location = curr_travel_time + second_location + 0.5
                
                    if  i['no_delivery_end_1'].iloc[0] == i['no_delivery_end_1'].iloc[1] or i['no_delivery_end_1'].iloc[0] == i['Min_Retain_Hour'].iloc[0] or i['no_delivery_start_2'].iloc[0] > i['no_delivery_start_2'].iloc[1]:
                        eta = first_location
                    else:
                        if len(i) == 2:
                            eta = second_location
                        else :
                            if  i['no_delivery_end_1'].iloc[1] == i['no_delivery_end_1'].iloc[2] or i['no_delivery_end_1'].iloc[1] == i['Min_Retain_Hour'].iloc[1] or i['no_delivery_start_2'].iloc[1] > i['no_delivery_start_2'].iloc[2]:                                
                                eta = second_location
                            else :
                                eta = third_location      

            return round(eta, 2)


        def trip_travelled_open(model):
            """
            Return the value of start time of each trip for each road tanker taken in previous loop
            """
            return ((k,v,i) for k in model.K for v in model.V for i in range(len(self.start_trips[k])))


        def trip_travelled_close(model):
            """
            Return the value of end time of each trip for each road tanker taken in previous loop
            """            
            return ((k,v,i) for k in model.K for v in model.V for i in range(len(self.end_trips[k])))


        def value_start(model, k,v,i):
            """
            Return the values of start times of each slot for each road tanker.
            """
            return self.start_trips[k][i]


        def same_terminal(model, k, r):
            """
            To generate binary values, 1 if the route and tanker used are of same terminal, othersie 0.
            k: vehicle number of road tanker
            r: route id
            """            
            if self.dict_route.get(r)[8] in list(self.truck_data.loc[(self.truck_data['Vehicle Number'] == k)]['default_terminal']):
                return 1
            else:
                return 0

        
        print("[ModelBuilding] Defining model indices and sets initiated...")
        
        try:
            self.multiload_stations = list(self.order_data[(self.order_data['Multiload_Status']=='Multiload') & ("LD" not in self.order_data['grouping'])]['Destinations'].unique())
        except KeyError as k:
            pass

        try:
            self.smp_stations = list(self.order_data[self.order_data['order_status']=='smp']['Destinations'].unique())
        except KeyError as k:
            pass

        try:
            self.commercial_stations = list(self.order_data[self.order_data['order_status']=='commercial']['Destinations'].unique())
        except KeyError as k:
            pass        

        # self.commercial_orders = list(self.order_data[self.order_data['Order_Type']=='CO']['Destinations'].unique())
        self.commercial_route = list(self.order_data[self.order_data['Order_Type']=='CO']['Route_ID'].unique())
        self.kerosene_route = list(self.order_data[self.order_data['Product']==70000018]['Route_ID'].unique())
        
        # TODO TESTING 
        if self.region == 'NORTHERN':
            self.highland_route = list(self.order_data[self.order_data['Cloud']==29]['Route_ID'].unique()) # Cameroon Highland stations in Cloud 29

        # self.threshold_hour = 22
        # self.model.threshold_hour = self.threshold_hour
        self.model.order_data = self.order_data
        self.capacity_dict = {m:v.capacity for m,v in self.resource_dict.items()}
        self.terminal_capacity_dict = {m:v.terminal_capacity for m,v in self.resource_dict.items()}
        self.start_trips = {m:v.trip_start for m,v in self.resource_dict.items()}
        self.end_trips = {m:v.trip_end for m,v in self.resource_dict.items()}
        self.tanker_score = {m:v.score for m,v in self.resource_dict.items()}
        self.day_balance_shift = {m:v[0][1] for m,v in self.truck_shift_balance.items()}
        self.depot_start_time = {m:v.depot_start_time for m,v in self.resource_dict.items()}
        self.depot_close_time = {m:v.depot_close_time for m,v in self.resource_dict.items()}
        
        # Declare the Set Index
        self.model.N = pyo.Set(initialize = list(self.model.order_data['Order_NO'].unique())) # set of stations
        self.model.K = pyo.Set(initialize = self.resource_dict.keys())  # set of vehicle
        self.model.R = pyo.Set(initialize = list(self.model.order_data['Route_ID'].unique())) # set of route
        self.model.C = pyo.Set(initialize = self.terminal_plus_capacity)
        # self.model.R_notld = pyo.Set(initialize = list(self.order_data[self.order_data['long_dist']!='LD']['Route_ID'].unique())) # set of route
        # self.model.R_ld = pyo.Set(initialize = list(self.order_data[self.order_data['long_dist']=='LD']['Route_ID'].unique())) # set of route
        self.model.V = pyo.RangeSet(1,8) # set of trips
        self.model.V_dash = pyo.RangeSet(1,7) # dummy set for V-1
        self.model.KVI_open = pyo.Set(initialize = trip_travelled_open, dimen=3) # sets of opening time of vehicles and trips already taken for station in previous loop
        # self.model.KVI_close = pyo.Set(initialize = trip_travelled_close, dimen=3) # sets of closing time of vehicles and trips already taken for station in previous loop
        print("[ModelBuilding] Defining model indices and sets completed successfully.")
        
        # Declare the Parameter
        print("[ModelBuilding] Defining model parameters initiated...")
        self.model.Q = pyo.Param(self.model.K, initialize = self.capacity_dict) # capacity of tankers
        self.model.QT = pyo.Param(self.model.K, initialize = self.terminal_capacity_dict, within=Any) # terminal-capacity combinations of tankers
        self.model.tr = pyo.Param(self.model.R, initialize = travel_time) # travelling time (including service time)
        self.model.x = pyo.Param(self.model.N, self.model.R, initialize = station_visit, mutable = True) # if x is visited by route r then 1 else 0
        self.model.tier = pyo.Param(self.model.R, initialize = station_tier)
        self.model.pto = pyo.Param(self.model.K, initialize = pto_tanker)
        self.model.diaphragm = pyo.Param(self.model.K, initialize = diaphragm_tanker)

        # TODO TESTING 
        if self.region == 'NORTHERN':
            self.model.highland = pyo.Param(self.model.K, initialize = highland_tanker) # RT's for Cameroon Highland stations only

        self.model.tanker_score = pyo.Param(self.model.K, initialize = self.tanker_score) # give high score to tanker of larger capacity and more numbers
        self.model.shift_balance = pyo.Param(self.model.K, initialize = shift_balance) # get the shift balance of each tanker
        self.model.start_shift = pyo.Param(self.model.K, initialize = start_shift) # get the start_shift of each tanker
        self.model.end_shift = pyo.Param(self.model.K, initialize = end_shift) # get the end_shift of each tanker
        self.model.terminal = pyo.Param(self.model.K, self.model.R, initialize = same_terminal)
        self.model.depot_start = pyo.Param(self.model.K, initialize = self.depot_start_time)
        self.model.depot_close = pyo.Param(self.model.K, initialize = self.depot_close_time)

        self.model.a = pyo.Param(self.model.R, initialize = retain_max) # min retain hour of route
        self.model.b = pyo.Param(self.model.R, initialize = get_retain_end) # min runout for route
        self.model.delta = pyo.Param(self.model.R, initialize = delta, within = NonNegativeReals) # delta parameter to calculate max traveling time for a route so that earliest start time is at least delta time early from min retain
        self.model.delta_return = pyo.Param(self.model.R, initialize = delta_2_destination, within = NonNegativeReals) # delta parameter to calculate max eta from max runout for a route so that start time is earlier
        #self.model.open_win = pyo.Param(self.model.KVI, initialize = value_start) # calculating trip start of vehicle for each trips already took in first loop 
        print("[ModelBuilding] Defining model parameters completed successfully.")
        
        # Define decision variables
        print("[ModelBuilding] Defining model decision variables initiated...")
        self.model.y = pyo.Var(self.model.R, self.model.K, self.model.V, domain = Binary) # variable y to identify if route r is assigned to vehicle k in trip v then 1 else 0
        # def _bounds_rule(m, k, v):
        #     return (self.depot_start_time[k], self.depot_close_time[k])
        # if self.loop_status.split('_')[1] == "First" :
        #     self.model.d = pyo.Var(self.model.K, self.model.V, bounds = _bounds_rule, domain = NonNegativeReals) # start time trip v for vehicle k, bounded by tanker's terminal start & close time
        # else :
        self.model.d = pyo.Var(self.model.K, self.model.V, domain = NonNegativeReals) # start time trip v for vehicle k
        self.model.open_var = pyo.Var(self.model.KVI_open, domain = Binary)  # indicator variable to find opening time of vehicle k travelled which trip v at which slot i 
        # self.model.close_var = pyo.Var(self.model.KVI_close, domain = Binary) # indicator variable to find closing time of vehicle k travelled which trip v at which slot i        
        self.model.epsilon = pyo.Var(self.model.K, domain = NonNegativeReals)  # deviation of utilization of vehicle k from avg
        self.model.u = pyo.Var(self.model.K, domain = NonNegativeReals)  # shift hour utilization of vehicle k
        self.model.count = pyo.Var(self.model.K, domain = Binary)  # indicator variable to find vehicle k travelled which trip v at which slot i
        # self.model.shift_diff_min = pyo.Var(domain = NonNegativeReals) # max shift hour of vehicle
        # self.model.shift_diff_max = pyo.Var(domain = NonNegativeReals) # min shift hour of vehicle
        self.model.shift_diff_min = pyo.Var(self.model.C, domain = NonNegativeReals)
        self.model.shift_diff_max = pyo.Var(self.model.C, domain = NonNegativeReals)
        self.model.shift_slack = pyo.Var(self.model.C, domain = NonNegativeReals)
        # self.model.multiload_slack = pyo.Var(self.model.R, domain = Binary)        
        print("[ModelBuilding] Defining model decision variables completed successfully.")

        # Define objective function
        print("[ModelBuilding] Defining model objective function initiated...")
        if self.loop_status.split('_')[1] == "First" and (self.rts_strategy == "multiload_all_capacity_first" or self.rts_strategy == "all_capacity_first"):
            if self.opo2_first :
                if self.workload_obj :
                    self.model.objective = pyo.Objective(rule = self.objective_function_0, sense = pyo.maximize)
                else : 
                    self.model.objective = pyo.Objective(rule = self.objective_function_0_2, sense = pyo.maximize)
            else :
                if self.workload_obj :
                    self.model.objective = pyo.Objective(rule = self.objective_function_0, sense = pyo.maximize)
                else : 
                    self.model.objective = pyo.Objective(rule = self.objective_function_0_2, sense = pyo.maximize)
        else :
            self.model.objective = pyo.Objective(rule = self.objective_function_0_1, sense = pyo.maximize)
        print("[ModelBuilding] Defining model objective function completed successfully.")

        print("[ModelBuilding] Defining model constraint function initiated...")
        
        self.__visit_restriction()
        print("[ModelBuilding] Visit restriction for normal orders constraint is completed...")

        self.__visit_route_once()
        print("[ModelBuilding] Visit restriction for route is completed...")

        if self.region == 'NORTHERN':
            self.__accessibility_pto_diaphragm_highland_tanker_terminal()
            print("[ModelBuilding] PTO tanker for commercial orders constraint is completed...")
            print("[ModelBuilding] Diaphragm tanker for orders with kerosene product constraint is completed...")
            print("[ModelBuilding] RT accessibility constraint is completed...")
            print("[ModelBuilding] Highland tanker for Cameron Highland orders constraint is completed...")
            print("[ModelBuilding] Same terminal constraint is completed...")
        else:
            self.__accessibility_pto_diaphragm_terminal()
            print("[ModelBuilding] PTO tanker for commercial orders constraint is completed...")
            print("[ModelBuilding] Diaphragm tanker for orders with kerosene product constraint is completed...")
            print("[ModelBuilding] RT accessibility constraint is completed...")
            print("[ModelBuilding] Same terminal constraint is completed...")


        self.__multiload_morning_evening()
        print("[ModelBuilding] Multiload constraint is completed...")

        self.__smp_asr_visit()
        print("[ModelBuilding] SMP ASR visit constraint is completed...")    

        self.__commercial_visit()
        print("[ModelBuilding] Commercial visit constraint is completed...") 

        self.__max_capacity_routes()
        print("[ModelBuilding] Equal tanker capacity constraint is completed...")

        self.__departure_trip_sequence_combine()
        print("[ModelBuilding] Trip sequence constraint is completed...")
        print("[ModelBuilding] Departure time sequence constraint is completed...")

        self.__time_window()
        print("[ModelBuilding] Time window constraint is completed...")

        # self.__slot_second_loop()
        # print("[ModelBuilding] Slot for second loop constraint is completed...")

        self.__free_slot()
        print("[ModelBuilding] Free slot constraint is completed...")

        self.__shift_utilization()
        print("[ModelBuilding] Shift utilization constraint is completed...")

        if self.loop_status.split('_')[1] == "First":

            self.__shift_max()
            print("[ModelBuilding] Shift max constraint is completed...")

            self.__shift_min()
            print("[ModelBuilding] Shift min constraint is completed...")

            self.__shift_balance()
            print("[ModelBuilding] Shift balance constraint is completed...")

        print("[ModelBuilding] Defining all model constraints functions are completed successfully.")        


    @staticmethod    
    def objective_function_0(model):
        ''' 
        Objective Function for using all capacity tanker in 1st interation loops :
        Max: No of orders scheduled i.e. trips assigned to tankers for all stations
        '''
        # obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K) 
        # obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.u[k]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K) 
        # obj1 = pyo.quicksum(model.count[k] for k in model.K)
        # obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.tanker_score[k]*model.tr[r]*model.y[r,k,v] for v in model.V for i in model.N for r in model.R for k in model.K)
        # obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.tanker_score[k]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K) - pyo.quicksum(model.count[k]*(model.threshold_hour - model.u[k]) for k in model.K)
        # obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.tanker_score[k]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K) 
        obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.tanker_score[k]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K)         
        # obj1 = 1000*(model.shift_diff_max - model.shift_diff_min)
        # obj1 = 1000*(pyo.quicksum(model.shift_diff_max[c] for c in model.C) - pyo.quicksum(model.shift_diff_min[c] for c in model.C))
        obj1 = 1000*(pyo.quicksum(model.shift_slack[c] for c in model.C))
        # obj2 = pyo.quicksum(model.count[k]*(model.shift_balance[k] - model.u[k]) for k in model.K)
        return obj0 - obj1
        # return obj0


    @staticmethod    
    def objective_function_0_1(model):
        ''' 
        Objective Function for using all capacity tanker in 1st interation loops :
        Max: No of orders scheduled i.e. trips assigned to tankers for all stations
        '''
        obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K)         
        # obj0 = pyo.quicksum(model.x[i,r]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K)         
        return obj0

    @staticmethod    
    def objective_function_0_2(model):
        ''' 
        Objective Function for using all capacity tanker in 1st interation loops :
        Max: No of orders scheduled i.e. trips assigned to tankers for all stations
        '''
        obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.tanker_score[k]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K)         
        return obj0


    # @staticmethod    
    # def objective_function_1(model):
    #     ''' 
    #     Objective Function for using sequential capacity tanker in 1st interation loops :
    #     Max: No of orders scheduled i.e. trips assigned to tankers for all stations
    #     '''
    #     obj0 = pyo.quicksum(model.x[i,r]*model.tier[r]*model.y[r,k,v] for v in model.V for i in model.N for r in model.dict_station_visit.get(i) for k in model.K) - pyo.quicksum(model.count[k]*(model.threshold_hour - model.u[k]) for k in model.K)
    #     obj1 = model.shift_diff_max - model.shift_diff_min
    #     obj2 = pyo.quicksum(model.count[k]*(model.shift_balance[k] - model.u[k]) for k in model.K)
    #     return obj0 - obj1 - obj2      


    @property
    def optimization_model(self):
        return self.model


    def __visit_restriction(self):
        """
        Restricting each of orders count of that particular station to be delivered at most once (1) in any route, r by any road tanker, k
        during any trip, v.
        """
        self.model.visit_restriction = pyo.ConstraintList()
        for i in self.model.N:
            # try:
            #     if i not in (self.multiload_stations + self.commercial_orders):
            self.model.visit_restriction.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in self.model.dict_station_visit.get(i) for v in self.model.V) <=1)
            # except AttributeError:
            #     if i not in self.commercial_orders:
            #         self.model.visit_restriction.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in self.model.R for v in self.model.V) <=1)


    def __visit_route_once(self):
        """
        Restricting each of the routes to be served at most once (1) by any road tanker, k during any trip, v.
        """        
        self.model.visit_route_once = pyo.ConstraintList()
        for r in self.model.R:
            self.model.visit_route_once.add(pyo.quicksum(self.model.y[r,k,v] for k in self.model.K for v in self.model.V) <= 1)

    
    def __accessibility_pto_diaphragm_terminal(self):
        '''
        assign commercial route to tanker with PTO only
        '''
        self.model.pto_commercial = pyo.ConstraintList()
        self.model.diaphragm_commercial = pyo.ConstraintList()
        self.model.accessibility_routes = pyo.ConstraintList()
        self.model.same_terminal = pyo.ConstraintList()
        for k in self.model.K:
            for r in self.model.R:
                for v in self.model.V:
                    if r in self.commercial_route :
                        self.model.pto_commercial.add(self.model.pto[k] * self.model.y[r,k,v] == self.model.y[r,k,v])
                    if r in self.kerosene_route :
                        self.model.diaphragm_commercial.add(self.model.diaphragm[k] * self.model.y[r,k,v] == self.model.y[r,k,v])
                    self.model.accessibility_routes.add(self.model.Q[k] * self.model.y[r,k,v] <= self.dict_route.get(r)[1] * self.model.y[r,k,v])
                    self.model.same_terminal.add(self.model.terminal[k,r] * self.model.y[r,k,v] == self.model.y[r,k,v])


    def __accessibility_pto_diaphragm_highland_tanker_terminal(self):
        '''
        assign commercial route to tanker with PTO only (Northern only)
        '''
        self.model.pto_commercial = pyo.ConstraintList()
        self.model.diaphragm_commercial = pyo.ConstraintList()
        self.model.highland_tanker = pyo.ConstraintList()
        self.model.accessibility_routes = pyo.ConstraintList()
        self.model.same_terminal = pyo.ConstraintList()
        for k in self.model.K:
            for r in self.model.R:
                for v in self.model.V:
                    if r in self.commercial_route :
                        self.model.pto_commercial.add(self.model.pto[k] * self.model.y[r,k,v] == self.model.y[r,k,v])
                    if r in self.kerosene_route :
                        self.model.diaphragm_commercial.add(self.model.diaphragm[k] * self.model.y[r,k,v] == self.model.y[r,k,v])
                    if r in self.highland_route :
                        self.model.highland_tanker.add(self.model.highland[k]*self.model.y[r,k,v] == self.model.y[r,k,v])
                    self.model.accessibility_routes.add(self.model.Q[k] * self.model.y[r,k,v] <= self.dict_route.get(r)[1] * self.model.y[r,k,v])
                    self.model.same_terminal.add(self.model.terminal[k,r] * self.model.y[r,k,v] == self.model.y[r,k,v])
    


    # def __pto_commercial(self):
    #     '''
    #     assign commercial route to tanker with PTO only
    #     '''
    #     self.model.pto_commercial = pyo.ConstraintList()
    #     for k in self.model.K:
    #         for r in self.commercial_route:
    #             for v in self.model.V:
    #                 self.model.pto_commercial.add(self.model.pto[k]*self.model.y[r,k,v] == self.model.y[r,k,v])


    # def __diaphragm_commercial(self):
    #     '''
    #     assign route with kerosene product to tanker with Diaphragm
    #     '''
    #     self.model.diaphragm_commercial = pyo.ConstraintList()
    #     for k in self.model.K:
    #         for r in self.kerosene_route:
    #             for v in self.model.V:
    #                 self.model.diaphragm_commercial.add(self.model.diaphragm[k]*self.model.y[r,k,v] == self.model.y[r,k,v])


    def __smp_asr_visit(self):
        '''
        SMP ASR visit constraints :
        Ensure SMP and ASR orders of same retail station will not be delivered in the same time windows
        '''
        if len(self.smp_stations) > 0 :
            self.model.smp_asr_visit = pyo.ConstraintList()
            for i in self.smp_stations:
                smp_id = []
                asr_id = []
                if len(self.order_data[(self.order_data['Destinations']==i)]['Route_ID'].unique())>0:
                    try :
                        smp_id = self.order_data[(self.order_data['Destinations']==i)&(self.order_data['order_status'] == "smp")]['Route_ID'].unique()
                        asr_id = self.order_data[(self.order_data['Destinations']==i)&(self.order_data['order_status'] != "smp")]['Route_ID'].unique()
                    except :
                        try :
                            smp_id = self.order_data[(self.order_data['Destinations']==i)&(self.order_data['order_status'] == "smp")]['Route_ID'].unique()
                        except :
                            try :
                                asr_id = self.order_data[(self.order_data['Destinations']==i)&(self.order_data['order_status'] != "smp")]['Route_ID'].unique()
                            except :
                                pass

                    if len(smp_id) > 0 and len(asr_id) > 0 :
                        exp_1 = pyo.quicksum(self.model.y[r,k,v]*(self.model.tr[r] + self.model.d[k,v]) for k in self.model.K for r in smp_id for v in self.model.V) 
                        self.model.smp_asr_visit.add(exp_1 <= pyo.quicksum(10000(1-self.model.y[r,k,v]) + self.model.y[r,k,v]*self.model.d[k,v] for k in self.model.K for r in asr_id for v in self.model.V))
                        exp_2 = pyo.quicksum(10000(1-self.model.y[r,k,v]) + self.model.y[r,k,v]*self.model.d[k,v] for k in self.model.K for r in smp_id for v in self.model.V) 
                        self.model.smp_asr_visit.add(exp_2 >= pyo.quicksum(self.model.y[r,k,v]*(self.model.tr[r] + self.model.d[k,v]) for k in self.model.K for r in asr_id for v in self.model.V))


    def __commercial_visit(self):
        '''
        Commercial visit constraints :
        Ensure commercial orders of same commercial station will not be delivered in the same time windows
        '''
        if len(self.commercial_stations) > 0 :
            self.model.commercial_visit = pyo.ConstraintList()
            for i in self.commercial_stations :
                comm_id = []
                if len(self.order_data[(self.order_data['Destinations']==i)]['Route_ID'].unique())>0:
                    try :
                        comm_id = self.order_data[(self.order_data['Destinations']==i)&(self.order_data['order_status'] == "commercial")]['Route_ID'].unique()
                    except :
                        pass
                    if len(comm_id) > 1 :
                        for id in comm_id :
                            curr_comm_id = []
                            curr_comm_id.append(id)
                            other_comm_id = [other_id for other_id in comm_id if other_id not in curr_comm_id]
                            exp_1 = pyo.quicksum(self.model.y[r,k,v]*(self.model.tr[r] + self.model.d[k,v]) for k in self.model.K for r in curr_comm_id for v in self.model.V) 
                            self.model.commercial_visit.add(exp_1 <= pyo.quicksum(10000(1-self.model.y[r,k,v]) + self.model.y[r,k,v]*self.model.d[k,v] for k in self.model.K for r in other_comm_id for v in self.model.V))
                            exp_2 = pyo.quicksum(10000(1-self.model.y[r,k,v]) + self.model.y[r,k,v]*self.model.d[k,v] for k in self.model.K for r in curr_comm_id for v in self.model.V) 
                            self.model.commercial_visit.add(exp_2 >= pyo.quicksum(self.model.y[r,k,v]*(self.model.tr[r] + self.model.d[k,v]) for k in self.model.K for r in other_comm_id for v in self.model.V))


    def __multiload_morning_evening(self):
        '''
        Multi-load constraints :
        1. Schedule both morning and evening orders of a stations if serving station is multiload
        2. Schedule evening orders iff morning orders are scheduled, otherwise don't schedule evening order.
        3. Schedule only morning order is also feasible. 
        '''
        if len(self.multiload_stations) > 0 :
            self.model.multi_load = pyo.ConstraintList()
            for i in self.multiload_stations:
                morn_id = []
                even_id = []
                morn_od = []
                even_od = []
                if len(self.order_data[(self.order_data['Destinations']==i)]['Route_ID'].unique())>0:
                    
                    if "five_rows_three_prods" in [m[1] for m in self.dict_multiload_route if m[0] == i] :
                        try:
                            morn_id = self.dict_multiload_route[i,'five_rows_three_prods','combined_1']
                            even_id = self.dict_multiload_route[i,'five_rows_three_prods','combined_2']
                            morn_od = self.dict_multiload_order[i,'five_rows_three_prods','combined_1']
                            even_od = self.dict_multiload_order[i,'five_rows_three_prods','combined_2']
                        except :
                            try:
                                morn_id = self.dict_multiload_route[i,'five_rows_three_prods','combined_1']
                                morn_od = self.dict_multiload_order[i,'five_rows_three_prods','combined_1']
                            except :
                                even_id = self.dict_multiload_route[i,'five_rows_three_prods','combined_2']
                                even_od = self.dict_multiload_order[i,'five_rows_three_prods','combined_2']
                    
                    elif "four_rows_three_prods" in [m[1] for m in self.dict_multiload_route if m[0] == i] :
                        try:
                            morn_id = self.dict_multiload_route[i,'four_rows_three_prods','combined_1']
                            even_id = self.dict_multiload_route[i,'four_rows_three_prods','combined_2']
                            morn_od = self.dict_multiload_order[i,'four_rows_three_prods','combined_1']
                            even_od = self.dict_multiload_order[i,'four_rows_three_prods','combined_2']                        
                        except :
                            try:
                                morn_id = self.dict_multiload_route[i,'four_rows_three_prods','combined_early']
                                even_id = self.dict_multiload_route[i,'four_rows_three_prods','combined_late']
                                morn_od = self.dict_multiload_order[i,'four_rows_three_prods','combined_early']
                                even_od = self.dict_multiload_order[i,'four_rows_three_prods','combined_late']
                            except :
                                try:
                                    morn_id = self.dict_multiload_route[i,'four_rows_three_prods','combined_early']
                                    even_id = self.dict_multiload_route[i,'four_rows_three_prods','single_late']   
                                    morn_od = self.dict_multiload_order[i,'four_rows_three_prods','combined_early']
                                    even_od = self.dict_multiload_order[i,'four_rows_three_prods','single_late']                                                              
                                except :
                                    try:
                                        morn_id = self.dict_multiload_route[i,'four_rows_three_prods','single_early']
                                        even_id = self.dict_multiload_route[i,'four_rows_three_prods','combined_late']
                                        morn_od = self.dict_multiload_order[i,'four_rows_three_prods','single_early']
                                        even_od = self.dict_multiload_order[i,'four_rows_three_prods','combined_late']                                     
                                    except :
                                        try:
                                            morn_id = self.dict_multiload_route[i,'four_rows_three_prods','single_multiload_early']
                                            even_id = self.dict_multiload_route[i,'four_rows_three_prods','single_normal_late'] 
                                            morn_od = self.dict_multiload_order[i,'four_rows_three_prods','single_multiload_early']
                                            even_od = self.dict_multiload_order[i,'four_rows_three_prods','single_normal_late']                                         
                                        except :
                                            try:
                                                morn_id = self.dict_multiload_route[i,'four_rows_three_prods','single_normal_early']
                                                even_id = self.dict_multiload_route[i,'four_rows_three_prods','single_multiload_late'] 
                                                morn_od = self.dict_multiload_order[i,'four_rows_three_prods','single_normal_early']
                                                even_od = self.dict_multiload_order[i,'four_rows_three_prods','single_multiload_late']                                             
                                            except:
                                                grouping = [m[2] for m in self.dict_multiload_route if m[0] == i and m[1] == 'four_rows_three_prods'] 
                                                if grouping[0] in ['combined_1','combined_early','single_early','single_multiload_early','single_normal_early'] :
                                                    morn_id = self.dict_multiload_route[i,'four_rows_three_prods', grouping[0]]
                                                    morn_od = self.dict_multiload_order[i,'four_rows_three_prods', grouping[0]]                                                
                                                else :
                                                    even_id = self.dict_multiload_route[i,'four_rows_three_prods', grouping[0]]
                                                    even_od = self.dict_multiload_order[i,'four_rows_three_prods', grouping[0]]

                    elif "four_rows_two_prods" in [m[1] for m in self.dict_multiload_route if m[0] == i] :
                        try:
                            morn_id = self.dict_multiload_route[i,'four_rows_two_prods','combined_1']
                            even_id = self.dict_multiload_route[i,'four_rows_two_prods','combined_2']
                            morn_od = self.dict_multiload_order[i,'four_rows_two_prods','combined_1']
                            even_od = self.dict_multiload_order[i,'four_rows_two_prods','combined_2']                        
                        except :
                            try:
                                morn_id = self.dict_multiload_route[i,'four_rows_two_prods','combined_1']
                                morn_od = self.dict_multiload_order[i,'four_rows_two_prods','combined_1']                            
                            except :
                                even_id = self.dict_multiload_route[i,'four_rows_two_prods','combined_2']
                                even_od = self.dict_multiload_order[i,'four_rows_two_prods','combined_2']

                    elif "three_rows_two_prods" in [m[1] for m in self.dict_multiload_route if m[0] == i] :
                        try:
                            morn_id = self.dict_multiload_route[i,'three_rows_two_prods','combined']
                            even_id = self.dict_multiload_route[i,'three_rows_two_prods','single']
                            morn_od = self.dict_multiload_order[i,'three_rows_two_prods','combined']
                            even_od = self.dict_multiload_order[i,'three_rows_two_prods','single']                        
                        except :
                            try:
                                morn_id = self.dict_multiload_route[i,'three_rows_two_prods','combined_early']
                                even_id = self.dict_multiload_route[i,'three_rows_two_prods','single_late']
                                morn_od = self.dict_multiload_order[i,'three_rows_two_prods','combined_early']
                                even_od = self.dict_multiload_order[i,'three_rows_two_prods','single_late']                            
                            except :
                                try:
                                    morn_id = self.dict_multiload_route[i,'three_rows_two_prods','single_early']
                                    even_id = self.dict_multiload_route[i,'three_rows_two_prods','combined_late']
                                    morn_od = self.dict_multiload_order[i,'three_rows_two_prods','single_early']
                                    even_od = self.dict_multiload_order[i,'three_rows_two_prods','combined_late']                                
                                except :
                                    try:
                                        morn_id = self.dict_multiload_route[i,'three_rows_two_prods','single_1']
                                        even_id = self.dict_multiload_route[i,'three_rows_two_prods','single_2']
                                        morn_od = self.dict_multiload_order[i,'three_rows_two_prods','single_1']
                                        even_od = self.dict_multiload_order[i,'three_rows_two_prods','single_2']                                    
                                    except :
                                        try:
                                            morn_id = self.dict_multiload_route[i,'three_rows_two_prods','single_multiload_early']
                                            even_id = self.dict_multiload_route[i,'three_rows_two_prods','single_normal_late']
                                            morn_od = self.dict_multiload_order[i,'three_rows_two_prods','single_multiload_early']
                                            even_od = self.dict_multiload_order[i,'three_rows_two_prods','single_normal_late']                                        
                                        except :
                                            try:
                                                morn_id = self.dict_multiload_route[i,'three_rows_two_prods','single_normal_early']
                                                even_id = self.dict_multiload_route[i,'three_rows_two_prods','single_multiload_late']
                                                morn_od = self.dict_multiload_order[i,'three_rows_two_prods','single_normal_early']
                                                even_od = self.dict_multiload_order[i,'three_rows_two_prods','single_multiload_late']                                            
                                            except :
                                                grouping = [m[2] for m in self.dict_multiload_route if m[0] == i and m[1] == 'three_rows_two_prods']
                                                if grouping[0] in ['combined','combined_early','single_early','single_1','single_multiload_early','single_normal_early'] :
                                                    morn_id = self.dict_multiload_route[i,'three_rows_two_prods', grouping[0]]
                                                    morn_od = self.dict_multiload_order[i,'three_rows_two_prods', grouping[0]]                                                
                                                else :
                                                    even_id = self.dict_multiload_route[i,'three_rows_two_prods', grouping[0]] 
                                                    even_od = self.dict_multiload_order[i,'three_rows_two_prods', grouping[0]]                                                               
                    else:
                        try:
                            morn_id = self.dict_multiload_route[i,'two_rows_one_prods','single_1']
                            even_id = self.dict_multiload_route[i,'two_rows_one_prods','single_2']
                            morn_od = self.dict_multiload_order[i,'two_rows_one_prods','single_1']
                            even_od = self.dict_multiload_order[i,'two_rows_one_prods','single_2']                        
                        except :
                            try:
                                morn_id = self.dict_multiload_route[i,'two_rows_one_prods','single_1']
                                morn_od = self.dict_multiload_order[i,'two_rows_one_prods','single_1']                            
                            except:
                                try:
                                    even_id = self.dict_multiload_route[i,'two_rows_one_prods','single_2']
                                    even_od = self.dict_multiload_order[i,'two_rows_one_prods','single_2']                                
                                except :
                                    pass
                                
                    if len(morn_id) > 0 and len(even_id) > 0 :
                        self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in morn_id for i in morn_od for  v in self.model.V) <=1)
                        self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in even_id for i in even_od for v in self.model.V) <=1)
                        if self.loop_status.split('_')[1] == "First":
                            self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in even_id for i in even_od for v in self.model.V) <= pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in morn_id for i in morn_od for v in self.model.V))
                            exp = pyo.quicksum(self.model.y[r,k,v]*(self.model.tr[r] + self.model.d[k,v]) for k in self.model.K for r in morn_id for v in self.model.V) 
                            self.model.multi_load.add(exp <= pyo.quicksum(self.model.y[r,k,v]*self.model.d[k,v] for k in self.model.K for r in even_id for v in self.model.V))
                        else :    
                            exp = pyo.quicksum(self.model.y[r,k,v]*(self.model.tr[r] + self.model.d[k,v]) for k in self.model.K for r in morn_id for v in self.model.V) 
                            self.model.multi_load.add(exp <= pyo.quicksum(10000(1-self.model.y[r,k,v]) + self.model.y[r,k,v]*self.model.d[k,v] for k in self.model.K for r in even_id for v in self.model.V))
                        # self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in self.model.dict_station_visit.get(i) for v in self.model.V) <=2)
                    elif len(morn_id) > 0 :
                        self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in morn_id for i in morn_od for v in self.model.V) <=1)
                        # self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in self.model.dict_station_visit.get(i) for v in self.model.V) <=1)
                    elif len(even_id) > 0 :
                        self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in even_id for i in even_od for v in self.model.V) <=1)
                        # self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in self.model.dict_station_visit.get(i) for v in self.model.V) <=1)                  
                    else :
                        pass
                        # self.model.multi_load.add(pyo.quicksum(self.model.x[i,r]*self.model.y[r,k,v] for k in self.model.K for r in self.model.dict_station_visit.get(i) for v in self.model.V) <=0)


    def __max_capacity_routes(self):
        """
        Assigning road tanker, k with capacity that is equal to the total quantity carried by route, r.
        """
        setattr(self.model, "max_capacity_routes", pyo.Constraint(self.model.R, self.model.K, self.model.V, noruleinit=True))
        c = getattr(self.model, "max_capacity_routes")

        for i in c._index:
            r = i[0]
            k = i[1]
            v = i[2]
            c._data[i] = _GeneralConstraintData(None, c)
            expr = LinearExpression()
            expr.linear_vars = [self.model.y[i], self.model.y[i]]
            expr.linear_coefs = [self.model.Q[k], -int(self.dict_route.get(r)[0])]
            expr.constant = 0
            c._data[i]._body = expr
            c._data[i]._equality = True
            c._data[i]._lower = NumericConstant(0.0)
            c._data[i]._upper = NumericConstant(0.0)

        # with open('file.txt', 'w') as output_file:
        #      self.model.max_capacity_routes.pprint(output_file)


    # def __accessibility_routes(self):
    #     '''
    #     assign route to tanker only when order size equal to tanker capacity for any trip
    #     '''
    #     self.model.accessibility_routes = pyo.ConstraintList()
    #     for k in self.model.K:
    #         for r in self.model.R:
    #             for v in self.model.V:
    #                 self.model.accessibility_routes.add(self.model.Q[k]*self.model.y[r,k,v] <= self.order_data.loc[(self.order_data['Route_ID'] == r)]['Accessibility'].iloc[0]*self.model.y[r,k,v])


    def __departure_trip_sequence_combine(self):
        '''
        start of next trip should be less than start of previous trip
        start of next trip should be greater than or equal to end of previous trip
        '''
        self.model.trip_sequence = pyo.ConstraintList()
        self.model.departure_trip_sequence = pyo.ConstraintList()
        for k in self.model.K:
            for v in self.model.V_dash:
                exp = pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) + self.model.d[k,v]
                self.model.departure_trip_sequence.add(exp <= self.model.d[k,v+1])
                self.model.trip_sequence.add(pyo.quicksum(self.model.y[r,k,v+1] for r in self.model.R) <= pyo.quicksum(self.model.y[r,k,v] for r in self.model.R))  


    # def __trip_sequence(self):
    #     '''
    #     start of next trip should be less than start of previous trip
    #     '''
    #     self.model.trip_sequence = pyo.ConstraintList()
    #     for k in self.model.K:
    #         for v in self.model.V_dash:
    #                 self.model.trip_sequence.add(pyo.quicksum(self.model.y[r,k,v+1] for r in self.model.R) <= pyo.quicksum(self.model.y[r,k,v] for r in self.model.R))                                      
    #                 # self.model.trip_sequence.add(pyo.quicksum(self.model.y[r,k,4] for r in self.model.R_notld) <= pyo.quicksum(self.model.y[r,k,v] for r in self.model.R_ld))                                      


    def __time_window(self):
        '''
        3: one or no route should be assigned to tanker in any trip
        3: start of any trip can't be after tanker start shift
        3: end of any trip should be before tanker end shift
        5: start of any trip should at least the travel time from min retain i.e. a[r] and before max eta from max runout i.e. b[r]
        7: total trip should end before tanker end shift
        '''
        self.model.const7 =  pyo.ConstraintList()
        self.model.const5 = pyo.ConstraintList()
        # self.model.const3 = pyo.ConstraintList()

        for k in self.model.K:
            for v in self.model.V:
                self.model.const5.add(pyo.quicksum(self.model.y[r,k,v]*(self.model.b[r]-self.model.delta_return[r]) for r in self.model.R) + 10000*(1-pyo.quicksum(self.model.y[r,k,v] for r in self.model.R)) >= self.model.d[k,v])
                self.model.const5.add(pyo.quicksum(self.model.y[r,k,v]*(self.model.a[r]-self.model.delta[r]) for r in self.model.R) <= self.model.d[k,v])
                self.model.const7.add(self.model.d[k,v]-self.model.d[k,1] + pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) <= self.model.end_shift[k])
                # self.model.const3.add(pyo.quicksum(self.model.y[r,k,v] for r in self.model.R)<=1)
                # self.model.const3.add(self.model.d[k,v] + pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) <= self.model.end_shift[k]) # change model.R to route that is not in LD
                # self.model.const3.add(self.model.d[k,v] <= self.model.start_shift[k])


    # def __time_window_long_dist(self):
    #     self.model.time_window_ld = pyo.ConstraintList()
    #     for k in self.model.K:
    #         for v in self.model.V:
    #             self.model.time_window_ld.add(pyo.quicksum(self.model.y[r,k,v]*(self.model.b[r]-self.model.tr[r])  for r in self.model.R_ld) + 10000*(1-pyo.quicksum(self.model.y[r,k,v] for r in self.model.R_ld)) >= self.model.d[k,v])
    #             self.model.time_window_ld.add(pyo.quicksum(self.model.y[r,k,v]*self.model.a[r] for r in self.model.R_ld) <= self.model.d[k,v])
    

    # def __departure_trip_sequence(self):
    #     '''
    #     start of next trip should be greater than or equal to end of previous trip
    #     '''
    #     self.model.departure_trip_sequence = pyo.ConstraintList()
    #     for k in self.model.K:
    #         for v in self.model.V_dash:
    #             exp = pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) + self.model.d[k,v]
    #             self.model.departure_trip_sequence.add(exp <= self.model.d[k,v+1])
           
    # def __slot_second_loop(self):
    #     '''
    #     time window constraint for second loop i.e. no trip can start and end in any slot which already taken in first loop
    #     '''
    #     self.model.slot_second_loop = pyo.ConstraintList()
    #     for k in self.model.K:
    #         for v in self.model.V:
    #             rhs = pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) + self.model.d[k,v]
    #             for i,open_win in zip(range(len(self.start_trips[k])),self.start_trips[k]) :
    #                 if self.model.shift_balance[k] != self.day_balance_shift[k] :
    #                     self.model.slot_second_loop.add(open_win*(1-self.model.open_var[k,v,i]) >= self.model.d[k,v])
    #                     self.model.slot_second_loop.add(open_win*(1-self.model.open_var[k,v,i]) >= rhs)
    #             self.model.slot_second_loop.add(pyo.quicksum(self.model.open_var[k,v,i] for i in range(len(self.start_trips[k]))) <= 1)
    #             for i,close_win in zip(range(len(self.end_trips[k])),self.end_trips[k]) :
    #                 if self.model.shift_balance[k] != self.day_balance_shift[k] :
    #                     self.model.slot_second_loop.add(close_win*(self.model.close_var[k,v,i]-1) <= self.model.d[k,v])
    #                     self.model.slot_second_loop.add(close_win*(self.model.close_var[k,v,i]-1) <= rhs)     
    #             self.model.slot_second_loop.add(pyo.quicksum(self.model.close_var[k,v,i] for i in range(len(self.end_trips[k]))) <= 1)     

    def __free_slot(self):
        '''
        time window constraint for free_slot i.e. trip can only start and end in any free slot available
        '''
        self.model.free_slot = pyo.ConstraintList()
        self.model.shift_windows = pyo.ConstraintList()
        for k in self.model.K:
            for v in self.model.V:
                if self.model.shift_balance[k] != self.day_balance_shift[k] :
                    lhs = pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) + self.model.d[k,v]
                    for i,open_win in zip(range(len(self.start_trips[k])),self.start_trips[k]) :
                        self.model.free_slot.add(self.model.d[k,v] >= open_win*(self.model.open_var[k,v,i]))
                    for i,close_win in zip(range(len(self.end_trips[k])),self.end_trips[k]) :
                        self.model.free_slot.add(lhs <= close_win*(self.model.open_var[k,v,i]))                        
                    self.model.free_slot.add(pyo.quicksum(self.model.open_var[k,v,i] for i in range(len(self.start_trips[k]))) <= 1)
                else :                      
                    self.model.shift_windows.add(pyo.quicksum(self.model.y[r,k,v] for r in self.model.R)<=1)
                    self.model.shift_windows.add(self.model.d[k,v] + pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) <= self.model.end_shift[k])
                    self.model.shift_windows.add(self.model.d[k,v] + pyo.quicksum(self.model.y[r,k,v]*self.model.tr[r] for r in self.model.R) <= self.model.depot_close[k]) 
                    self.model.shift_windows.add(self.model.d[k,v] >= self.model.start_shift[k])
                    self.model.shift_windows.add(self.model.d[k,v] >= self.model.depot_start[k])


    def __shift_utilization(self):
        '''
        shift_utilization : calculating deviation from thresold set by user and capturing shift utilized
        '''
        self.model.shift_utilization = pyo.ConstraintList()
        for k in self.model.K:
            self.model.shift_utilization.add(self.model.u[k] == pyo.quicksum(self.model.y[r,k,v] * self.model.tr[r] for r in self.model.R for v in self.model.V))
            self.model.shift_utilization.add(self.model.epsilon[k] <= self.model.shift_balance[k] - self.model.u[k])
            self.model.shift_utilization.add(self.model.epsilon[k] >= self.model.shift_balance[k] - self.model.u[k])
            self.model.shift_utilization.add(self.model.u[k] <= 1000*self.model.count[k])


    # def __shift_max(self):
    #     '''
    #     obtain max shift utilization of tankers
    #     '''
    #     self.model.shift_max = pyo.ConstraintList()
    #     for k in self.model.K :
    #         self.model.shift_max.add(self.model.shift_diff_max >= self.model.shift_balance[k] - self.model.u[k])
    

    # def __shift_min(self):
    #     '''
    #     obtain min shift utilization of tankers
    #     '''
    #     self.model.shift_min = pyo.ConstraintList()
    #     for k in self.model.K :
    #         self.model.shift_min.add(self.model.shift_diff_min <= self.model.shift_balance[k] - self.model.u[k])


    def __shift_max(self):
        '''
        obtain max shift utilization of tankers
        '''
        self.model.shift_max = pyo.ConstraintList()
        for k in self.model.K :
            self.model.shift_max.add(self.model.shift_diff_max[str(self.model.QT[k])] >= self.model.shift_balance[k] - self.model.u[k])
    

    def __shift_min(self):
        '''
        obtain min shift utilization of tankers
        '''
        self.model.shift_min = pyo.ConstraintList()
        for k in self.model.K :
            self.model.shift_min.add(self.model.shift_diff_min[str(self.model.QT[k])] <= self.model.shift_balance[k] - self.model.u[k])


    def __shift_balance(self):
        '''
        shift balance target with slack
        '''
        self.model.workload_balance = pyo.ConstraintList()
        for c in self.model.C :
            self.model.workload_balance.add((self.model.shift_diff_max[c] - self.model.shift_diff_min[c]) - self.model.shift_slack[c] <= 12)    