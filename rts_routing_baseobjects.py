#from input_generation import order_data, truck_data, station_data, truck_weight, product_density, dist_matrix
import sys
import os
from src.input_gen_modules.rts_input_handler import InputHandler
from src.rts_routing_modules.rts_routing_utilities import *
import pandas as pd
import numpy as np
import random
import string
import sys
from copy import deepcopy
import warnings
import datetime 
from multiprocessing import Process, Manager
import requests
import os

from src.utility.db_connection_helper import get_db_connection

warnings.filterwarnings("ignore")
from sqlalchemy import create_engine
from conf import Config, Logger

from database_connector import DatabaseConnector, YAMLFileConnector

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

class Order(object):
    def __init__(self, id, quantity, priority, product_type, destination, cloud, slot, runout, closing_time, accessibility, distance, weight, end_stock_day, tank_status, multiload_status, condition, grouping, order_status, combine_tag, terminal_97, terminal_95, long_distance, **kwargs):
        """
        Args:
            id: unique id for order
            quantity: quantity of order
            product_type: product i.e. 95, 97, diesel
            destination: ship -to id, station id
            cloud: station belong to which cloud
            time: 1st load, afternoon, last load, for central region reading
            from dmr
            slot:  retain window
            accessibility: accessibility column for truck
            distance: distance of station from terminal
        """
        self.unique_id = id
        self.quantity = quantity
        self.priority = priority
        self.product_type = product_type
        self.destination = destination
        self.process = True    
        self.quantity_left = 0
        self.cloud = int(cloud)
        self.slot = slot    #retain
        self.runout = runout
        self.closing_time = closing_time
        self.accessibility = accessibility
        self.distance = distance
        self.weight = weight
        self.end_stock_day = end_stock_day
        self.tank_status = tank_status
        self.multiload_status = multiload_status
        self.condition = condition
        self.grouping = grouping
        self.order_status = order_status
        self.combine_tag = combine_tag
        self.terminal_97 = terminal_97
        self.terminal_95 = terminal_95
        self.long_distance = long_distance
        if len(kwargs) != 0 :
            values_view = kwargs.values()
            value_iterator = iter(values_view)
            if kwargs.get('ori_status') :
                self.ori_status = next(value_iterator)
            if kwargs.get('identifier') :
                self.identifier = next(value_iterator)

    # getter for order id
    def get_order_id(self):
        return self.id

    # getter for order quantity
    def return_order_quantity(self):
        return self.quantity
    
    def update_process(self):
        if self.process:
            print(self.process)
            self.process = False
            print(self.process)

    # exporting unassigned orders into list if dictionary to convert into csv
    def output(self):
        data = []
        data.append({"Order ID": self.unique_id, "Destinations": self.destination, "Product": self.product_type, "Order Quantity": self.quantity, "Cloud":self.cloud, "Retain":self.slot, "Process":self.process})
        return data

class Resource(object):
    def __init__(self, resource_id, shift_type, resource_capacity, subresource, subresource_quantity, weight_limit, dry_weight, shift_start, shift_end, min_remaining_shift, depot_start_time, date_time=None, **kwargs):
        """
        Args: Resembles with truck
            resource_id: truck name is id here
            resource_capacity: capacity of truck
            subresource: no of compartments in truck. note it
            subresource_quantity: capacity of compartment
            type: Not using now
            fix_weight: RT weight = Kerb Weight (*BTM) of both Prime Mover + Trailer 
        """
        self.id = resource_id
        self.shift_type = shift_type
        self.capacity = resource_capacity # use for adding orders and reduce capacity
        self.tanker_capacity = resource_capacity # fixed tanker capacity to check accessibility
        self.sub_resource = subresource  # compartments
        self.sub_resource_quantity = subresource_quantity  # compartment size
        self.weight_limit = weight_limit
        self.dry_weight = dry_weight
        self.shift_start = shift_start
        self.shift_end = shift_end
        self.min_remaining_shift = min_remaining_shift
        self.depot_start_time = depot_start_time
        self.date_time = date_time
        self.schedule = []
        if len(kwargs) != 0 :
            values_view = kwargs.values()
            value_iterator = iter(values_view)      
            if kwargs.get('depot_close_time') :
                self.depot_close_time = next(value_iterator)
            if kwargs.get('rt_terminal') :
                self.rt_terminal = next(value_iterator)                     
     
    # getter for id
    def get_id(self):
        return self.id

    # getter for resource capacity
    def get_resource_capacity(self):
        return self.capacity

    # getter for compartment number
    def get_compartment_number(self):
        return int(self.capacity/self.sub_resource_quantity)

    # boolean to check truck is available or not
    def is_available(self):
        if self.shift:
            return True

    # to log unused trucks, or leftover trucks
    def output(self):
        data = []
        data.append({"TruckID": self.id, "No. Compartment": self.sub_resource, "Shift": self.shift_hour, "Remain Shift": self.remain_shift_hour})
        return data
        

class Solution(object):
    """list of assigned resources i.e. order loaded truck"""
    def __init__(self, id, solution, loop, unmatched_log, opo2_first):
        """
        Args:
            id: id for solution . unique id generated in code
            solution: list of assigned resource object
        """
        self.id = id
        self.solution = solution
        self.loop = loop
        self.unmatched_log  = unmatched_log 
        self.opo2_first = opo2_first

    def overall_utilization(self):
        sum = 0
        for s in self.solution:
            sum += self.solution[s].utilization() * self.solution[s].sub_resource
        return sum

    def get_neighbor(self, algo, n):
        """
        Args: funtion to get neighbor solution
            algo: name of algorithm used to get neighbor
            n: no of times times you want to swap to get neighbor solution
        """
        i = 0
        while i < n:
            if algo == 'random':
                swap1 = random.choice(list(self.solution.keys()))
                swap2 = random.choice(list(self.solution.keys()))
                while swap1 == swap2:
                    swap2 = random.choice(list(self.solution.keys()))
                order1 = self.solution[swap1].orders
                order2 = self.solution[swap2].orders
                rand1 = random.choice(order1)
                rand2 = random.choice(order2)
                temp1 = deepcopy(rand1)
                temp2 = deepcopy(rand2)
                self.solution[swap1].remove_order(rand1)
                self.solution[swap2].remove_order(rand2)
                if self.solution[swap1].can_fit(temp2) and self.solution[swap2].can_fit(temp1):
                    self.solution[swap1].add_order(rand2)
                    self.solution[swap2].add_order(rand1)
                else:
                    self.solution[swap1].add_order(rand1)
                    self.solution[swap2].add_order(rand2)


            if algo == 'combine':
                select1 = random.choice(list(self.solution.keys()))
                select2 = random.choice(list(self.solution.keys()))
                while select1 == select2:
                    select2 = random.choice(list(self.solution.keys()))
                order1 = self.solution[select1].orders
                order2 = self.solution[select2].orders
                if len(order1) <= 2 and len(order2) <= 1 :
                   if self.solution[select1].can_fit(order2[0]):
                       self.solution[select1].add_order(order2[0])
                       del self.solution[select2]
                elif len(order1) <= 1 and len(order2) <= 2 :
                    if self.solution[select2].can_fit(order1[0]):
                       self.solution[select2].add_order(order1[0])
                       del self.solution[select1]
            i += 1

    def check_feasibility(self):
        for i in self.solution:
            print(i)

    # exporting solution object into list if dictionary to convert into csv
    # later i.e. readable form
    def output(self):
        data = []
        if self.unmatched_log == False :
            if self.loop.split('_')[1] == "First" and self.opo2_first == False : 
                for s in self.solution:
                    for o in self.solution[s].orders:
                        data.append({"Route_ID": self.solution[s].index, "Priority": o.priority, "Terminal_97": o.terminal_97, "Terminal_Default": o.terminal_95, "No_Compartment": self.solution[s].sub_resource, "Tanker_Cap": self.solution[s].tanker_capacity, "Order_ID": o.unique_id,
                                    "Destinations": o.destination, "Product": o.product_type, "Order_Quantity": o.quantity, "Utilization":self.solution[s].filled/(self.solution[s].sub_resource*5460), "Retain_Hour":o.slot, "Runout":o.runout, "End_Stock_Day":o.end_stock_day,
                                    "Travel_Time":self.solution[s].total_travel_time, "Accessibility": o.accessibility, "Tank_Status": o.tank_status, "Multiload_Status" :o.multiload_status, "condition":o.condition, "grouping":o.grouping, "order_status":o.order_status, "combine_tag":o.combine_tag, "long_distance":o.long_distance})
            elif self.loop.split('_')[1] == "Second" or self.opo2_first == True :
                for s in self.solution:
                    for o in self.solution[s].orders:
                        data.append({"Route_ID": self.solution[s].index, "Priority": o.priority, "Terminal_97": o.terminal_97, "Terminal_Default": o.terminal_95, "No_Compartment": self.solution[s].sub_resource, "Tanker_Cap":  self.solution[s].tanker_capacity, "Order_ID": o.unique_id,
                                    "Destinations": o.destination, "Product": o.product_type, "Order_Quantity": o.quantity, "Utilization":self.solution[s].filled/(self.solution[s].sub_resource*5460), "Retain_Hour":o.slot, "Runout":o.runout,"End_Stock_Day":o.end_stock_day,
                                    "Travel_Time":self.solution[s].total_travel_time, "Accessibility": o.accessibility, "Tank_Status": o.tank_status, "Multiload_Status" :o.multiload_status, "condition":o.condition, "grouping":o.grouping, "order_status":o.order_status, "combine_tag":o.combine_tag, "ori_status":o.ori_status, "long_distance":o.long_distance})
            else :
                pass  
            return data

        else :
            if self.loop.split('_')[1] == "First" and self.opo2_first == False : 
                for s in self.solution:
                    for o in self.solution[s].unmatched_orders:
                        data.append({"Route_ID": self.solution[s].index, "Priority": o.priority, "Terminal_97": o.terminal_97, "Terminal_Default": o.terminal_95, "No_Compartment": self.solution[s].sub_resource, "Tanker_Cap":  self.solution[s].tanker_capacity, "Order_ID": o.unique_id,
                                    "Destinations": o.destination, "Product": o.product_type, "Order_Quantity": o.quantity, "Order_Weight": o.weight, "Tanker_DryWeight" : self.solution[s].dry_weight, "Tanker_WeightLimit" : self.solution[s].weight_limit, "Retain_Hour":o.slot, "Runout":o.runout, "End_Stock_Day":o.end_stock_day,
                                    "Accessibility": o.accessibility, "Tank_Status": o.tank_status, "Multiload_Status" :o.multiload_status, "condition":o.condition, "grouping":o.grouping, "order_status":o.order_status, "combine_tag":o.combine_tag, "long_distance":o.long_distance, "violation_log":self.solution[s].message})
            elif self.loop.split('_')[1] == "Second" or self.opo2_first == True :
                for s in self.solution:
                    for o in self.solution[s].unmatched_orders:
                        data.append({"Route_ID": self.solution[s].index, "Priority": o.priority, "Terminal_97": o.terminal_97, "Terminal_Default": o.terminal_95, "No_Compartment": self.solution[s].sub_resource, "Tanker_Cap":  self.solution[s].tanker_capacity, "Order_ID": o.unique_id,
                                    "Destinations": o.destination, "Product": o.product_type, "Order_Quantity": o.quantity, "Order_Weight": o.weight, "Tanker_DryWeight" : self.solution[s].dry_weight, "Tanker_WeightLimit" : self.solution[s].weight_limit, "Retain_Hour":o.slot, "Runout":o.runout,"End_Stock_Day":o.end_stock_day,
                                    "Accessibility": o.accessibility, "Tank_Status": o.tank_status, "Multiload_Status" :o.multiload_status, "condition":o.condition, "grouping":o.grouping, "order_status":o.order_status, "combine_tag":o.combine_tag, "ori_status":o.ori_status, "long_distance":o.long_distance, "violation_log":self.solution[s].message})
            else :
                pass                       
            return data        

    # to calculate cost of solution
    def overall_cost(self):
        return sum([self.solution[s].cost() for s in self.solution])

    # to calculate distance of solution
    def total_distance_travelled(self):
        return sum([self.solution[s].distance_travelled() for s in self.solution])

# Generate Route
class RouteGenerator(Resource):
    def __init__(self, *arg, **kwargs):
        # self._logger = Logger().logger
        self.index = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(7))
        self.orders = []  # list of order object
        self.filled = 0   # how much quantity of truck is filled
        #self.total_time = 0 # how much total travel time + unloading time + 1 hour loading time
        #self.total_distance= 0 # how much total distance travelled from terminal to last destinations
        self.curr_travel_time = 0 # how much current travel time + unloading time + 1 hour loading time (if from terminal)
        self.curr_distance = 0 # how much current travel distnace
        self.last_destination = [] # store last destination
        self.travel_time = 0 # time to substract from current shift
        self.total_travel_time = 0 # total time to substract from resource dict 
        self.destinations = []
        self.previous_retain = [] 
        self.count = 0 # for sequence to add unloading time
        self.unloading_mins = 0 # storing unloading time at each destination for eta
        self.travel_mins = 0 # calculate the plan load time backward
        self.forward_mins = 0 # calculate the eta forward
        self.prod_weight = 0
        self.status = True
        #self.manager = Manager() #set up manager for multiprocess
        #self.parallel_status_list = self.manager.list() #use manager to share data between functions during multiprocess
        self.return_time = 0 # store return time
        self.return_dest = [] # store destination to return to depot
        self.no_drops = 0 # store number of drops
        self.weight = 0
        self.message = None
        self.unmatched_orders = []
        super(RouteGenerator, self).__init__(*arg, **kwargs)
    # decorator to get list of stations visited by this assigned_resource ie
    # truck


    def get_destinations(self, order):
        """
        Args: to get current destination of order to add
              destination_list : contains destinations of order
              self.last_destination : store last destination
        """
        destination_list = [str(order.destination)]
        if self.last_destination == []:
            if order.product_type == 70000011:
                 destination_list.insert(0, str(order.terminal_97))
            else :
                 destination_list.insert(0, str(order.terminal_95))
        else :
            destination_list.insert(0, self.last_destination)   
        self.last_destination = destination_list[-1] #store last destination
        return destination_list    


    # def open_route_distance_cal(self, start_point, end_point, region):
    #     """[calculate distance and time taken from start point and end point on the fly]
    #     Args:
    #         start_point ([string]): [station or depot]
    #         end_point ([string]): [station or depot]

    #     Returns:
    #         [Total_Distance]: [distance between station - depot or station to station]
    #         [Time_taken_hour]: [Time_taken_hour between station - depot or station to station]
    #     """
    #     Total_Distance = 0
    #     Time_taken_hour = 0
    #     df_cloud_region = pd.DataFrame()
    #     # print("[OpenRoute API] Reading GPS data from database is being initiated...")
    #     station_data_gps = InputHandler.get_gps_data(self.import_settings)
    #     station_data_gps['Fuel Acc'] = station_data_gps['Fuel Acc'].astype(int).astype(str)
    #     # print("[OpenRoute API] Reading GPS data from database has completed...")

    #     # print("[OpenRoute API] Getting the longitude and latitude for start point & end point is being initiated...")
    #     longitude_first = station_data_gps['Longitude'][station_data_gps['Fuel Acc']==start_point].values
    #     latitude_first = station_data_gps['Latitude'][station_data_gps['Fuel Acc']==start_point].values
    #     longitude_second = station_data_gps['Longitude'][station_data_gps['Fuel Acc']==end_point].values
    #     latitude_second = station_data_gps['Latitude'][station_data_gps['Fuel Acc']==end_point].values
    #     # print("[OpenRoute API] Getting the longitude and latitude for start point & end point haas completed...")

    #     if (len(longitude_first) == 0): # depot cannot be locate in rts_gps_full_data'
    #         # print("[OpenRoute API] Getting the longitude and latitude of terminal is being initiated...")
    #         depot_data_gps = InputHandler.get_terminal_data(self.import_settings)
    #         longitude_first = depot_data_gps['longitude'][depot_data_gps['terminal_ID']==start_point].values
    #         latitude_first = depot_data_gps['latitude'][depot_data_gps['terminal_ID']==start_point].values
    #         # print("[OpenRoute API] Getting the longitude and latitude of terminal has completed...")

    #     if (len(longitude_second) == 0): # depot cannot be locate in rts_gps_full_data
    #         # print("[OpenRoute API] Getting the longitude and latitude of terminal is being initiated...")
    #         depot_data_gps = InputHandler.get_terminal_data(self.import_settings) 
    #         longitude_second = depot_data_gps['longitude'][depot_data_gps['terminal_ID']==end_point].values
    #         latitude_second = depot_data_gps['latitude'][depot_data_gps['terminal_ID']==end_point].values
    #         # print("[OpenRoute API] Getting the longitude and latitude of terminal has completed...")

    #     df_temp = {'Fuel Acc': [start_point, end_point]}
    #     df_temp_accName = pd.DataFrame(df_temp, columns = ['Fuel Acc'])
        
    #     if len(longitude_first) > 0 and len(latitude_first) > 0 and len(longitude_second) > 0 and len(latitude_second) > 0 :
    #         body = {"locations":[[float(longitude_first), float(latitude_first)], [float(longitude_second), float(latitude_second)]],
    #                 "id":"temp","metrics":["distance"],"resolve_locations":"true","units":"km"}
    #         headers = {
    #             'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
    #             'Authorization': '5b3ce3597851110001cf6248bcc69e0202de46c9a47e9d02385f2312',
    #             'Content-Type': 'application/json; charset=utf-8'
    #         }
    #         #print("[OpenRoute API] Sending post request to Openroute is being initiated...")
    #         call = requests.post('https://api.openrouteservice.org/v2/matrix/driving-hgv', json=body, headers=headers)
            
    #         if (call.status_code==200):
    #             lists_output = call.json()['distances']
    #             df_full = pd.DataFrame()
    #             for i in df_temp_accName['Fuel Acc']:
    #                 for j in df_temp_accName['Fuel Acc']:
    #                     df = pd.DataFrame()
    #                     df['First_location'] = [i]
    #                     df['Next_location'] = [j]
    #                     df_full = df_full.append(df).reset_index(drop=True)

    #             df_full['Total_Distance'] = np.resize(lists_output,len(df_full))
    #             df_full = df_full[df_full['Total_Distance']!=0.0].reset_index(drop=True) 
    #             df_full['Time_taken_hour']=(pd.to_numeric(df_full['Total_Distance'])/40)
    #             df_full['Time_taken_hour']=df_full['Time_taken_hour'].round(2)
    #             df_full['Region'] = region
    #             #df_full.to_sql('rts_openroute_distance', con=engine, if_exists='append', index=False)
    #             DBConnector.save(df_full, self.import_settings['Tables']['open_route_distance']['tablename'], if_exists='append')

    #             #print("[OpenRoute API] Distance time matrix is succesfully loaded to DB")
    #         else:
    #             print("[OpenRoute API] Openroute API has problem calculating distance, please find the error message: {}, {}".format(call.status_code, call.reason))
    #             print("[Distance Timematrix] Reading distance time matrix is being initiated...")
    #             full_dist_matrix = InputHandler.get_dist_backup(self.import_settings)
    #             try :
    #                 Total_Distance = float(full_dist_matrix[(full_dist_matrix['First_location'] == str(start_point)) & (full_dist_matrix['Next_location'] == str(end_point))].Total_Distance)
    #                 Time_taken_hour = float(full_dist_matrix[(full_dist_matrix['First_location'] == str(start_point)) & (full_dist_matrix['Next_location'] == str(end_point))].Time_taken_hour)
    #             except Exception as e:
    #                 self.status = False
    #                 self.message = f"[Distance Timematrix Error] Distance time matrix has no pre-calulated value for {start_point} and {end_point}."                    
    #                 print(self.message)
    #                 Total_Distance = 1
    #                 Time_taken_hour = 1
    #             return Total_Distance, Time_taken_hour
    #     else :
    #         self.status = False 
    #         Total_Distance = 1
    #         Time_taken_hour = 1
    #         return Total_Distance, Time_taken_hour

    #     return df_full['Total_Distance'][0], df_full['Time_taken_hour'][0]


    def distance_travelled(self, destinations, region, order):
            """
            Args: to calculate total and current time + distance 
                self.destinations: contain all destinations in selected tanker, start from terminal (insert manually)
                curr_travel_time: how much current travel time + unloading time + 1 hour loading time (if from terminal)
                curr_travel_distance: how much current travel distance
            """
            curr_travel_time = 0
            curr_travel_distance = 0
            time_leave_depot = self.depot_start_time
            eta = self.date_time + datetime.timedelta(days=1)
            dist_matrix = InputHandler.get_dist_matrix(region)
            dist_matrix = dist_matrix.drop_duplicates(subset=['First_location', 'Next_location'], keep="first")

            self.status = True

            for i, j in zip(destinations[:-1], destinations[1:]):
                i = str(i)
                j = str(j)
                if i != j:
                    # To cater for eastern region constraint where orders from kerteh to kelantan requires an additional hour to change driver at Terengganu
                    kerteh_kelantan_add_hour_travel_time = 0 
                    if order.long_distance == 'LDLAST3':
                        kerteh_kelantan_add_hour_travel_time = 1
                    if i == str(order.terminal_95) or i == str(order.terminal_97): #plus 1 hour loading + waiting time at terminal and 30 mins unloading at station
                        if (len(dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)])!=0):              
                            curr_travel_time = float(dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Time_taken_hour) + 1.5 
                            self.travel_mins = float(((dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Total_Distance)/40)*60) + 90
                            if region == 'EASTERN':
                                curr_travel_time += kerteh_kelantan_add_hour_travel_time
                                self.travel_mins += (kerteh_kelantan_add_hour_travel_time*60)   
                            if self.count == 0:
                                self.forward_mins = float(((dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Total_Distance)/40)*60) + 60
                                eta = time_leave_depot + datetime.timedelta(minutes = self.forward_mins)
                        else:
                            print("[RouteGenerator] No destination found, OpenSource API Calculation in Progress...")
                            curr_distance, curr_travel_time = open_route_distance_cal(i, j, region)
                            if self.status :
                                curr_travel_time = curr_travel_time + 1.5 + kerteh_kelantan_add_hour_travel_time
                                self.travel_mins = (curr_distance/40)*60 + 90
                                # print("[RouteGenerator] Distance is successfully computed using OpenSource API...")
                                if self.count == 0:
                                    self.forward_mins = (curr_distance/40)*60
                                    eta = time_leave_depot + datetime.timedelta(minutes = self.forward_mins)
                            else :
                                pass
                    else :
                        if (len(dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)])!=0):
                            curr_travel_time = float(dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Time_taken_hour) + 0.5
                            self.travel_mins += float(((dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Total_Distance)/40)*60) + self.unloading_mins
                            self.forward_mins += float(((dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Total_Distance)/40)*60) + self.unloading_mins
                            eta = time_leave_depot + datetime.timedelta(minutes = self.forward_mins)
                        else:
                            print("[RouteGenerator] 2nd No destination found, OpenSource API Calculation in Progress...")
                            curr_distance, curr_travel_time = open_route_distance_cal(i, j, region)
                            if self.status :
                                curr_travel_time = curr_travel_time + 0.5
                                self.travel_mins += (curr_distance/40)*60 + self.unloading_mins
                                self.forward_mins += float((curr_distance/40)*60) + self.unloading_mins
                                eta = time_leave_depot + datetime.timedelta(minutes = self.forward_mins)
                                # print("[RouteGenerator] 2nd Distance is successfully computed using OpenSource API...")
                            else :
                                pass 
                    
                    if self.status : 
                        if self.return_time == 0 :
                            self.return_time = eta
                            self.return_dest = j
                        elif self.return_time < eta :
                            self.return_time = eta
                            self.return_dest = j
                        else:
                            pass

                        if (len(dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)])!=0):
                            curr_travel_distance = dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Total_Distance
                        else:
                            curr_travel_distance = curr_distance
                    else :
                        pass

                if self.status :     
                    self.count += 1
                    self.unloading_mins += 30
                else :
                    pass

            if self.status :
                if eta >= self.shift_end :
                    self.status = False
            else :
                eta = None
            return curr_travel_time, curr_travel_distance, eta


    def convert_retain(self, slot):
        slot_dt = pd.to_datetime(slot, format='%H')
        slot_dt = slot_dt.strftime("%Y-%m-%d %H:%M:%S")
        slot_dt = datetime.datetime.strptime(slot_dt, '%Y-%m-%d %H:%M:%S')
        slot_dt = slot_dt + pd.offsets.DateOffset(year=self.date_time.year, day=self.date_time.day, month=self.date_time.month)
        slot_dt = slot_dt + pd.DateOffset(days=1)  
        #slot_dt = slot_dt.strftime("%Y-%m-%d %H:%M:%S")
        #slot_dt = datetime.datetime.strptime(slot_dt, '%Y-%m-%d %H:%M:%S')
        return slot_dt


    def plan_load_time(self):
        tank_shift_start = self.shift_start
        time_leave_depot = self.depot_start_time
        retain =  max(i.slot for i in self.orders)

        retain = self.convert_retain(retain)
        start_time = retain - datetime.timedelta(minutes=self.travel_mins) 

        if start_time < tank_shift_start : 
            start_time = tank_shift_start
        diff_mins = (start_time - time_leave_depot).total_seconds() / 60

        self.status = True

        if start_time < self.depot_close_time:
                        
            for order in self.orders:
                order.pldtime = start_time
                order.eta += datetime.timedelta(minutes=diff_mins)
                order_eta = order.eta
                order_id = order.unique_id
                
                if order_eta >= self.shift_end :
                    self.status = False
                    self.message = f"[Shift Time Constraint] Order: {order_id} adjusted ETA : {order_eta} more than tanker shift end time : {self.shift_end}."
                    for order in self.orders :
                        self.unmatched_orders.append(order)
                    self.orders = []
                    break
            
            if self.status :
                max_value_1 = 0
                max_value_2 = 0
                max_value_3 = 0
                max_value_4 = 0
                max_value_5 = 0
                max_time = 0
                count_1 = 0
                count_2 = 0
                count_3 = 0
                count_4 = 0
                count_5 = 0
                count_6 = 0
                
                for i in self.orders:
                    retain = self.convert_retain(i.slot)
                    runout = i.runout

                    if i.order_status == "multiload" and i.eta > runout:
                        # print("[RouteGenerator Constraint] Checking constraint for multiload and ETA > Runout...")
                        time_gap_mins = abs((i.eta - runout).total_seconds() / 60)
                        if count_1 == 0 :
                            max_value_1 = time_gap_mins
                        else :
                            if time_gap_mins >= max_value_1 :
                                max_value_1 = time_gap_mins
                        count_1 += 1
                    
                    elif i.order_status == "multiload" and i.eta < retain :
                        # print("[RouteGenerator Constraint] Checking constraint for multiload and ETA < Retain...")
                        time_gap_mins = abs((retain - i.eta).total_seconds() / 60)
                        if count_2 == 0 :
                            max_value_2 = time_gap_mins
                        else :
                            if time_gap_mins >= max_value_2 :
                                max_value_2 = time_gap_mins
                        count_2 += 1
                        
                    elif i.order_status == "normal" and i.eta < retain:
                        # print("[RouteGenerator Constraint] Checking constraint for normal and ETA < Retain...")
                        time_gap_mins = abs((retain - i.eta).total_seconds() / 60)
                        if count_3 == 0 :
                            max_value_3 = time_gap_mins
                        else :
                            if time_gap_mins >= max_value_3 :
                                max_value_3 = time_gap_mins
                        count_3 += 1               

                    elif i.order_status == "smp" and i.eta > runout:
                        # print("[RouteGenerator Constraint] Checking constraint for smp and ETA > Runout...")
                        time_gap_mins = abs((i.eta - runout).total_seconds() / 60)
                        if count_4 == 0 :
                            max_value_4 = time_gap_mins
                        else :
                            if time_gap_mins >= max_value_4 :
                                max_value_4 = time_gap_mins
                        count_4 += 1       

                    elif i.order_status == "smp" and i.eta < retain:
                        # print("[RouteGenerator Constraint] Checking constraint for smp and ETA < Retain...")
                        time_gap_mins = abs((retain - i.eta).total_seconds() / 60)
                        if count_5 == 0 :
                            max_value_5 = time_gap_mins
                        else :
                            if time_gap_mins >= max_value_5 :
                                max_value_5 = time_gap_mins
                        count_5 += 1                         

                    else :
                        count_6 += 1
                
                max_time_status = 0
                
                if count_1 > 0 or count_2 > 0 :
                    if max_value_1 >= max_value_2 :
                        max_time_status = 1
                        max_time = max_value_1
                    else :
                        max_time_status = 2
                        max_time = max_value_2
                    
                    if count_4 > 0 or count_5 > 0 : 
                        if max_value_4 >= max_time :
                            max_time_status = 1
                            max_time = max_value_4
                        elif max_value_5 >= max_time :
                            max_time_status = 2
                            max_time = max_value_5
                        else :
                            pass

                    for order in self.orders:
                        retain = self.convert_retain(order.slot)
                        runout = order.runout

                        if max_time_status < 2:
                            order.pldtime -= datetime.timedelta(minutes=max_time)
                            order.eta -= datetime.timedelta(minutes=max_time)
                        else:
                            order.pldtime += datetime.timedelta(minutes=max_time)
                            order.eta += datetime.timedelta(minutes=max_time)

                        order_pldtime = order.pldtime
                        order_eta = order.eta
                        order_id = order.unique_id
                        closing_time = order.closing_time
                        # runout_duration = (abs((runout - retain).total_seconds() / 3600))

                        if order.order_status == "multiload" and (order.eta < retain or order.eta > runout) :
                        #if order.order_status == "multiload" and (order.eta < retain or order.eta > (retain + datetime.timedelta(minutes=360))) :
                            # print("[RouteGenerator Constraint] Checking constraint for multiload and ETA < Retain or ETA > Runout...")
                            # print("[RouteGenerator Constraint] Runout Constraint where ETA {}, Retain {}, Runout {}".format(i.eta, retain, runout))
                            self.status = False
                            self.message = f"[Runout Constraint] Multiload Order : {order_id} adjusted ETA : {order_eta} earlier than retain : {retain} or later than runout : {runout}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.order_status == "smp" and (order.eta < retain or order.eta > runout) :
                        #if order.order_status == "multiload" and (order.eta < retain or order.eta > (retain + datetime.timedelta(minutes=360))) :
                            # print("[RouteGenerator Constraint] Checking constraint for smp and ETA < Retain or ETA > Runout...")
                            # print("[RouteGenerator Constraint] Runout Constraint where ETA {}, Retain {}, Runout {}".format(i.eta, retain, runout))
                            self.status = False
                            self.message = f"[Runout Constraint] SMP Order : {order_id} adjusted ETA : {order_eta} earlier than retain : {retain} or later than runout : {runout}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.order_status == "normal" and (order.eta < retain or order.eta > runout) :
                            # print("[RouteGenerator Constraint] Checking constraint for normal and ETA < Retain...")
                            self.status = False
                            self.message = f"[Retain Constraint] Normal Order : {order_id} adjusted ETA : {order_eta} earlier than retain : {retain} or later than runout : {runout}"
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.eta >= self.shift_end :
                            self.status = False
                            self.message = f"[Shift Time Constraint] Order: {order_id} adjusted ETA : {order_eta} later than tanker shift end time : {self.shift_end}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.pldtime < self.shift_start:
                            self.status = False
                            self.message = f"[Shift Time Constraint] Order: {order_id} adjusted plan load time : {order_pldtime} earlier than tanker shift start time : {self.shift_start}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.pldtime > self.depot_close_time:
                            self.status = False
                            self.message = f"[Depot Closing Time Constraint] Order: {order_id} adjusted plan load time : {order_pldtime} later than depot closing time : {self.depot_close_time}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)                            

                        if order.eta >= closing_time:
                            self.status = False
                            self.message = f"[Station Closing Time Constraint] Order: {order_id} adjusted ETA : {order_eta} later than station closing time : {closing_time}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)                    

                        if self.status == False :
                            self.orders = []
                            break

                elif count_4 > 0 or count_5 > 0 : 
                    if max_value_4 >= max_value_5 :
                        max_time_status = 1
                        max_time = max_value_4
                    else :
                        max_time_status = 2
                        max_time = max_value_5

                    for order in self.orders:
                        retain = self.convert_retain(order.slot)
                        runout = order.runout

                        if max_time_status < 2:
                            order.pldtime -= datetime.timedelta(minutes=max_time)
                            order.eta -= datetime.timedelta(minutes=max_time)
                        else:
                            order.pldtime += datetime.timedelta(minutes=max_time)
                            order.eta += datetime.timedelta(minutes=max_time)

                        order_pldtime = order.pldtime
                        order_eta = order.eta
                        order_id = order.unique_id
                        closing_time = order.closing_time
                        # runout_duration = (abs((runout - retain).total_seconds() / 3600))

                        if order.order_status == "smp" and (order.eta < retain or order.eta > runout) :
                        #if order.order_status == "multiload" and (order.eta < retain or order.eta > (retain + datetime.timedelta(minutes=360))) :
                            # print("[RouteGenerator Constraint] Checking constraint for smp and ETA < Retain or ETA > Runout...")
                            # print("[RouteGenerator Constraint] Runout Constraint where ETA {}, Retain {}, Runout {}".format(i.eta, retain, runout))
                            self.status = False
                            self.message = f"[Runout Constraint] SMP Order : {order_id} adjusted ETA : {order_eta} earlier than retain : {retain} or later than runout : {runout}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.order_status == "normal" and (order.eta < retain or order.eta > runout) :
                            # print("[RouteGenerator Constraint] Checking constraint for normal and ETA < Retain...")
                            self.status = False
                            self.message = f"[Retain Constraint] Normal Order : {order_id} adjusted ETA : {order_eta} earlier than retain : {retain} or later than runout : {runout}"
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.eta >= self.shift_end :
                            self.status = False
                            self.message = f"[Shift Time Constraint] Order: {order_id} adjusted ETA : {order_eta} later than tanker shift end time : {self.shift_end}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.pldtime < self.shift_start:
                            self.status = False
                            self.message = f"[Shift Time Constraint] Order: {order_id} adjusted plan load time : {order_pldtime} earlier than tanker shift start time : {self.shift_start}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.pldtime > self.depot_close_time:
                            self.status = False
                            self.message = f"[Depot Closing Time Constraint] Order: {order_id} adjusted plan load time : {order_pldtime} later than depot closing time : {self.depot_close_time}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)                           

                        if order.eta >= closing_time:
                            self.status = False
                            self.message = f"[Station Closing Time Constraint] Order: {order_id} adjusted ETA : {order_eta} later than station closing time : {closing_time}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)                                              
                
                elif count_3 > 0 :
                    max_time = max_value_3
                    for order in self.orders:
                        order.pldtime += datetime.timedelta(minutes=max_time)
                        order.eta += datetime.timedelta(minutes=max_time)
                        order_pldtime = order.pldtime
                        order_eta = order.eta
                        order_id = order.unique_id
                        closing_time = order.closing_time

                        if order.pldtime > self.depot_close_time:
                            self.status = False
                            self.message = f"[Depot Closing Time Constraint] Order: {order_id} adjusted plan load time : {order_pldtime} later than depot closing time : {self.depot_close_time}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)                     

                        if order.eta >= self.shift_end :
                            self.status = False
                            self.message = f"[Shift Time Constraint] Order: {order_id} adjusted ETA : {order_eta} more than tanker shift end time : {self.shift_end}."
                            for order in self.orders:
                                self.unmatched_orders.append(order)

                        if order.eta >= closing_time :
                            self.status = False
                            self.message = f"[Station Closing Time Constraint] Order: {order_id} adjusted ETA : {order_eta} later than station closing time : {closing_time}."
                            for order in self.orders:
                                self.unmatched_orders.append(order) 

                        if self.status == False :
                            self.orders = []
                            break                                                  
                
                else :
                    self.status = True
                    pass

                return self.status
        
            else:
                return self.status
        
        else :
            self.status = False
            self.message = f"[Depot Closing Time Constraint] Orders earliest plan load time : {start_time} later than depot closing time : {self.depot_close_time}."
            for order in self.orders:
                self.unmatched_orders.append(order)
            return self.status


    def return_depot(self, region):
        """
        Args: to get current destination of order to add
              destination_list : contains destinations of order
              self.last_destination : store last destination
        """
        returning_depot = str(min(i.terminal_95 for i in self.orders))
        if region == "EASTERN" and 70000011 in [i.product_type for i in self.orders]:
            returning_depot = next(i.terminal_97 for i in self.orders if i.terminal_97 != None)
        destination_list = [str(self.return_dest)]
        destination_list.insert(len(destination_list), returning_depot)

        return_mins = 0
        return_time = max(p.eta for p in self.orders)
        dist_matrix = InputHandler.get_dist_matrix(region)
        dist_matrix = dist_matrix.drop_duplicates(subset=['First_location', 'Next_location'], keep="first")

        for i, j in zip(destination_list[:-1], destination_list[1:]):
            i = str(i)
            j = str(j)
            if (len(dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)])!=0):
                return_mins = float(((dist_matrix[(dist_matrix['First_location'] == i) & (dist_matrix['Next_location'] == j)].Total_Distance)/40)*60)
                return_time += datetime.timedelta(minutes = return_mins) 
            else:
                print("No destination found, OpenSource API Calculation in Progress")
                curr_distance, curr_travel_time = open_route_distance_cal(i, j, region)
                return_mins = (curr_distance/40)*60 + 30
                print("Distance computed using API. ")
                return_time += datetime.timedelta(minutes = return_mins)

        self.return_time = return_time + datetime.timedelta(minutes = 30)
        self.total_travel_time += return_mins/60
        self.load_time = max(p.pldtime for p in self.orders)

        def is_time_between(begin_time, end_time, check_time=None):
            # If check time is not given, default to current UTC time
            check_time = check_time or datetime.utcnow().time()
            if begin_time < end_time:
                return check_time >= begin_time and check_time <= end_time
            else: # crosses midnight
                return check_time >= begin_time or check_time <= end_time
        
        if is_time_between(self.shift_start, self.shift_end, self.return_time):
            self.status = True
        else:
            self.status = False
            self.message = f"[Shift Time Constraint] Route's return time at depot : {self.return_time} more than tanker shift end time : {self.shift_end}."

        if self.total_travel_time > self.min_remaining_shift:
            self.status = False
        else :
            self.status = True

        return self.status


    def add_order(self, order):
        """
        Args: to add order into truck
            order: order to be added into truck
        """
        self.capacity -= (int(order.quantity / self.sub_resource_quantity)) * self.sub_resource_quantity
        self.filled += (int(order.quantity / self.sub_resource_quantity)) * self.sub_resource_quantity
        self.orders.append(order)


    def add_unmatched_order(self, order):
        """
        Args: to store unmatched order

        """
        self.unmatched_orders.append(order)


    # Check constraints
    def can_fit(self, order, region):
        """
        Args: boolean to check order can be fit or not
            order: order to be checked
        """
        list_constraints = []
        self.dest_list = self.get_destinations(order)
        self.curr_travel_time, self.curr_distance, est_eta = self.distance_travelled(self.dest_list, region, order)
        if self.status :
            if self.capacity >= order.quantity :
                list_constraints.append(True)
            else :
                return False

            tanker_capacity = self.tanker_capacity
            order_accessibility = order.accessibility
            # print("[RouteGenerator Constraint] Checking constraint for tanker capacity <= accessibility...")
            if tanker_capacity <= order_accessibility :
                list_constraints.append(True)
            else :
                self.message = f"[Accessibility Constraint] Tanker capacity : {tanker_capacity} more than station accessibility : {order_accessibility}."
                return False

            total_order_weight = self.get_weight(order)
            # print("[RouteGenerator Constraint] Checking constraint for total order weight <= HSSE weight...")
            if total_order_weight <= self.weight_limit :
                list_constraints.append(True)
            else :
                self.message = f"[HSSE Weight Limit Constraint] Total delivery weight (orders + dry) : {total_order_weight} more than approved tanker {tanker_capacity} weight limit : {self.weight_limit}."
                return False

            if all(list_constraints):
                self.total_travel_time += self.curr_travel_time
                order.eta = est_eta
                self.add_order(order)
                return True
            
        else :
            if (self.curr_distance == 0).all() :
                return False
            else :
                self.message = f"[Shift Time Constraint] Estimated ETA from zeroth hour : {est_eta} more than tanker end shift time : {self.shift_end}."
                return False


    def can_fit_list(self, orders_list, region, unmatched_capacity, unmatched_terminal = False):
        list_constraints = []
        normal_retain = self.date_time + datetime.timedelta(days=1)
        multiload_retain = self.date_time + datetime.timedelta(days=1)
        multiload_runout = self.date_time + datetime.timedelta(days=1)
        normal_runout = self.date_time + datetime.timedelta(days=1)
        
        if unmatched_terminal == False:
            if unmatched_capacity == False:
                no_drops = len(set([order[1].destination for order in orders_list]))

                # double check no of drops constraint by region
                if no_drops < 3 and region == "CENTRAL":
                    list_constraints.append(True) 
                elif no_drops < 4 and region != "CENTRAL":
                    list_constraints.append(True)
                else:
                    list_constraints.append(False) 
                
                # if no of drops are 2, 3 -> store multiload and non-mulitload orders and their max retain seperately 
                if no_drops > 1:
                    multiload_check = []
                    normal_check = []
                    for order in orders_list:
                        if  "multiload" in order[1].order_status :
                            multiload_check.append(order[1].order_status)
                            if multiload_retain < order[1].slot:
                                multiload_retain = order[1].slot
                            if multiload_runout < order[1].runout:
                                multiload_runout = order[1].runout                            
                        else :
                            normal_check.append(order[1].order_status)
                            if normal_retain < order[1].slot:
                                normal_retain = order[1].slot                            
                            if normal_runout < order[1].runout:
                                normal_runout = order[1].runout

                    # check for retain cannot be more than minimum runout of (Multiload, Normal), (Multiload, Multiload) & (Normal, Normal) pairs of route
                    if  normal_runout < self.shift_end and multiload_runout < self.shift_end :                              
                        if len(multiload_check) > 0 and len(normal_check) > 0 :
                            normal_retain = self.convert_retain(normal_retain)
                            multiload_retain = self.convert_retain(multiload_retain)
                            runout = min(order[1].runout for order in orders_list) 

                            if runout > multiload_retain and runout > normal_retain :
                                if multiload_retain > normal_retain :
                                    runout_duration = (abs((runout - multiload_retain).total_seconds() / 3600))
                                    if (abs((multiload_retain - normal_retain).total_seconds() / 3600)) <= runout_duration :
                                        list_constraints.append(True)
                                    else :
                                        self.message = f"[Runout Constraint] Difference between normal retain : {normal_retain} and multiload retain : {multiload_retain} more than minimum runout : {runout} from multiload retain : {multiload_retain} duration : {runout_duration}."
                                        for order in orders_list:
                                            self.add_unmatched_order(order[1])
                                        list_constraints.append(False)                                    

                                elif normal_retain > multiload_retain : 
                                        runout_duration = (abs((runout - normal_retain).total_seconds() / 3600))
                                        if (abs((normal_retain - multiload_retain).total_seconds() / 3600)) <= runout_duration :
                                            list_constraints.append(True)
                                        else :
                                            self.message = f"[Runout Constraint] Difference between multiload retain : {multiload_retain} and normal retain : {normal_retain} more than minimum runout : {runout} from normal retain : {normal_retain} duration : {runout_duration}."
                                            for order in orders_list:
                                                self.add_unmatched_order(order[1])
                                            list_constraints.append(False) 
                            else :
                                self.message = f"[Runout Constraint] Minimum runout : {runout} is less than multiload retain : {multiload_retain} or normal retain : {normal_retain}."
                                for order in orders_list:
                                    self.add_unmatched_order(order[1])
                                list_constraints.append(False)
                        
                        elif len(multiload_check) > 1 :
                            max_retain = max(order[1].slot for order in orders_list)
                            max_retain = self.convert_retain(max_retain)
                            min_retain = min(order[1].slot for order in orders_list)
                            min_retain = self.convert_retain(min_retain)
                            runout = min(order[1].runout for order in orders_list)

                            if runout > max_retain :
                                runout_duration = (abs((runout - max_retain).total_seconds() / 3600))
                                if (abs((max_retain - min_retain).total_seconds() / 3600)) <= runout_duration:
                                    list_constraints.append(True)
                                else:
                                    self.message = f"[Runout Constraint] Difference between maximum multiload retain : {max_retain} and minimum multiload retain : {min_retain} more than minimum runout : {runout} from maximum multiload retain : {max_retain} duration : {runout_duration}."
                                    for order in orders_list:
                                        self.add_unmatched_order(order[1])
                                    list_constraints.append(False)
                            else :
                                self.message = f"[Runout Constraint] Minimum runout : {runout} is less than maximum multiload retain of 2 multiload orders : {max_retain}."
                                for order in orders_list:
                                    self.add_unmatched_order(order[1])
                                list_constraints.append(False)

                        elif len(normal_check) > 1 :      
                            max_retain = max(order[1].slot for order in orders_list)
                            max_retain = self.convert_retain(max_retain)
                            min_retain = min(order[1].slot for order in orders_list)
                            min_retain = self.convert_retain(min_retain)
                            runout = min(order[1].runout for order in orders_list)

                            if runout > max_retain :
                                runout_duration = (abs((runout - max_retain).total_seconds() / 3600))
                                if (abs((max_retain - min_retain).total_seconds() / 3600)) <= runout_duration:
                                    list_constraints.append(True)
                                else:
                                    self.message = f"[Runout Constraint] Difference between maximum normal retain : {max_retain} and minimum normal retain : {min_retain} more than minimum runout : {runout} from maximum normal retain : {max_retain} duration : {runout_duration}."
                                    for order in orders_list:
                                        self.add_unmatched_order(order[1])
                                    list_constraints.append(False)
                            else :
                                self.message = f"[Runout Constraint] Minimum runout : {runout} is less than maximum normal retain of 2 normal orders : {max_retain}."
                                for order in orders_list:
                                    self.add_unmatched_order(order[1])
                                list_constraints.append(False)                                                      
                        
                        else :
                            list_constraints.append(True)
                    else :
                        list_constraints.append(True)
                else :
                    list_constraints.append(True)

                if all(list_constraints):
                    for order in orders_list:
                        list_constraints.append(self.can_fit(order[1], region))
                        if all(list_constraints):
                            continue
                        else:
                            for order in orders_list:
                                self.add_unmatched_order(order[1])
                            break
                if all(list_constraints):
                    list_constraints.append(self.plan_load_time())
                    if all(list_constraints):
                        list_constraints.append(self.return_depot(region))
                    else: 
                        self.orders = []

            else :
                list_constraints.append(False)
                order_sum = 0
                for order in orders_list:
                    self.add_unmatched_order(order[1])
                    order_sum += order[1].quantity
                self.message = f"[Fully Fitted Constraint] Total Order Quantity : {order_sum} not fully fit Tanker : {self.capacity}."

        else :
            list_constraints.append(False)
            u97 = []
            non_u97 = []
            for order in orders_list:
                if order[1].product_type == 70000011 :
                    u97.append(order[1].terminal_97)
                else :
                    non_u97.append(order[1].terminal_95)
                self.add_unmatched_order(order[1])
            terminal_list = set([item for sublist in [u97,non_u97] for item in sublist])
            self.message = f"[Single Terminal Constraint] More than one terminal : {terminal_list} in number of orders : {len(orders_list)}."
        
        return all(list_constraints)


    def retain_range(self, orders):
        """[calculate the difference between retain hours for each paired orders, range should be 4 hours or lesser.]

        Args:
            orders ([dictionary]): orders to be checked
        """
        self.previous_retain.append(orders.slot)
        diff = max(self.previous_retain) - min(self.previous_retain)
        return diff


    def get_weight(self, orders):
        """[calculate the compartment truck weight and product carried weight]

        Args:
            orders ([dictionary]): orders to be checked
        """
        self.prod_weight += orders.weight
        total_weight = self.dry_weight + self.prod_weight
        return total_weight


    def utilization(self):
        '''
        :return: utilization of truck
        :rtype: float
        '''
        return self.filled/(self.filled + self.capacity)

    def remove_order(self, order):
        """
        Args: to add order into truck
            order: order to be added into truck
        """
        self.capacity += (int(order.quantity / self.sub_resource_quantity)) * self.sub_resource_quantity
        self.filled -= (int(order.quantity / self.sub_resource_quantity)) * self.sub_resource_quantity
        self.orders.remove(order)

    def cost(self):
        '''
        to calculate cost of truck
        :return: cost of trip
        :rtype: float
        '''
        try:
            max_dist = max(distance_data.loc[(distance_data['Cust.dep.'].isin(self.destinations)) & (distance_data['Terminal']=='KVDT (M808)')]['Distance'])
        except KeyError or ValueError:
            max_dist = 0
        sum_quantity = self.filled
        rate_df = rate_data.loc[(rate_data['Distance_Lower'] <= max_dist) & (rate_data['Distance_Upper'] >= max_dist) &
                                (rate_data['Volume_Lower'] <= sum_quantity) & (rate_data['Volume_Upper'] >= sum_quantity)]
        prod_quantity = {}
        for o in self.orders:
            try:
                prod_quantity[o.product_type] += o.quantity
            except KeyError:
                prod_quantity[o.product_type] = o.quantity
        trip_cost = 0
        for i in prod_quantity:
            trip_cost += sum(prod_quantity[i] * rate_df[rate_df['Product'] == i]['Rate'].values)
        return trip_cost

    def best_route(self):
        '''
        :return: best sequence of trip
        :rtype: list
        '''
        return shortest_tour(alltours(self.destinations))
    
    def update_shift(self):
        Resource._shift -= 1