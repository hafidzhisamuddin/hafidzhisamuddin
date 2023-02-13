import os
from itertools import combinations
from math import sqrt, floor
import pandas as pd
import numpy as np
import pickle
import geopy.distance as gp
import collections
import bisect
import time
from datetime import datetime,  timedelta
from functools import cmp_to_key
import random
from sqlalchemy import create_engine
from conf import Config
import requests
from dateutil import parser

from src.input_gen_modules.rts_input_handler import InputHandler

from database_connector import DatabaseConnector, YAMLFileConnector
from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

"""
Weighted Interval scheduling algorithm.
Runtime complexity: O(n log n)
"""

class Interval(object):
    '''Date weighted interval'''

    def __init__(self, shipment, start, finish, weight, len_order):
        self.shipment = shipment
        self.start = int(time.mktime(start.to_pydatetime().timetuple()))
        self.finish = int(time.mktime(finish.timetuple()))
        self.weight = weight

    def __repr__(self):
        return str((self.shipment, datetime.fromtimestamp(self.start), datetime.fromtimestamp(self.finish), self.weight))

def compute_previous_intervals(I):
        '''For every interval j, compute the rightmost mutually compatible interval i, where i < j
            I is a sorted list of Interval objects (sorted by finish time)
        '''
        # extract start and finish times
        start = [i.start for i in I]
        finish = [i.finish for i in I]

        p = []
        for j in range(len(I)):
            i = bisect.bisect_right(finish, start[j]) - 1  # rightmost interval f_i <= s_j
            p.append(i)

        return p

def schedule_weighted_intervals(I):
    '''Use dynamic algorithm to schedule weighted intervals
    sorting is O(n log n),
    finding p[1..n] is O(n log n),
    finding OPT[1..n] is O(n),
    selecting is O(n)
    whole operation is dominated by O(n log n)
    '''

    I.sort(key=cmp_to_key(lambda x, y: x.finish - y.finish))  
    p = compute_previous_intervals(I)

    # compute OPTs iteratively in O(n), here we use DP
    OPT = collections.defaultdict(int)
    OPT[-1] = 0
    OPT[0] = 0
    for j in range(1, len(I)):
        OPT[j] = max(I[j].weight + OPT[p[j]], OPT[j - 1])

    # given OPT and p, find actual solution intervals in O(n)
    O = []
    
    def compute_solution(j):
        if j >= 0:  # will halt on OPT[-1]
            if I[j].weight + OPT[p[j]] > OPT[j - 1]:
                O.append(I[j])
                compute_solution(p[j])
            else:
                compute_solution(j - 1)
    compute_solution(len(I) - 1)

    # resort, as our O is in reverse order (OPTIONAL)
    O.sort(key=cmp_to_key(lambda x, y: x.finish - y.finish))

    return O


def calculate_distance(x1, y1, x2, y2):
    return sqrt((x1-x2)**2 + (y1-y2)**2)


def possible_pairs(l):
    return combinations(l, 2)


# matrix = []

# dm_df = pd.DataFrame(matrix)
# pickle.dump(dm_df, open("matrix.p", "wb"))
# dm_df1 = pickle.load(open("matrix.p", "rb"))

def get_distance(lat1, long1, lat2, long2):
    coord1 = lat1, long1
    coord2 = lat2, long2
    return gp.geodesic(coord1, coord2).km


def knapsack(items):
    weight = [i[1] for i in items]

    table = [[0 for w in range(len(weight) + 1)] for i in range(len(items) + 1)]

    for i in range(1, len(items) + 1):
        item_name, item_weight, item_value = items[i-1]
        for w in range(1, len(weight) + 1):
            if item_weight > w:
                table[i][w] = table[i-1][w]
            else:
                table[i][w] = max(table[i-1][w], table[i-1][w-item_weight] + item_value)

    result = []
    w = weight
    for i in range(len(items), 0, -1):
        if (table[i][w] != table[i-1][w]):
            item_name, item_weight, item_value = items[i-1]
            result.append(items[i-1])
            w -= item_weight
    return result


# Counts the total weight and total value of items to be added
def totalvalue(comb):
    weight_total = value_total = 0
    for item_name, item_weight, item_value in comb:
        weight_total  += item_weight
        value_total += item_value
    return value_total, weight_total


def get_schedule(terminal, orders_list):
    '''
    :param terminal: object where the truck start route
    :param orders_list: list of orders to be schedule and check its feasibility
    :returns bool for route exist or not, list of start and end of trip
    '''
    can_start = min(terminal.spot_list)  # find which slot is available
    destination_list = [o.destination for o in orders_list]
    loading_time = loading_time(orders_list)
    start_time = can_start + loading_time
    orders_list.append(0, depot)
    for i, j in zip(destination_list[:-1], destination_list[1:]):
        start_time += distance(i, j)/40 + unloading(o)*len([o for o in orders_list if o.destination==j])
        if arrival_time >= retain(j):
            route = True
        else:
            route = False
            break
    end_time = start_time + distance(j, depot)/40
    return route, [can_start, end_time]


# Python program to print all subsets with given sum 
  
# The vector v stores current subset. 
def printAllSubsetsRec(arr, n, v, sum) : 
  
    # If remaining sum is 0, then print all 
    # elements of current subset. 
    ans =[]
    if (sum == 0) : 
        for value in v : 
            # print(value, end=" ") 
            ans.append(value)

        # print(ans) 
        # return ans
      
  
    # If no remaining elements, 
    if (n == 0): 
        return ans
  
    # We consider two cases for every element. 
    # a) We do not include last element. 
    # b) We include last element in current subset.
    ans = [] 
    ans.append(printAllSubsetsRec(arr, n - 1, v, sum)) 
    v1 = [] + v 
    v1.append(arr[n - 1]) 
    ans.append(printAllSubsetsRec(arr, n - 1, v1, sum - arr[n - 1]))
    return ans
  
  
# Wrapper over printAllSubsetsRec() 
def printAllSubsets(arr, n, sum): 
  
    v = []
    return printAllSubsetsRec(arr, n, v, sum) 
  
  
# Driver code 
  
arr = [ 2, 5, 8, 4, 6 ] 
sum = 13
n = len(arr) 
hello = printAllSubsets(arr, n, sum)
flat = [m for i in hello for j in i for k in j for l in k for m in l  if len(m)>0]
for i in hello:
    for j in i:
        for k in j:
            for l in k:
                for m in l:
                    for n in m:
                        pass
                        # print(n)

def replace_start_shift_time(truck_master_region_row, depot_dates):
    """
    This function replaces start shift of truck with terminal's start time if truck is of Double shift type
    """
    if truck_master_region_row['shift_type'] == 'Double':
        return depot_dates.groupby(['terminal']).get_group(truck_master_region_row['default_terminal'])['time_from'].astype(str).values[0]
    else:
        return "08:00:00"                        


def replace_opening_time_scheduling(station_restriction_row, depot_dates):
    """
    This function replaces opening start time with respective terminal opening time if station opening time is 00:00:00.
    """
    import datetime as dt # importing datetime here so it does not mess up datetime in other scripts

    if station_restriction_row['Opening_Time'] == dt.time(0,0,0):
        return depot_dates.groupby(['terminal']).get_group(
            station_restriction_row['terminal'])['time_from'].astype(str).values[0]
    else:
        return station_restriction_row['Opening_Time'] 

    # if station_restriction_row['Opening_Time'] == dt.time(0,0,0):
    #     return depot_dates.groupby(['terminal']).get_group(
    #         terminal_merge.groupby(keyname).get_group(station_restriction_row[keyname])['terminal'].values[0]
    #         )['time_from'].astype(str).values[0]
    # else:
    #     return station_restriction_row['Opening_Time']                          


def texify_numeric_station_id(station_id) :
    """
    Change numeric station id into "00XXXX.." format
    """
    modified_retail_list = []
    for x in station_id :
        if len(str(x)) == 10 :
            modified_retail_list.append(str(x))
        elif len(str(x)) == 9 :
            modified_retail_list.append("0"+str(x))
        else :
            modified_retail_list.append("00"+str(x))
    
    return modified_retail_list


def find_truck_free_windows(time_windows) :
    """
    Find free windows from utilized time windows of tanker from locked orders and/or 1st loop scheduling
    """
    if (time_windows[0][4] != time_windows[0][5]) :
        tp = []
        tp = [(time_windows[0][0] , time_windows[0][0])]

        for i, _ in enumerate(time_windows) :
            tp.append((time_windows[i][2], time_windows[i][3]))
        tp.append((time_windows[0][1] , time_windows[0][1])) 

        free_time = []
        for i,_ in enumerate(tp):
            if i > 0:
                if (tp[i][0] - tp[i-1][1]) > 0:
                    tf_start = tp[i-1][1]
                    delta = tp[i][0] - tp[i-1][1]
                    tf_end = tf_start + delta
                    free_time.append((tf_start ,tf_end))
    else :
       free_time = []
       free_time.append((time_windows[0][2] , time_windows[0][3]))

    return free_time                        

def modify_multiload_retain(retain_column, column_list, sort_list, order_data):
    """
    Add 1 hour to retain of multiload (or multiple normal) orders of same product, quantity and retain
    for distinction of row
    """
    
    column_list = column_list + retain_column
    for retain in retain_column :
        duplicate_df = order_data[order_data.duplicated(subset = column_list, keep = False)]

        if len (duplicate_df) :
            duplicate_df['diff'] = duplicate_df.sort_values(sort_list + [retain]).groupby(sort_list)[retain].diff()
            duplicate_df = duplicate_df.dropna(subset=['diff'])
            time_list = ['diff']
            for col in time_list :
                duplicate_df[col] = change_timedelta_format(duplicate_df[col])
            duplicate_df['diff'] = duplicate_df.apply(lambda row : parser.parse(str(row['diff'])).hour, axis = 1)
            duplicate_df['diff'] = duplicate_df['diff'] + 1
            duplicate_df[retain] = duplicate_df.apply(lambda row : row[retain] + timedelta(hours=row['diff']), axis = 1)
        else :
            duplicate_df = pd.DataFrame() 
    return duplicate_df


def renaming_product_id(df, column):

    """Renaming product ID to follow the config file."""

    res = dict((k,v) for k,v in Config.DATA['PRODUCT_ID'].items())
    for k,v in Config.DATA['PRODUCT_ID'].items():
        df[column] = df[column].replace(k, v, regex=True)
    return df

        
def renaming_product_name(df, column):
    
    """Renaming product name to follow the config file."""

    res = dict((k,v) for k,v in Config.DATA['PRODUCT_NAME'].items())
    for k,v in Config.DATA['PRODUCT_NAME'].items():
        df[column] = df[column].replace(k, v, regex=True)
    return df  
        
        
def calc_muliload_data(data, n):
    greater_than_22 = random.sample(list(data[data['balance_shift'] > 22]['Vehicle Number']), n)
    trucks_df = pd.DataFrame()
    for truck in greater_than_22:
        test = data[data['Vehicle Number'] == truck]
        data = data[data['Vehicle Number'] != truck]
        test = test.append([test]*2,ignore_index=True)
        for i, row in test.iterrows():
            if i == 1:
                test['end_shift'][i] = test['start_shift'][i] + 12.5 
                test['Vehicle Number'][i] = str(test['Vehicle Number'][i])+'_1'
                test['balance_shift'][i] = test['end_shift'][i] - test['start_shift'][i] #timedelta(hours=1,minutes=30)
            elif i== 2:
                test['start_shift'][i] = test['end_shift'][i-1] #timedelta(hours=1)
                test['Vehicle Number'][i] = str(test['Vehicle Number'][i])+'_2'
                test['balance_shift'][i] = test['end_shift'][i] - test['start_shift'][i] #timedelta(hours=1,minutes=30)
        test = test.drop(test.index[0])
        trucks_df = trucks_df.append(test)
    data = data.append(trucks_df,ignore_index=True)
    return data


def frac(n):
    i = int(n)
    f = round((n - int(n)), 4)
    return (i, f)
                
def frmt(hour): 
    hours, _min = frac(hour)
    minutes, _sec = frac(_min*60)
    seconds, _msec = frac(_sec*60)
    return "%02d:%02d:%02d"%(hours, minutes, seconds)


def order_tagging_numeric(route_output):
    """[Return numeric tagging of each combination of tank status + multiload status]

    Args:
        route_output ([dataframe]): [output of all possible routes]
        route_ID (string) : unique route ID
    Returns:
        [integers]: [sum of numeric of each order data row]
    """
    data_tank_unique = route_output['Tank_Status'].unique()
    unique_status = ['missing_tank','Error','Normal', 'LV2', 'LV1', 'TC']
    unique_status_remain = [x for x in unique_status if x in data_tank_unique]
    score = [10*i+1 for i in range(len(unique_status_remain))]
    status_count = ({i:j for i,j in zip(unique_status_remain, score)})
    route_output['tank_numeric'] = route_output['Tank_Status'].map(status_count)
    route_output['tank_numeric'] = route_output['tank_numeric'].astype(int)
    route_output['numeric_count'] = np.where(route_output['Multiload_Status']=='Multiload', 10000*route_output['tank_numeric'], route_output['tank_numeric'])
    group_sum = pd.DataFrame(route_output.groupby('Route_ID')['numeric_count'].sum()).reset_index().rename(columns={'numeric_count':'sum_count'})
    route_output = pd.merge(route_output, group_sum, on='Route_ID')
    # scale the range of route_output to 0 - 100 to follow objective 1 in milp
    OldRange = (route_output['sum_count'].max() - route_output['sum_count'].min())  
    minimum = route_output['sum_count'].min()
    NewRange = 100 - 1
    route_output['sum_count'] = (((route_output['sum_count']- route_output['sum_count'].min()) * NewRange) / OldRange) + 1
    route_output['sum_count'][route_output['sum_count'].isnull()] = minimum # impute with 1 cause NA when divide by Zero range
    #route_output['sum_count'] = route_output['sum_count'].astype(int)
    #numeric_count = route_output['sum_count'][route_output['Route_ID']==route_ID][0] 
    return route_output


def change_table_datatype(table, tableName):
    """[convert table column to respectived data types]

    Args:
        table ([dataframe]): [table in server]
        tableName ([string]): [name of the table inside server db]

    Returns:
        [table]: [dataframe]
    """
    with open("../src/bin/config.txt") as json_file:
        config = json.load(json_file)
    tables_rts = {}
    for key in list(config['Tables'].keys()):
        tables_rts[key] = config['Tables'][key]['tablename']
    if tableName in tables_rts.values():
        val_list = list(tables_rts.values())
        key_list = list(tables_rts.keys())
        position = val_list.index(tableName)
        for column in config['Tables'][key_list[position]]['columns']:
            table[column][table[column].isnull()] = 0
            table[column] = table[column].astype(config['Tables'][key_list[position]]['columns'][column])
    return table

def open_route_distance_cal(start_point, end_point, region):
    """[calculate distance and time taken from start point and end point on the fly]
    Args:
        start_point ([string]): [station or depot]
        end_point ([string]): [station or depot]

    Returns:
        [Total_Distance]: [distance between station - depot or station to station]
        [Time_taken_hour]: [Time_taken_hour between station - depot or station to station]
    """
    Total_Distance = 0
    Time_taken_hour = 0
    df_cloud_region = pd.DataFrame()
    # print("[OpenRoute API] Reading GPS data from database is being initiated...")
    station_data_gps = InputHandler.get_gps_data(region)
    station_data_gps['ship_to_party'] = station_data_gps['ship_to_party'].astype(int).astype(str)
    # print("[OpenRoute API] Reading GPS data from database has completed...")

    # print("[OpenRoute API] Getting the longitude and latitude for start point & end point is being initiated...")
    longitude_first = station_data_gps['longitude'][station_data_gps['ship_to_party']==start_point].values
    latitude_first = station_data_gps['latitude'][station_data_gps['ship_to_party']==start_point].values
    longitude_second = station_data_gps['longitude'][station_data_gps['ship_to_party']==end_point].values
    latitude_second = station_data_gps['latitude'][station_data_gps['ship_to_party']==end_point].values
    # print("[OpenRoute API] Getting the longitude and latitude for start point & end point haas completed...")

    if (len(longitude_first) == 0): # depot cannot be locate in rts_gps_full_data'
        # print("[OpenRoute API] Getting the longitude and latitude of terminal is being initiated...")
        depot_data_gps = InputHandler.get_terminal_data(region)
        longitude_first = depot_data_gps['longitude'][depot_data_gps['code']==start_point].values
        latitude_first = depot_data_gps['latitude'][depot_data_gps['code']==start_point].values
        # print("[OpenRoute API] Getting the longitude and latitude of terminal has completed...")

    if (len(longitude_second) == 0): # depot cannot be locate in rts_gps_full_data
        # print("[OpenRoute API] Getting the longitude and latitude of terminal is being initiated...")
        depot_data_gps = InputHandler.get_terminal_data(region) 
        longitude_second = depot_data_gps['longitude'][depot_data_gps['code']==end_point].values
        latitude_second = depot_data_gps['latitude'][depot_data_gps['code']==end_point].values
        # print("[OpenRoute API] Getting the longitude and latitude of terminal has completed...")

    df_temp = {'Fuel Acc': [start_point, end_point]}
    df_temp_accName = pd.DataFrame(df_temp, columns = ['Fuel Acc'])
    
    if len(longitude_first) > 0 and len(latitude_first) > 0 and len(longitude_second) > 0 and len(latitude_second) > 0 :
        body = {"locations":[[float(longitude_first), float(latitude_first)], [float(longitude_second), float(latitude_second)]],
                "id":"temp","metrics":["distance"],"resolve_locations":"true","units":"km"}
        headers = {
            'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
            #'Authorization': '5b3ce3597851110001cf6248bcc69e0202de46c9a47e9d02385f2312',
            'Authorization': '5b3ce3597851110001cf6248bde1927f17ee4b23bacb4a1a82517707',
            'Content-Type': 'application/json; charset=utf-8'
        }
        #print("[OpenRoute API] Sending post request to Openroute is being initiated...")
        call = requests.post('https://api.openrouteservice.org/v2/matrix/driving-hgv', json=body, headers=headers)
        if (call.status_code==200):
            print("[OpenRoute API] Openroute API connection successful")
            lists_output = call.json()['distances']
            df_full = pd.DataFrame()
            for i in df_temp_accName['Fuel Acc']:
                for j in df_temp_accName['Fuel Acc']:
                    df = pd.DataFrame()
                    df['First_location'] = [i]
                    df['Next_location'] = [j]
                    df_full = df_full.append(df).reset_index(drop=True)

            df_full['Total_Distance'] = np.resize(lists_output,len(df_full))
            df_full = df_full[df_full['Total_Distance']!=0.0].reset_index(drop=True) 
            df_full['Time_taken_hour']=(pd.to_numeric(df_full['Total_Distance'])/40)
            df_full['Time_taken_hour']=df_full['Time_taken_hour'].round(2)
            df_full['Region'] = region.title()
            DBConnector.save(df_full, Config.TABLES['openroute_distance']["tablename"], if_exists='append')
            print("[OpenRoute API] Distance time matrix is succesfully loaded to DB")
        else:
            print("[OpenRoute API] Openroute API has problem calculating distance, please find the error message: {}, {}".format(call.status_code, call.reason))
            print("[Distance Timematrix] Reading distance time matrix is being initiated...")
            full_dist_matrix = InputHandler.get_dist_backup()
            try :
                Total_Distance = float(full_dist_matrix[(full_dist_matrix['First_location'] == str(start_point)) & (full_dist_matrix['Next_location'] == str(end_point))].Total_Distance)
                Time_taken_hour = float(full_dist_matrix[(full_dist_matrix['First_location'] == str(start_point)) & (full_dist_matrix['Next_location'] == str(end_point))].Time_taken_hour)
            except Exception as e:
                message = f"[Distance Timematrix Error] Distance time matrix has no pre-calulated value for {start_point} and {end_point}."                    
                Total_Distance = 1
                Time_taken_hour = 1
            return Total_Distance, Time_taken_hour
    else :
        Total_Distance = 1
        Time_taken_hour = 1
        return Total_Distance, Time_taken_hour
    return df_full['Total_Distance'][0], df_full['Time_taken_hour'][0]


def change_timedelta_format(time_column):
    time_column = time_column.apply(lambda x: x.seconds)
    time_column = pd.to_datetime(time_column,unit='s').astype(str).apply(lambda x: pd.to_datetime(x).time())
    return time_column


def change_strtodatetime_format(time_column, date):
    time_column = time_column.astype(str).apply(lambda x: datetime.combine(date, pd.to_datetime(x).time()))
    return time_column


def change_datetimetofloat_format(time_column):
    time_column = round(time_column.dt.hour + time_column.dt.minute/60, 2)
    return time_column


def change_retain_format(order, date):
    if 'retain_hour' in order.columns:
        # if all(order.astype(str).query('retain_hour.str.contains(":")')) :
        #     order['retain_hour'] = order['retain_hour'].astype(str).apply(lambda x: x.split(':', 1)[0])
        # else :
        #     pass
        order['retain_hour'] = order['retain_hour'].astype(int)
        order['retain_hour'] = np.where(order['retain_hour'] > 0, order['retain_hour'], 2)
        order['retain_hour'] = np.where(order['retain_hour'] == 24, 23, order['retain_hour'])
        order['retain_hour'] = pd.to_datetime(order['retain_hour'],format='%H')
        order['retain_hour'] = order['retain_hour'].dt.strftime("%Y-%m-%d %H:%M:%S")
        order['retain_hour'] = pd.to_datetime(order['retain_hour'], format='%Y-%m-%d %H:%M:%S')
        order['retain_hour'] = order['retain_hour'] + pd.offsets.DateOffset(year=date.year, day=date.day, month=date.month)
        order['retain_hour'] = order['retain_hour'] + pd.DateOffset(days=1)
        order['retain_hour'] = order['retain_hour'].dt.strftime("%Y-%m-%d %H:%M:%S")
        order['retain_hour'] = pd.to_datetime(order['retain_hour'], format='%Y-%m-%d %H:%M:%S')
    else :
        order['Min_Retain_Hour'] = order['Min_Retain_Hour'].astype(int)
        order['Min_Retain_Hour'] = np.where(order['Min_Retain_Hour'] == 24, 23, order['Min_Retain_Hour'])
        order['Min_Retain_Hour'] = pd.to_datetime(order['Min_Retain_Hour'],format='%H')
        order['Min_Retain_Hour'] = order['Min_Retain_Hour'].dt.strftime("%Y-%m-%d %H:%M:%S")
        order['Min_Retain_Hour'] = pd.to_datetime(order['Min_Retain_Hour'], format='%Y-%m-%d %H:%M:%S')
        order['Min_Retain_Hour'] = order['Min_Retain_Hour'] + pd.offsets.DateOffset(year=date.year, day=date.day, month=date.month)
        order['Min_Retain_Hour'] = order['Min_Retain_Hour'] + pd.DateOffset(days=1)

        # order['Max_Retain_Hour'] = order['Max_Retain_Hour'].astype(int)
        # order['Max_Retain_Hour'] = np.where(order['Max_Retain_Hour'] == 24, 23, order['Max_Retain_Hour'])
        # order['Max_Retain_Hour'] = pd.to_datetime(order['Max_Retain_Hour'],format='%H')
        # order['Max_Retain_Hour'] = order['Max_Retain_Hour'].dt.strftime("%Y-%m-%d %H:%M:%S")
        # order['Max_Retain_Hour'] = pd.to_datetime(order['Max_Retain_Hour'], format='%Y-%m-%d %H:%M:%S')
        # order['Max_Retain_Hour'] = order['Max_Retain_Hour'] + pd.DateOffset(days=1)
        # order['Max_Retain_Hour'] = order['Max_Retain_Hour'].dt.strftime("%Y-%m-%d %H:%M:%S")
        # order['Max_Retain_Hour'] = pd.to_datetime(order['Max_Retain_Hour'], format='%Y-%m-%d %H:%M:%S')
    return order

def convert_time_interval(time_1, time_2, scheduled_orders, date):
    open_list = []
    close_list = []
    for order in scheduled_orders.iterrows():
        open = order[1][time_1]
        hours = int(open)
        minutes = (open*60) % 60
        seconds = (open*3600) % 60
        
        open_time = "%d:%02d:%02d" % (hours, minutes, seconds)
    
        close = order[1][time_2]
        hours = int(close)
        minutes = (close*60) % 60
        seconds = (close*3600) % 60
        
        close_time = "%d:%02d:%02d" % (hours, minutes, seconds)
    
        open_time = pd.to_datetime(date)+ pd.to_timedelta(open_time)
        open_time += timedelta(days=1)

        close_time = pd.to_datetime(date)+ pd.to_timedelta(close_time)
        close_time += timedelta(days=1)
                    
        open_list.append(open_time)
        close_list.append(close_time)
        
    scheduled_orders[time_1] = open_list
    scheduled_orders[time_2] = close_list
    return scheduled_orders 


def forecast_tomorrow_not_opo(forecast_data, station, product):
        #product_id_name = {70100346:'BIODIESEL_B10', 70020771:'PRIMAX_95'}
        try:
            return forecast_data[(forecast_data['id']==station)&(forecast_data['product']==product)]['forecast_sales_d1'].iloc[0]
        except Exception as err :
            return print("NO FORECAST DATA", err, station, product)


def inv_tomorrow(inventory_data_tomorrow, station, product):
        #product_id_name = {70100346:'BIODIESEL_B10', 70020771:'PRIMAX_95'}
        try:
            return inventory_data_tomorrow[(inventory_data_tomorrow['id']==station)&(inventory_data_tomorrow['product']==product)]['opening_inventory_d1'].iloc[0] 
        except Exception as err :
            return print("NO INVENTORY DATA", err, station, product)


def get_alert(End_stock_day, Max_stock_day, Closing_inven):
    """
    Args:
        End_stock_day: End stock of tomorrow
        Max_stock_day: Max stock of tomorrow
        Closing_inven: Closing inventory of tomorrow
    """
    if End_stock_day < 0 :
        return 'OOS'
    elif End_stock_day > Max_stock_day:
        return 'DIVERSION'
    elif 2 < End_stock_day < 2.8: 
        return 'CRITICAL'
    elif End_stock_day <= 2 or Closing_inven <= 5000 :
        return 'LOW-LOW'
    else:
        return 'NORMAL'


def find_retain(final_d, inventory_data_tomorrow, forecast_data, station, product, sales_time_slot, master_all):
    """
    Args:
        forecast: name itself explanatory
        stock: name itself explanatory
    """
    try:
        forecast_tmr = forecast_tomorrow_not_opo(forecast_data, station, product)
    except Exception as err :
        print(err, station, product)
    try:
        inv_tmr = inv_tomorrow(inventory_data_tomorrow, station, product)
    except Exception as err :
        print(err, station, product)

    if product==70020771:
        tank_size = master_all[(master_all['acc']==station)&(master_all['product']==product)]['tank_capacity'].iloc[0]
    elif product==70100346:
        tank_size = master_all[(master_all['acc']==station)&(master_all['product']==product)]['tank_capacity'].iloc[0]
    elif product==70000011:
        tank_size = master_all[(master_all['acc']==station)&(master_all['product']==product)]['tank_capacity'].iloc[0]
    else:
        tank_size = 0
    ullage = 0.876*float(tank_size) - inv_tmr
    df = forecast_tmr * sales_time_slot['perct sales rate'].cumsum()

    if final_d > 0:
        try:
            slot_id = sales_time_slot.iloc[min(df.index[df > (final_d - ullage)])]['End Time']
        except ValueError:
            slot_id = 23
    elif final_d == 0:
        slot_id = 0
    else:
        slot_id = 0

    return slot_id


def calc_run_out_time(station, date, product, forecast_data, end_stock_day, sales_time_slot):
        """Calculate the runout hour and date based on the formula: (Opening Inventory + Proposed Delivery - Forecast - 0.074 *tank size)/forecast

        Args:
            station ([string]): [station id]
            date ([datetime]): [dmr dates]
            product ([string]): [product type]
            forecast_data ([float]): [forecast for tomorrow from DMR]
            end_stock_day ([float]): [end stock day]
            sales_time_slot ([dataframe]): [sales distribution of station]

        Returns:
            runout_time [datetime]: [runout hour]
        """
        runout_date = np.nan
        runout_time = np.nan

        if ~(np.isnan(end_stock_day)):
            runout_date = date + timedelta(days=1) + timedelta(days=floor(end_stock_day))

            try:
                forecast_tmr = forecast_tomorrow_not_opo(forecast_data, station, product)
            except Exception as err :
                print(err, station, product)

            # try:
            #     inv_tmr = inv_tomorrow(inventory_data_tomorrow, station, product)
            # except Exception as err :
            #     print(err, station, product)                

            df = pd.DataFrame()
            df['cumsum_value'] = forecast_tmr * sales_time_slot['perct sales rate'].cumsum()
            df['hour_only'] = sales_time_slot['End Time']

            #percent = ((Open_inven+final_delivery-(math.floor(end_stock_day)*Forecast)-(0.074*tank_size))/Forecast)*100
            percent = float(str(end_stock_day-int(end_stock_day))[1:]) * 100
            # print(f'Runout percentage is {percent}')

            if ((df['cumsum_value'].max()>=percent) and (df['cumsum_value'].min()<=percent)):
                runout_time = runout_date + timedelta(hours = df[(df['cumsum_value']>percent)]['hour_only'][0:1].item())          
            elif df['cumsum_value'].min()>=percent:
                runout_time = runout_date
            else :
                runout_time = runout_date + timedelta(days=1)
        else:
            runout_date = np.nan
            runout_time = np.nan
        
        # self._logger.info(f'Runout_date {runout_date} and runout_time is {runout_time} for Station {station_id} and Product {product}')
        # print(f'Runout_date {runout_date} and runout_time is {runout_time} for Station {station_id} and Product {product}')
        return runout_time


def calculate_tank_status(master_all, forecast_data, station, product):
        """[calculate the tank status based on forecast tomorrow and tank size, different for P95 and Diesel]

        Args:
            station ([String]): Station ID
            product ([String]): [Product Code]

        Returns:
            Tank_Status[String]: [tank status: LV1, LV2, TC, Normal, Error]
        """
        try:
            forecast_tmr = forecast_tomorrow_not_opo(forecast_data, station, product)
        except Exception as err :
            print("FORECAST TMR ERROR", err, station, product)

        if forecast_tmr != None :
            if product==70020771:
                try :
                    tank_size = master_all[(master_all['acc']==station)&(master_all['product']==product)]['tank_capacity'].iloc[0]

                    if 0 < forecast_tmr <= 5000:
                        return "LV1"
                    elif 5000 < forecast_tmr <= 10000 and forecast_tmr/(0.876 * float(tank_size)) < 0.3:
                        return "LV2"
                    elif forecast_tmr/(0.876 * float(tank_size)) > 0.3:
                        return "TC"
                    elif forecast_tmr > 10000 and forecast_tmr/(0.876 * float(tank_size)) <=0.3 :
                        return "Normal"
                    else:
                        return "Error"
                except Exception as err :
                    return "STATION INACTIVE"

            elif product==70100346:
                try:
                    tank_size = master_all[(master_all['acc']==station)&(master_all['product']==product)]['tank_capacity'].iloc[0]

                    if 0 < forecast_tmr <= 5000:
                        return "LV1"
                    elif 5000 < forecast_tmr <= 10000 and forecast_tmr/(0.876 * float(tank_size)) < 0.3:
                        return "LV2"
                    elif forecast_tmr/(0.876 * float(tank_size)) > 0.3:
                        return "TC"
                    elif forecast_tmr > 10000 and forecast_tmr/(0.876 * float(tank_size)) <=0.3 :
                        return "Normal"
                    else:
                        return "Error"
                except Exception as err :
                    return "STATION INACTIVE"                    
            
            elif product==70000011:
                try:
                    tank_size = master_all[(master_all['acc']==station)&(master_all['product']==product)]['tank_capacity'].iloc[0]

                    if 0 < forecast_tmr <= 5000:
                        return "LV1"
                    elif 5000 < forecast_tmr <= 10000 and forecast_tmr/(0.876 * float(tank_size)) < 0.3:
                        return "LV2"
                    elif forecast_tmr/(0.876 * float(tank_size)) > 0.3:
                        return "TC"
                    elif forecast_tmr > 10000 and forecast_tmr/(0.876 * float(tank_size)) <=0.3 :
                        return "Normal"
                    else:
                        return "Error"
                except Exception as err :
                    return "STATION INACTIVE"                      
            else:
                return "PRODUCT NOT RECOGNISED"
        else :
            return "NO FORECAST DATA"


def end_day_stock_1(order, inventory_data_tomorrow, forecast_data):
    try:
        inv_tmr = inv_tomorrow(inventory_data_tomorrow, order['Ship-to'], order['Material'])
    except Exception as err :
        print("INVENTORY ERROR",err, order['Ship-to'], order['Material'])
    try:
        forecast_tmr = forecast_tomorrow_not_opo(forecast_data, order['Ship-to'], order['Material'])
    except Exception as err :
        print("FORECAST ERROR",err, order['Ship-to'], order['Material'])
    try:
        end_stock_day = (inv_tmr + order['Qty'] - forecast_tmr)/forecast_tmr
    except Exception as err :
        return print("END STOCK DAY ERROR",err, order['Ship-to'], order['Material'])

    return round(end_stock_day, 2)


def end_day_stock_2(order, order_change, inventory_data_tomorrow, forecast_data):
    error_forecast_tmr = []
    error_end_stock_day = []
    error_closin_inv = []
    try:
        inv_tmr = inv_tomorrow(inventory_data_tomorrow, order[1]['Ship-to'], order[1]['Material'])
    except Exception as err :
        print(err, order[1]['Ship-to'], order[1]['Material'])
    try:
        forecast_tmr = forecast_tomorrow_not_opo(forecast_data, order[1]['Ship-to'], order[1]['Material'])
    except Exception as err :
        error_forecast_tmr = err
        print(err, order[1]['Ship-to'], order[1]['Material'])
    try:
        end_stock_day = (inv_tmr + order_change - forecast_tmr)/forecast_tmr
    except Exception as err :
        error_end_stock_day = err
        print(err, order[1]['Ship-to'], order[1]['Material'])
    try:
        closin_inv = forecast_tmr * end_stock_day
    except Exception as err :
        error_closin_inv = err
        return print(error_end_stock_day, error_closin_inv, error_forecast_tmr, order[1]['Ship-to'], order[1]['Material'])

    return round(end_stock_day, 2), closin_inv, forecast_tmr


def opo2_check_list(order, order_change, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log):
    
    list_constraints = []
    modification_log = []
    log_msg = None
    log_status = None
    modify_status = None

    if order[1]['Qty'] == order_change:
        modify_status = "Original"
    else :
        modify_status = "Modified"

    try:
        end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_change, inventory_data_tomorrow, forecast_data)
    except Exception as err :
        print(err, order[1]['Ship-to'], order[1]['Material'])
        end_stock_day, closin_inv, forecast_tmr = 0.0, 0.0, 0.0
    
    if order[1]['Material']==70020771: 
        tank_size = master_all[(master_all['acc']==order[1]['Ship-to'])&(master_all['product']==order[1]['Material'])]['tank_capacity'].iloc[0]  
    elif order[1]['Material']==70100346:
        tank_size = master_all[(master_all['acc']==order[1]['Ship-to'])&(master_all['product']==order[1]['Material'])]['tank_capacity'].iloc[0]  
    elif order[1]['Material']==70000011:
        tank_size = master_all[(master_all['acc']==order[1]['Ship-to'])&(master_all['product']==order[1]['Material'])]['tank_capacity'].iloc[0]
    else:
        tank_size = 0
    
    try:
        max_stock_day = round((float(tank_size) * 0.876) / forecast_tmr,2)
    except Exception as err :
        max_stock_day = end_stock_day
    
    alert = get_alert(end_stock_day, max_stock_day, closin_inv)
        
    if end_stock_day >= 1 or order[1]['Qty'] == order_change :
        if order[1]['Qty'] == order_change or end_stock_day <= max_stock_day :
            list_constraints.append(True)
        else :
            list_constraints.append(False)
            if activate_opo2_log:
                log_status = "INVALID"
                log_msg = f"[INVALID] End Stock Day : {end_stock_day} more than Max Stock Day : {max_stock_day}."
                retain_hour = find_retain(order_change, inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
                runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
                modification_log.append([log_status, modify_status, log_msg, "DIVERSION", order[1]['Ship-to'], order[1]['Material'], order[1]['Qty'], order_change, retain_hour, runout_hour,
                                    end_stock_day, order[1]['Cloud2'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], 
                                    order[1]['grouping'], order[1]['order_status']])

        if order[1]['Tank_Status'] == 'TC' and all(list_constraints) :
            if order[1]['Qty'] <= order_change :
                if order[1]['Qty'] == order_change or alert != 'OOS' and alert != 'DIVERSION' :
                    list_constraints.append(True)
                    if activate_opo2_log:
                        log_status = "VALID"
                        log_msg = f"[VALID] TC Alert : {alert}. End Stock Day : {end_stock_day} equal / less than Max Stock Day : {max_stock_day}."
                        retain_hour = find_retain(order_change, inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
                        runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
                        modification_log.append([log_status, modify_status, log_msg, alert, order[1]['Ship-to'], order[1]['Material'], order[1]['Qty'], order_change, retain_hour, runout_hour,
                                        end_stock_day, order[1]['Cloud2'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], 
                                        order[1]['grouping'], order[1]['order_status']])
                else:
                    list_constraints.append(False)
                    if activate_opo2_log:
                        log_status = "INVALID"
                        log_msg = f"[INVALID] TC Alert : {alert}. End Stock Day : {end_stock_day} equal / less than Max Stock Day : {max_stock_day}."
                        retain_hour = find_retain(order_change, inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
                        runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
                        modification_log.append([log_status, modify_status, log_msg, alert, order[1]['Ship-to'], order[1]['Material'], order[1]['Qty'], order_change, retain_hour, runout_hour,
                                        end_stock_day, order[1]['Cloud2'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], 
                                        order[1]['grouping'], order[1]['order_status']])
            else :
                list_constraints.append(False)
                if activate_opo2_log:
                    log_status = "INVALID"
                    ori_quantity = order[1]['Qty']
                    log_msg = f"[INVALID] TC - OPO2 Quantity : {order_change} less than DMR original Quantity : {ori_quantity}."
                    retain_hour = find_retain(order_change, inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
                    runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
                    modification_log.append([log_status, modify_status, log_msg, alert, order[1]['Ship-to'], order[1]['Material'], order[1]['Qty'], order_change, retain_hour, runout_hour,
                                    end_stock_day, order[1]['Cloud2'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], 
                                    order[1]['grouping'], order[1]['order_status']])

        elif all(list_constraints):
            if order[1]['Qty'] == order_change or alert == 'NORMAL':
                list_constraints.append(True)
                if activate_opo2_log:
                    log_status = "VALID"
                    log_msg = f"[VALID] Non-TC Alert : {alert}. End Stock Day : {end_stock_day} equal / less than Max Stock Day : {max_stock_day}."
                    retain_hour = find_retain(order_change, inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
                    runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
                    modification_log.append([log_status, modify_status, log_msg, alert, order[1]['Ship-to'], order[1]['Material'], order[1]['Qty'], order_change, retain_hour, runout_hour,
                                    end_stock_day, order[1]['Cloud2'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], 
                                    order[1]['grouping'], order[1]['order_status']])
            else:
                list_constraints.append(False)
                if activate_opo2_log:
                    log_status = "INVALID"
                    log_msg = f"[INVALID] Non-TC Alert : {alert}. End Stock Day : {end_stock_day} equal / less than Max Stock Day : {max_stock_day}."
                    retain_hour = find_retain(order_change, inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
                    runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
                    modification_log.append([log_status, modify_status, log_msg, alert, order[1]['Ship-to'], order[1]['Material'], order[1]['Qty'], order_change, retain_hour, runout_hour,
                                    end_stock_day, order[1]['Cloud2'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], 
                                    order[1]['grouping'], order[1]['order_status']])           
        else :
            pass
    
    else :
        list_constraints.append(False)

        if activate_opo2_log:
            log_status = "INVALID"
            log_msg = f"[INVALID] End Stock Day : {end_stock_day} less than 1 day."
            retain_hour = find_retain(order_change, inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
            runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
            modification_log.append([log_status, modify_status, log_msg, alert, order[1]['Ship-to'], order[1]['Material'], order[1]['Qty'], order_change, retain_hour, runout_hour,
                                end_stock_day, order[1]['Cloud2'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], 
                                order[1]['grouping'], order[1]['order_status']])

    if activate_opo2_log:
        cols = ['Valid_Status','Modify_Status','Log','Alert','Destinations','Product','Old_Order','New_order','New_Retain', 'New_Retain', 'New_EndStockDay','Cloud','Accessibility','Tank_Status','long_distance','Multiload_Status','condition','grouping','order_status']
        modification_DT = pd.DataFrame(modification_log, columns=cols)
        modification_DT['DMR_Date'] = date
        modification_DT['Loop_Counter'] = loop_status
        modification_DT['Job_StartTime'] = start_time
        modification_DT['Txn_ID'] = txn_id

        try :        
            DBConnector.save(modification_DT, Config.TABLES['opo2_log']["tablename"], if_exists='append')
        except Exception as err :
            print("[OPO2] OPO2 Logs for {} in {} failed to push to DB due to {}.".format(loop_status, txn_id, err))

    return all(list_constraints)


# def order_permutation(order_data, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log):

#     cols = ['Ship-to', 'Material', 'Qty', 'retain_hour', 'Runout','Total_weight', 'End_Stock_Day', 'acc','Cloud2','distance_terminal_u95','accessibility','Tank_Status','long_distance','Multiload_Status','condition','grouping', 'order_status', 'ori_status']
#     modify_order = []
#     for order in order_data.iterrows(): 
#         order_quants = [order[1]['Qty']+i*5460 for i in range(-2,3) if (order[1]['Qty']+(i*5460) > 0 and order[1]['Qty']+i*5460 < 54600 and opo2_check_list(order, order[1]['Qty']+i*5460, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log))]
#         for i in range(len(order_quants)):
#             total_weight = order_quants[i] * order[1]['Density_values']
#             end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_quants[i], inventory_data_tomorrow, forecast_data)
#             retain_hour = find_retain(order_quants[i], inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
#             if order_quants[i] == order[1]['Qty'] :
#                 modify_order.append([order[1]['Ship-to'], order[1]['Material'],order_quants[i], retain_hour, order[1]['Runout'],total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'original'])
#             else :
#                 modify_order.append([order[1]['Ship-to'], order[1]['Material'], order_quants[i], retain_hour, order[1]['Runout'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'modified'])
#     new_order_perm = pd.DataFrame(modify_order, columns=cols)
#     return new_order_perm


def order_permutation_central(order_data, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log, region):
    
    cols = Config.MODEL_INPUTOUTPUT['order_process_subset'][region]['second_loop_opo2']
    modify_order = []
    order_data['long_distance'] = "non_LD"
    
    for order in order_data.iterrows(): 
        order_quants = [order[1]['Qty']+i*5460 for i in range(-2,3) if (order[1]['Qty']+(i*5460) > 0 and order[1]['Qty']+i*5460 < 54600 and opo2_check_list(order, order[1]['Qty']+i*5460, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log))]
        for i in range(len(order_quants)):
            total_weight = order_quants[i] * order[1]['Density_values']
            end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_quants[i], inventory_data_tomorrow, forecast_data)
            retain_hour = find_retain(order_quants[i], inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
            runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
            if order_quants[i] == order[1]['Qty'] :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'original'])
            else :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'opo2_modified'])
    new_order_perm = pd.DataFrame(modify_order, columns=cols)
    return new_order_perm


def order_permutation_southern(order_data, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log, region):
    
    cols = Config.MODEL_INPUTOUTPUT['order_process_subset'][region]['second_loop_opo2']
    modify_order = []
    
    for order in order_data.iterrows(): 
        order_quants = [order[1]['Qty']+i*5460 for i in range(-2,3) if (order[1]['Qty']+(i*5460) > 0 and order[1]['Qty']+i*5460 < 54600 and opo2_check_list(order, order[1]['Qty']+i*5460, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log))]
        for i in range(len(order_quants)):
            total_weight = order_quants[i] * order[1]['Density_values']
            end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_quants[i], inventory_data_tomorrow, forecast_data)
            retain_hour = find_retain(order_quants[i], inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
            runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
            if order_quants[i] == order[1]['Qty'] :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'original'])
            else :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'opo2_modified'])
    new_order_perm = pd.DataFrame(modify_order, columns=cols)
    return new_order_perm


def order_permutation_eastern(order_data, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log, region):
    
    cols = Config.MODEL_INPUTOUTPUT['order_process_subset'][region]['second_loop_opo2']
    modify_order = []
    
    for order in order_data.iterrows(): 
        order_quants = [order[1]['Qty']+i*5460 for i in range(-2,3) if (order[1]['Qty']+(i*5460) > 0 and order[1]['Qty']+i*5460 < 54600 and opo2_check_list(order, order[1]['Qty']+i*5460, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log))]
        for i in range(len(order_quants)):
            total_weight = order_quants[i] * order[1]['Density_values']
            end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_quants[i], inventory_data_tomorrow, forecast_data)
            retain_hour = find_retain(order_quants[i], inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
            runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
        for i in range(len(order_quants)):
            total_weight = order_quants[i] * order[1]['Density_values']
            end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_quants[i], inventory_data_tomorrow, forecast_data)
            retain_hour = find_retain(order_quants[i], inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
            runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
            if order_quants[i] == order[1]['Qty'] :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order[1]['terminal'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'original'])
            else :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order[1]['terminal'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'opo2_modified'])
    new_order_perm = pd.DataFrame(modify_order, columns=cols)
    return new_order_perm


def order_permutation_northern(order_data, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log, region):
    
    cols = Config.MODEL_INPUTOUTPUT['order_process_subset'][region]['second_loop_opo2']
    modify_order = []
    
    for order in order_data.iterrows(): 
        order_quants = [order[1]['Qty']+i*5460 for i in range(-2,3) if (order[1]['Qty']+(i*5460) > 0 and order[1]['Qty']+i*5460 < 54600 and opo2_check_list(order, order[1]['Qty']+i*5460, inventory_data_tomorrow, forecast_data, sales_time_slot, master_all, date, loop_status, start_time, txn_id, activate_opo2_log))]
        for i in range(len(order_quants)):
            total_weight = order_quants[i] * order[1]['Density_values']
            end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_quants[i], inventory_data_tomorrow, forecast_data)
            retain_hour = find_retain(order_quants[i], inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
            runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
        for i in range(len(order_quants)):
            total_weight = order_quants[i] * order[1]['Density_values']
            end_stock_day, closin_inv, forecast_tmr = end_day_stock_2(order, order_quants[i], inventory_data_tomorrow, forecast_data)
            retain_hour = find_retain(order_quants[i], inventory_data_tomorrow, forecast_data, order[1]['Ship-to'], order[1]['Material'], sales_time_slot, master_all)
            runout_hour = calc_run_out_time(order[1]['Ship-to'], date, order[1]['Material'], forecast_data, end_stock_day, sales_time_slot)
            if order_quants[i] == order[1]['Qty'] :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order[1]['terminal'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'original'])
            else :
                modify_order.append([order[1]['Ship-to'], order[1]['Priority'], order[1]['Material'], order[1]['terminal'], order_quants[i], retain_hour, runout_hour, order[1]['Opening_Time'], order[1]['Closing_Time'], total_weight, end_stock_day, order[1]['acc'], order[1]['Cloud2'], order[1]['distance_terminal_u95'], order[1]['accessibility'], order[1]['Tank_Status'], order[1]['long_distance'], order[1]['Multiload_Status'], order[1]['condition'], order[1]['grouping'], order[1]['order_status'], 'opo2_modified'])
    new_order_perm = pd.DataFrame(modify_order, columns=cols)
    return new_order_perm

