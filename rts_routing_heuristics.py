from numpy import meshgrid
#from pyomo.core.base.plugin import pyomo_callback
from src.rts_routing_modules.rts_routing_baseobjects import RouteGenerator
#from input_generation import shift_start, shift_end
import random
import sys, os
from src.rts_routing_modules.rts_routing_utilities import printAllSubsets, schedule_weighted_intervals, Interval
from collections import OrderedDict
from copy import deepcopy
import time
import dask
from dask import delayed
from itertools import chain, repeat, count, islice, combinations
from collections import Counter

os.environ["PYDEVD_USE_FRAME_EVAL"] = "NO"

def repeat_chain(values, counts):
    return chain.from_iterable(map(repeat, values, counts))


def unique_combinations_from_value_counts(values, counts, r):
    n = len(counts)
    indices = list(islice(repeat_chain(count(), counts), r))
    if len(indices) < r:
        return
    while True:
        yield tuple(values[i] for i in indices)
        for i, j in zip(reversed(range(r)), repeat_chain(reversed(range(n)), reversed(counts))):
            if indices[i] != j:
                break
        else:
            return
        j = indices[i] + 1
        for i, j in zip(range(i, r), repeat_chain(count(j), counts[j:])):
            indices[i] = j    


def unique_combinations(iterable, r):
    values, counts = zip(*Counter(iterable).items())
    return unique_combinations_from_value_counts(values, counts, r)


def route_generate_central(orders_dict, resource_dict, date_time, multiload_combi, cloud, region, opo2_first, max_stops=[2,1]):
    print("CLOUD IS " + str(cloud))
    orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True} 
    resource_dict = {k: v for k, v in resource_dict.items() if resource_dict[k].min_remaining_shift > 0}
    resource_quant = {resource_dict[k].capacity: v for k, v in resource_dict.items()}
    resource_type = sorted(resource_quant.keys(), reverse=True)
    result_rg = []
    unmatched_rg = []
    station_list_set = set([k[1].identifier for k in orders_dict.items()])
    station_list = list(station_list_set)

    # def checkIfDuplicates_1(listOfElems):
    #     ''' Check if given list contains any duplicates '''
    #     if len(listOfElems) == len(set(listOfElems)):
    #         return False
    #     else:
    #         return True

    # result = checkIfDuplicates_1(station_list)
    # if result:
    #     print('Yes, list contains duplicates')
    # else:
    #     print('No duplicates found in list')  

    for res_type in resource_type:
        on1_on2_tanker = False
        res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "Double"}
        if len(res_dict_type) > 0 :
            choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
            min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift 
        else :
            res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "OH"}
            if len(res_dict_type) > 0 : 
                choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time           
                min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift
            else :
                res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and (resource_dict[k].shift_type == "On1" or resource_dict[k].shift_type == "On2")}
                if len(res_dict_type) > 0 : 
                    on1_on2_tanker = True
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_shift_start = res_dict_type[min(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_start))].shift_start
                    max_shift_end = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end))].shift_end
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift
                else :
                    res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type}                    
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift
                
        # choice2 = random.choice(list(res_dict_type.keys()))
        for m in max_stops:
            perm = unique_combinations(station_list, m)
            # perm = set(combinations(station_list, m))
            for i in list(perm):
                multiload_status = []
                ori_status = []
                drop_1 = {}
                drop_2 = {}

                if len(i) > 1 and (i[0].split('_')[0] != i[1].split('_')[0]):
                    orders =[]
                    drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                    drop_2 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[1]}
    
                    multiload_status.append(list(drop_1.values())[0].multiload_status)
                    multiload_status.append(list(drop_2.values())[0].multiload_status)

                    if opo2_first == True :
                        ori_status.append(list(drop_1.values())[0].ori_status)
                        ori_status.append(list(drop_2.values())[0].ori_status)

                    if opo2_first == True and len(ori_status) > 0 and max(ori_status, key=len) == "original" :
                        orders =[]

                    elif multiload_combi != 'Later':
                        orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                        orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))

                    elif multiload_combi == 'Later' and len(set(multiload_status)) > 1 :
                        orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                        orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))

                    elif multiload_combi == 'Later' and len(set(multiload_status)) < 2 and max(multiload_status, key=len) != "Normal":
                        orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                        orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))

                    else :
                        orders = []
                    
                    if len(orders) >0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) == res_type :
                        if on1_on2_tanker :
                            rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time,
                                            depot_close_time = resource_dict[choice2].depot_close_time)
                        else :
                            rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time,
                                            depot_close_time = resource_dict[choice2].depot_close_time)
                        
                        if rg.can_fit_list(orders, region, False) :
                            result_rg.append(rg)
                        else :
                            unmatched_rg.append(rg)

                    elif len(orders) >0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) != res_type :
                        rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                           resource_dict[choice2].sub_resource,
                                           resource_dict[choice2].sub_resource_quantity,
                                           resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                           resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time,
                                           depot_close_time = resource_dict[choice2].depot_close_time)
                        if rg.can_fit_list(orders, region, True) == False :
                            unmatched_rg.append(rg)

                    else :
                        pass

                elif len(i) == 1:
                    orders =[]
                    drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                    for key in range(len(drop_1)):

                        if opo2_first == True and list(drop_1.values())[0].ori_status == "original" :
                            orders = []
                        elif multiload_combi != 'Later':
                            orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                        elif multiload_combi == 'Later' and list(drop_1.values())[0].multiload_status != "Normal" :
                            orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                        else :
                            orders = []
                            
                        if len(orders) >0 and int(list(drop_1.values())[key].quantity) == res_type :
                            if on1_on2_tanker :
                                rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                                resource_dict[choice2].sub_resource,
                                                resource_dict[choice2].sub_resource_quantity,
                                                resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                                min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time,
                                                depot_close_time = resource_dict[choice2].depot_close_time)
                            else :
                                rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                                resource_dict[choice2].sub_resource,
                                                resource_dict[choice2].sub_resource_quantity,
                                                resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                                resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time,
                                                depot_close_time = resource_dict[choice2].depot_close_time)
                            
                            if rg.can_fit_list(orders, region, False) :
                                result_rg.append(rg)
                            else :
                                unmatched_rg.append(rg)

                        elif len(orders) >0 and int(list(drop_1.values())[key].quantity) != res_type :
                            rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time,
                                            depot_close_time = resource_dict[choice2].depot_close_time)
                            if rg.can_fit_list(orders, region, True) == False :
                                unmatched_rg.append(rg)

                        else :
                            pass
                
                else :
                    pass

    return {cloud: {'cloud_wise_route': result_rg, 'cloud_wise_unmatched': unmatched_rg}}


def route_generate_southern(orders_dict, resource_dict, date_time, multiload_combi, cloud, region, opo2_first, max_stops=[2,1]):
    print("CLOUD IS", cloud)
    orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
    resource_dict = {k: v for k, v in resource_dict.items() if resource_dict[k].min_remaining_shift > 0}
    resource_quant = {resource_dict[k].capacity: v for k, v in resource_dict.items()}
    resource_type = sorted(resource_quant.keys(), reverse=True)
    result_rg = []
    unmatched_rg = []
    orders_dict_LD = {k: v for k, v in orders_dict.items() if "LD" in orders_dict[k].grouping}
    orders_dict_nonLD = {k: v for k, v in orders_dict.items() if "LD" not in orders_dict[k].grouping}
    LD_nonLD_orders_dict = [orders_dict_LD, orders_dict_nonLD]
    LD_nonLD_orders_dict = [order for order in LD_nonLD_orders_dict if order] # Filtering out empty orders_dict
    station_list_LD = [k[1].identifier for k in orders_dict_LD.items()]
    station_list_nonLD = [k[1].identifier for k in orders_dict_nonLD.items()]

    for res_type in resource_type:
        on1_on2_tanker = False
        res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "Double"}
        if len(res_dict_type) > 0 :
            choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
            min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift 
        else :
            res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "OH"}
            if len(res_dict_type) > 0 : 
                choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time           
                min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift
            else :
                res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and (resource_dict[k].shift_type == "On1" or resource_dict[k].shift_type == "On2")}
                if len(res_dict_type) > 0 : 
                    on1_on2_tanker = True
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_shift_start = res_dict_type[min(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_start))].shift_start
                    max_shift_end = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end))].shift_end
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift
                else :
                    res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type}                    
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift

        for order_dict in LD_nonLD_orders_dict :
            try :
                LD_tag = order_dict[min(order_dict.keys(), key=(lambda k: order_dict[k].grouping))].grouping
            except Exception as e:
                LD_tag = [order_dict[k].grouping for k in order_dict.keys()]
            
            if "LD" in LD_tag:
                station_list = station_list_LD.copy()
                max_stops = [3,2,1]
            else :
                station_list = station_list_nonLD.copy()
                max_stops = [2,1]

            for m in max_stops:
                perm = combinations(station_list, m)
                for i in set(perm):
                    multiload_status = []
                    ori_status = []

                    # If the route has combinations are three drops, make sure each drop is of unique destinations 
                    if len(i) > 2 and i[0].split('_')[0] != i[1].split('_')[0] and i[0].split('_')[0] != i[2].split('_')[0] and i[1].split('_')[0] != i[2].split('_')[0]:
                        orders =[]
                        u97 = []
                        non_u97 = []
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        drop_2 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[1]}
                        drop_3 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[2]}
                        
                        # Collect the supplying terminal of the destination, based on products
                        if list(drop_1.values())[0].product_type == 70000011:
                            u97.append(list(drop_1.values())[0].terminal_97)
                        else :
                            non_u97.append(list(drop_1.values())[0].terminal_95)
                        if list(drop_2.values())[0].product_type == 70000011:
                            u97.append(list(drop_2.values())[0].terminal_97)
                        else :
                            non_u97.append(list(drop_2.values())[0].terminal_95)
                        if list(drop_3.values())[0].product_type == 70000011:
                            u97.append(list(drop_3.values())[0].terminal_97)
                        else :
                            non_u97.append(list(drop_3.values())[0].terminal_95)
                            
                        # Check if the route has only one terminal serving all destinations 
                        if len(set([item for sublist in [u97,non_u97] for item in sublist])) < 2 :
                            multiload_status.append(list(drop_1.values())[0].multiload_status)
                            multiload_status.append(list(drop_2.values())[0].multiload_status)
                            multiload_status.append(list(drop_3.values())[0].multiload_status)
                            
                            if opo2_first == True :
                                ori_status.append(list(drop_1.values())[0].ori_status)
                                ori_status.append(list(drop_2.values())[0].ori_status)
                                ori_status.append(list(drop_3.values())[0].ori_status)                             
                            
                            if opo2_first == True and len(ori_status) > 0 and max(ori_status, key=len) == "original" :
                                orders =[]

                            # Accept combination of normal + multiload (evening load) only in "Later" loop to avoid duplicate normal + normal orders with "Early" loop
                            elif multiload_combi != 'Later':
                                orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                                orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                                orders.append((list(drop_3.keys())[0], list(drop_3.values())[0]))
                            elif multiload_combi == 'Later' and len(set(multiload_status)) > 1 :
                                orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                                orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                                orders.append((list(drop_3.keys())[0], list(drop_3.values())[0]))
                            elif multiload_combi == 'Later' and len(set(multiload_status)) < 2 and list(set(multiload_status))[0] != "Normal":
                                orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                                orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                                orders.append((list(drop_3.keys())[0], list(drop_3.values())[0]))
                            else :
                                orders =[]

                            if len(orders) > 0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity + list(drop_3.values())[0].quantity) == res_type :
                                if on1_on2_tanker :
                                    rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, 
                                            depot_close_time = resource_dict[choice2].depot_close_time)
                                else :
                                    rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                            resource_dict[choice2].depot_start_time, date_time, 
                                            depot_close_time = resource_dict[choice2].depot_close_time)                            
                            
                                if rg.can_fit_list(orders, region, False) :
                                    result_rg.append(rg)
                                else :
                                    unmatched_rg.append(rg)
                            
                            elif len(orders) > 0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity + list(drop_3.values())[0].quantity) != res_type :
                                rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                        resource_dict[choice2].sub_resource,
                                        resource_dict[choice2].sub_resource_quantity,
                                        resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                        resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                        resource_dict[choice2].depot_start_time, date_time, 
                                        depot_close_time = resource_dict[choice2].depot_close_time)                            
                                
                                if rg.can_fit_list(orders, region, True) == False :
                                    unmatched_rg.append(rg)
                            
                            else :
                                pass

                        else :
                            rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                    resource_dict[choice2].sub_resource,
                                    resource_dict[choice2].sub_resource_quantity,
                                    resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                    resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                    resource_dict[choice2].depot_start_time, date_time, 
                                    depot_close_time = resource_dict[choice2].depot_close_time)   

                            if rg.can_fit_list(orders, region, False, True) == False :
                                unmatched_rg.append(rg)

                    if len(i) < 3 and len(i) > 1 and (i[0].split('_')[0] != i[1].split('_')[0]):
                        orders =[]
                        u97 = []
                        non_u97 = []
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        drop_2 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[1]}

                        if list(drop_1.values())[0].product_type == 70000011:
                            u97.append(list(drop_1.values())[0].terminal_97)
                        else :
                            non_u97.append(list(drop_1.values())[0].terminal_95)
                        if list(drop_2.values())[0].product_type == 70000011:
                            u97.append(list(drop_2.values())[0].terminal_97)
                        else :
                            non_u97.append(list(drop_2.values())[0].terminal_95)

                        if len(set([item for sublist in [u97,non_u97] for item in sublist])) < 2 :
                            #print(int(list(drop_1.values())[0].quantity), int(list(drop_2.values())[0].quantity), int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity), res_type)
                            multiload_status.append(list(drop_1.values())[0].multiload_status)
                            multiload_status.append(list(drop_2.values())[0].multiload_status)

                            if opo2_first == True :
                                ori_status.append(list(drop_1.values())[0].ori_status)
                                ori_status.append(list(drop_2.values())[0].ori_status)
                            
                            if opo2_first == True and len(ori_status) > 0 and max(ori_status, key=len) == "original" :
                                orders =[]                            

                            elif multiload_combi != 'Later':
                                orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                                orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                            elif multiload_combi == 'Later' and len(set(multiload_status)) > 1 :
                                orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                                orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                            elif multiload_combi == 'Later' and len(set(multiload_status)) < 2 and list(set(multiload_status))[0] != "Normal":
                                orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                                orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                            else :
                                orders =[]

                            if len(orders) >0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) == res_type :
                                if on1_on2_tanker :
                                    rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, 
                                            depot_close_time = resource_dict[choice2].depot_close_time)
                                else :
                                    rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                            resource_dict[choice2].depot_start_time, date_time, 
                                            depot_close_time = resource_dict[choice2].depot_close_time) 

                                if rg.can_fit_list(orders, region, False) :
                                    result_rg.append(rg)
                                else :
                                    unmatched_rg.append(rg)
                            
                            elif len(orders)>0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) != res_type :
                                rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                        resource_dict[choice2].sub_resource,
                                        resource_dict[choice2].sub_resource_quantity,
                                        resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                        resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                        resource_dict[choice2].depot_start_time, date_time, 
                                        depot_close_time = resource_dict[choice2].depot_close_time)

                                if rg.can_fit_list(orders, region, True) == False :
                                    unmatched_rg.append(rg)
                            
                            else :
                                pass

                        else :
                            rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                    resource_dict[choice2].sub_resource,
                                    resource_dict[choice2].sub_resource_quantity,
                                    resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                    resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                    resource_dict[choice2].depot_start_time, date_time, 
                                    depot_close_time = resource_dict[choice2].depot_close_time)

                            if rg.can_fit_list(orders, region, False, True) == False :
                                unmatched_rg.append(rg)

                    if len(i) == 1:
                        orders =[]
                        
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}

                        for key in range(len(drop_1)):
                            if opo2_first == True and list(drop_1.values())[0].ori_status == "original" :
                                orders = []                            
                            elif multiload_combi != 'Later':
                                orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                            elif multiload_combi == 'Later' and list(drop_1.values())[0].multiload_status != "Normal" :
                                orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                            else :
                                orders =[]


                            if len(orders) >0 and int(list(drop_1.values())[key].quantity) == res_type :
                                if on1_on2_tanker :
                                    rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, 
                                            depot_close_time = resource_dict[choice2].depot_close_time)
                                else :
                                    rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                            resource_dict[choice2].sub_resource,
                                            resource_dict[choice2].sub_resource_quantity,
                                            resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                            resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                            resource_dict[choice2].depot_start_time, date_time, 
                                            depot_close_time = resource_dict[choice2].depot_close_time) 
                                
                                if rg.can_fit_list(orders, region, False) :
                                    result_rg.append(rg)
                                else :
                                    unmatched_rg.append(rg)
                            
                            elif len(orders) >0 and int(list(drop_1.values())[key].quantity) != res_type :
                                rg = RouteGenerator(resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, 
                                        resource_dict[choice2].sub_resource,
                                        resource_dict[choice2].sub_resource_quantity,
                                        resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight,
                                        resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, 
                                        resource_dict[choice2].depot_start_time, date_time, 
                                        depot_close_time = resource_dict[choice2].depot_close_time) 
                                
                                if rg.can_fit_list(orders, region, True) == False :
                                    unmatched_rg.append(rg)
                            else :
                                pass

    return {cloud: {'cloud_wise_route': result_rg, 'cloud_wise_unmatched': unmatched_rg}}


def route_generate_eastern(orders_dict, resource_dict, date_time, multiload_combi, cloud, region, opo2_first, max_stops=[2,1]):
    print("CLOUD IS", cloud)
    orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
    resource_dict = {k: v for k, v in resource_dict.items() if resource_dict[k].min_remaining_shift > 0}
    resource_quant = {resource_dict[k].capacity: v for k, v in resource_dict.items()}
    resource_type = sorted(resource_quant.keys(), reverse=True)
    result_rg = []
    unmatched_rg = []
    orders_dict_LD = {k: v for k, v in orders_dict.items() if "LD" in orders_dict[k].grouping}
    orders_dict_nonLD = {k: v for k, v in orders_dict.items() if "LD" not in orders_dict[k].grouping}
    full_orders_dict = [orders_dict_LD, orders_dict_nonLD]
    full_orders_dict = [order for order in full_orders_dict if order] # Filtering out empty orders_dict
    station_list_LD = [k[1].identifier for k in orders_dict_LD.items()]
    station_list_nonLD = [k[1].identifier for k in orders_dict_nonLD.items()]

    for res_type in resource_type:
        on1_on2_tanker = False
        # Extracting RTs with maximum shift and matching capacity
        res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "Double"}
        if len(res_dict_type) > 0 :
            choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
            min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift   
        else :
            res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "OH"}
            if len(res_dict_type) > 0 : 
                choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift           
            else :
                res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and (resource_dict[k].shift_type == "On1" or resource_dict[k].shift_type == "On2")}
                if len(res_dict_type) > 0 : 
                    on1_on2_tanker = True
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_shift_start = res_dict_type[min(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_start))].shift_start
                    max_shift_end = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end))].shift_end
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift
                else :
                    res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type}  
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift

        for order_dict in full_orders_dict:
            try:
                group_tag = order_dict[min(order_dict.keys(), key=(lambda k: order_dict[k].grouping))].grouping
            except Exception as e:
                group_tag = [order_dict[k].grouping for k in order_dict.keys()]

            if "LD" in group_tag:
                station_list = station_list_LD.copy()
                stops = min(3, len(station_list))
                max_stops = [x for x in range(stops, 0, -1)] # [3, 2, 1]
            else:
                station_list = station_list_nonLD.copy()
                stops = min(2, len(station_list))
                max_stops = [x for x in range(stops, 0, -1)] # [2, 1]

            for m in max_stops:
                perm = combinations(station_list, m)
                for i in set(perm):
                    multiload_status = []
                    ori_status = []

                    # If the route has combinations are three drops, make sure each drop is of unique destinations 
                    if len(i) > 2 and i[0].split('_')[0] != i[1].split('_')[0] and i[0].split('_')[0] != i[2].split('_')[0] and i[1].split('_')[0] != i[2].split('_')[0]:
                        orders =[]
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        drop_2 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[1]}
                        drop_3 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[2]}

                        # TESTING REQUIRED!!! When do we add ori_status value?
                        if opo2_first == True :
                            ori_status.append(list(drop_1.values())[0].ori_status)
                            ori_status.append(list(drop_2.values())[0].ori_status)
                            ori_status.append(list(drop_3.values())[0].ori_status)

                        # TESTING REQUIRED!!! When do we add ori_status value?
                        if opo2_first == True and len(ori_status) > 0 and max(ori_status, key=len) == "original" :
                            orders =[]
                        else:
                            orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                            orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                            orders.append((list(drop_3.keys())[0], list(drop_3.values())[0]))

                        # Resulting in a route up to m drops
                        if len(orders) > 0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity + list(drop_3.values())[0].quantity) == res_type:
                            if on1_on2_tanker:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            else :
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                            if rg.can_fit_list(orders, region, False, False):
                                result_rg.append(rg)
                            else:
                                unmatched_rg.append(rg)

                        elif len(orders) > 0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity + list(drop_3.values())[0].quantity) != res_type:
                            rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            if rg.can_fit_list(orders, region, True, False) == False:
                                unmatched_rg.append(rg)
                        else:
                            # Currently only in Eastern Routing
                            rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            if rg.can_fit_list(orders, region, False, True) == False:
                                unmatched_rg.append(rg)

                    elif len(i) == 2 and (i[0].split('_')[0] != i[1].split('_')[0]):
                        orders =[]
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        drop_2 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[1]}

                        if opo2_first == True :
                            ori_status.append(list(drop_1.values())[0].ori_status)
                            ori_status.append(list(drop_2.values())[0].ori_status)

                        # TESTING REQUIRED!!! When do we add ori_status value?
                        if opo2_first == True and len(ori_status) > 0 and max(ori_status, key=len) == "original" :
                            orders =[]
                        else:
                            # TODO: DOUBLE CHECK meaning and usage of ori_status
                            orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                            orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))

                        if len(orders) >0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) == res_type:
                            if on1_on2_tanker:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            else:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,resource_dict[choice2].sub_resource,resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                            if rg.can_fit_list(orders, region, False, False):
                                result_rg.append(rg)
                            else:
                                unmatched_rg.append(rg)

                        elif len(orders)>0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) != res_type:
                            rg = RouteGenerator(
                                resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                            if rg.can_fit_list(orders, region, True, False) == False:
                                unmatched_rg.append(rg)
                            else:
                                pass

                        else:
                            rg = RouteGenerator(
                                resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            if rg.can_fit_list(orders, region, False, True) == False:
                                unmatched_rg.append(rg)

                    elif len(i) == 1:
                        orders =[]
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        for key in range(len(drop_1)):

                            if opo2_first == True and list(drop_1.values())[0].ori_status == "original" :
                                orders =[]                            
                            elif multiload_combi != 'Later':
                                orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                            elif multiload_combi == 'Later' and list(drop_1.values())[0].multiload_status != "Normal":
                                orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                            else:
                                orders =[]

                            if len(orders) >0 and int(list(drop_1.values())[key].quantity) == res_type:
                                if on1_on2_tanker :
                                    rg = RouteGenerator(
                                        resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                                else:
                                    rg = RouteGenerator(
                                        resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                                if rg.can_fit_list(orders, region, False, False):
                                    result_rg.append(rg)
                                else:
                                    unmatched_rg.append(rg)

                            elif len(orders) >0 and int(list(drop_1.values())[key].quantity) != res_type:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time) 

                                if rg.can_fit_list(orders, region, True, False) == False:
                                    unmatched_rg.append(rg)
                            else :
                                pass
                    else:
                        pass

    return {cloud: {'cloud_wise_route': result_rg, 'cloud_wise_unmatched': unmatched_rg}}

def route_generate_northern(orders_dict, resource_dict, date_time, multiload_combi, cloud, region, opo2_first, max_stops=[2,1]):
    print("CLOUD IS", cloud)
    orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
    resource_dict = {k: v for k, v in resource_dict.items() if resource_dict[k].min_remaining_shift > 0}
    resource_quant = {resource_dict[k].capacity: v for k, v in resource_dict.items()}
    resource_type = sorted(resource_quant.keys(), reverse=True)
    result_rg = []
    unmatched_rg = []
    orders_dict_LD = {k: v for k, v in orders_dict.items() if "LD" in orders_dict[k].grouping}
    orders_dict_nonLD = {k: v for k, v in orders_dict.items() if "LD" not in orders_dict[k].grouping}
    full_orders_dict = [orders_dict_LD, orders_dict_nonLD]
    full_orders_dict = [order for order in full_orders_dict if order] # Filtering out empty orders_dict
    station_list_LD = [k[1].identifier for k in orders_dict_LD.items()]
    station_list_nonLD = [k[1].identifier for k in orders_dict_nonLD.items()]


    for res_type in resource_type:
        on1_on2_tanker = False
        # Extracting RTs with maximum shift and matching capacity
        res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "Double"}
        if len(res_dict_type) > 0 :
            choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
            min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift   
        else :
            res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and resource_dict[k].shift_type == "OH"}
            if len(res_dict_type) > 0 : 
                choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift           
            else :
                res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type and (resource_dict[k].shift_type == "On1" or resource_dict[k].shift_type == "On2")}
                if len(res_dict_type) > 0 : 
                    on1_on2_tanker = True
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_shift_start = res_dict_type[min(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_start))].shift_start
                    max_shift_end = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end))].shift_end
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift
                else :
                    res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type}  
                    choice2 = max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].shift_end)) # choose respective road tanker of different capacity based on max shift end time 
                    min_remaining_shift = res_dict_type[max(res_dict_type.keys(), key=(lambda k: res_dict_type[k].min_remaining_shift))].min_remaining_shift


        for order_dict in full_orders_dict:
            try:
                group_tag = order_dict[min(order_dict.keys(), key=(lambda k: order_dict[k].grouping))].grouping
            except Exception as e:
                group_tag = [order_dict[k].grouping for k in order_dict.keys()]

            if "LD" in group_tag:
                station_list = station_list_LD.copy()
                stops = min(3, len(station_list))
                max_stops = [x for x in range(stops, 0, -1)] # [3, 2, 1]
            else:
                station_list = station_list_nonLD.copy()
                stops = min(2, len(station_list))
                max_stops = [x for x in range(stops, 0, -1)] # [2, 1]

            for m in max_stops:
                perm = combinations(station_list, m)
                for i in set(perm):
                    multiload_status = []
                    ori_status = []

                    # If the route has combinations are three drops, make sure each drop is of unique destinations 
                    if len(i) > 2 and i[0].split('_')[0] != i[1].split('_')[0] and i[0].split('_')[0] != i[2].split('_')[0] and i[1].split('_')[0] != i[2].split('_')[0]:
                        orders =[]
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        drop_2 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[1]}
                        drop_3 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[2]}

                        # TESTING REQUIRED!!! When do we add ori_status value?
                        if opo2_first == True :
                            ori_status.append(list(drop_1.values())[0].ori_status)
                            ori_status.append(list(drop_2.values())[0].ori_status)
                            ori_status.append(list(drop_3.values())[0].ori_status)

                        # TESTING REQUIRED!!! When do we add ori_status value?
                        if opo2_first == True and len(ori_status) > 0 and max(ori_status, key=len) == "original" :
                            orders =[]
                        else:
                            orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                            orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))
                            orders.append((list(drop_3.keys())[0], list(drop_3.values())[0]))

                        # Resulting in a route up to m drops
                        if len(orders) > 0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity + list(drop_3.values())[0].quantity) == res_type:
                            if on1_on2_tanker:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            else :
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                            if rg.can_fit_list(orders, region, False, False):
                                result_rg.append(rg)
                            else:
                                unmatched_rg.append(rg)

                        elif len(orders) > 0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity + list(drop_3.values())[0].quantity) != res_type:
                            rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            if rg.can_fit_list(orders, region, True, False) == False:
                                unmatched_rg.append(rg)
                        else:
                            # Currently only in Eastern Routing
                            rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            if rg.can_fit_list(orders, region, False, True) == False:
                                unmatched_rg.append(rg)

                    elif len(i) == 2 and (i[0].split('_')[0] != i[1].split('_')[0]):
                        orders =[]
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        drop_2 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[1]}

                        if opo2_first == True :
                            ori_status.append(list(drop_1.values())[0].ori_status)
                            ori_status.append(list(drop_2.values())[0].ori_status)

                        # TESTING REQUIRED!!! When do we add ori_status value?
                        if opo2_first == True and len(ori_status) > 0 and max(ori_status, key=len) == "original" :
                            orders =[]
                        else:
                            # TODO: DOUBLE CHECK meaning and usage of ori_status
                            orders.append((list(drop_1.keys())[0], list(drop_1.values())[0]))
                            orders.append((list(drop_2.keys())[0], list(drop_2.values())[0]))

                        if len(orders) >0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) == res_type:
                            if on1_on2_tanker:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            else:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity,resource_dict[choice2].sub_resource,resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                            if rg.can_fit_list(orders, region, False, False):
                                result_rg.append(rg)
                            else:
                                unmatched_rg.append(rg)

                        elif len(orders)>0 and int(list(drop_1.values())[0].quantity + list(drop_2.values())[0].quantity) != res_type:
                            rg = RouteGenerator(
                                resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                            if rg.can_fit_list(orders, region, True, False) == False:
                                unmatched_rg.append(rg)
                            else:
                                pass

                        else:
                            rg = RouteGenerator(
                                resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                            if rg.can_fit_list(orders, region, False, True) == False:
                                unmatched_rg.append(rg)

                    elif len(i) == 1:
                        orders =[]
                        drop_1 = {k: v for k, v in orders_dict.items() if orders_dict[k].identifier == i[0]}
                        for key in range(len(drop_1)):

                            if opo2_first == True and list(drop_1.values())[0].ori_status == "original" :
                                orders =[]                            
                            elif multiload_combi != 'Later':
                                orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                            elif multiload_combi == 'Later' and list(drop_1.values())[0].multiload_status != "Normal":
                                orders.append((list(drop_1.keys())[key], list(drop_1.values())[key]))
                            else:
                                orders =[]

                            if len(orders) >0 and int(list(drop_1.values())[key].quantity) == res_type:
                                if on1_on2_tanker :
                                    rg = RouteGenerator(
                                        resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, min_shift_start, max_shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)
                                else:
                                    rg = RouteGenerator(
                                        resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time)

                                if rg.can_fit_list(orders, region, False, False):
                                    result_rg.append(rg)
                                else:
                                    unmatched_rg.append(rg)

                            elif len(orders) >0 and int(list(drop_1.values())[key].quantity) != res_type:
                                rg = RouteGenerator(
                                    resource_dict[choice2].id, resource_dict[choice2].shift_type, resource_dict[choice2].capacity, resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].weight_limit, resource_dict[choice2].dry_weight, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end, min_remaining_shift, resource_dict[choice2].depot_start_time, date_time, depot_close_time = resource_dict[choice2].depot_close_time) 

                                if rg.can_fit_list(orders, region, True, False) == False:
                                    unmatched_rg.append(rg)
                            else :
                                pass
                    else:
                        pass

    return {cloud: {'cloud_wise_route': result_rg, 'cloud_wise_unmatched': unmatched_rg}}

# TODO Write in heuristic class, change Resource and AssignedResource
#  dependency
def AssignResource(id, capacity, sub_resource, sub_resource_quantity, type, weight_limit, remain_shift_hour):
    raise Exception("Not implemented, kept for future work")


def fit_order(orders_dict, resource_dict):
    """
    Args:
        orders_dict: key= order unique id, value = Order class
        resource_dict: key= truck name, value = resource class i.e. truck
    """
    ar = []
    unassigned_order = []
    trial_resource = {}
    total_resource = len(resource_dict)
    while len(orders_dict):
        resource_dict = {k: v for k, v in resource_dict.items() if resource_dict[k].balance_shift > 0}
        orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].quantity > 0}
        choice = random.choice(list(orders_dict.keys()))
        if choice in trial_resource:
            pass
        else:
            trial_resource[choice] = []

        if ((len(ar) == 0) and (len(resource_dict)>0) and (len(trial_resource[choice]) != total_resource)):
            choice2 = random.choice(list(resource_dict.keys()))
            last_resource = AssignResource(resource_dict[choice2].id, resource_dict[choice2].capacity,
                                           resource_dict[choice2].sub_resource,
                                           resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].type, resource_dict[choice2].weight_limit, resource_dict[choice2].remain_shift_hour)
            if last_resource.can_fit(orders_dict[choice]):      # and remaining_orders_quantity > last_resource.capacity:
                last_resource.add_order(orders_dict[choice])
                ar.append(last_resource)
                del orders_dict[choice]
                resource_dict[choice2].remain_shift_hour -= last_resource.substract_time
                #print("last_resource.shift_hour :", resource_dict[choice2].shift_hour)
                #print("last_resource.remain_shift_hour :", resource_dict[choice2].remain_shift_hour)
            else:
                trial_resource[choice].append(last_resource)
        elif ((len(resource_dict)>0)and (len(trial_resource[choice]) != total_resource)):
            last_resource = ar[-1]
            if last_resource.can_fit(orders_dict[choice]) :#and remaining_orders_quantity > last_resource.capacity:
                last_resource.add_order(orders_dict[choice])
                del orders_dict[choice]
                resource_dict[choice2].remain_shift_hour -= last_resource.substract_time
                #print("last_resource.shift_hour :", resource_dict[choice2].shift_hour)
                #print("last_resource.remain_shift_hour :", resource_dict[choice2].remain_shift_hour)
            else:
                choice2 = random.choice(list(resource_dict.keys()))
                new_resource = AssignResource(resource_dict[choice2].id, resource_dict[choice2].capacity,
                                              resource_dict[choice2].sub_resource, resource_dict[choice2].sub_resource_quantity,
                                              resource_dict[choice2].type, resource_dict[choice2].weight_limit, resource_dict[choice2].remain_shift_hour)

                if new_resource.can_fit(orders_dict[choice]): #and remaining_orders_quantity > last_resource.capacity:
                    new_resource.add_order(orders_dict[choice])
                    ar.append(new_resource)
                    del orders_dict[choice]
                    resource_dict[choice2].remain_shift_hour -= new_resource.substract_time
                    #print("new_resource.shift_hour :", resource_dict[choice2].shift_hour)
                    #print("new_resource.remain_shift_hour :", resource_dict[choice2].remain_shift_hour)
                else:
                    trial_resource[choice].append(last_resource)
        else:
            print("no more resources available")
            unassigned_order.append(orders_dict[choice])
            del orders_dict[choice]
    return ar, resource_dict, orders_dict, unassigned_order

def fit_fully_truck(orders_dict, resource_dict):
    orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
    resource_dict = {k: v for k, v in resource_dict.items() if (resource_dict[k].remain_shift_hour > 0) and (resource_dict[k].type == 'TERM')}
    resource_quant = {resource_dict[k].capacity: v for k, v in resource_dict.items()}
    resource_type = sorted(resource_quant.keys(), reverse=True) # try bigger truck first
    #resource_type = resource_quant.keys() # try bigger truck first
    result_ar = []
    unassigned_order = []
    previous_orderdict = {}
    sublist = []
    reverse_drop_shipment = []
    trial = 0
    while len(resource_dict) and len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}) >3 and trial < len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True})*200 :
        print("Length of orders", len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}))
        try:
            orders_dict = OrderedDict(sorted(orders_dict.items(), key=lambda x: x[
            1].quantity, reverse=False))
            print("1st order dict",len(orders_dict))
            #print("1st resource dict", len(resource_dict))
            print("cloud", orders_dict[list(orders_dict.keys())[0]].cloud)
            print("trial", trial)
            #randomSize = random.choice(list(resource_type))
            new_list = [list(orders_dict.items())[0], list(orders_dict.items())[1], list(orders_dict.items())[2], list(orders_dict.items())[-2], list(orders_dict.items())[-1]]
            #print("new list :", new_list[0][1], new_list[1][1], new_list[2][1], new_list[3][1], new_list[4][1])
            new_list = random.sample(list(orders_dict.items()), 3)
            for res_type in resource_type:
                # ("res_type :", res_type)
                res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type}
                #print("res_dict_type :", res_dict_type)
                #random.shuffle(list(res_dict_type.keys()))
                choice2 = random.choice(list(res_dict_type.keys()))
                print("choice2 :", choice2)
                arr = [k[1].quantity for k in new_list ]
                # print("arr :",arr)
                assigned_orders = printAllSubsets(arr, 3, resource_dict[choice2].capacity)
                # print("assigned_orders :", assigned_orders)
                print("For truck capacity :", resource_dict[choice2].capacity)
                flat = [k for i in assigned_orders for j in i for k in j if len(k)>0]
                #flat = [m for i in assigned_orders for j in i for k in j for l in k for m in l if len(m)>0]
                # print("flat :", flat)
                if len(flat)>0:
                    choice = random.choice(list(range(len(flat))))
                    # print(list(range(len(flat))))
                    # print("choice of flat",choice)
                    ar = AssignResource(resource_dict[choice2].id, resource_dict[choice2].capacity,
                                                resource_dict[choice2].sub_resource,
                                                resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].type,
                                                resource_dict[choice2].weight_limit, resource_dict[choice2].shift_hour, resource_dict[choice2].remain_shift_hour,
                                                resource_dict[choice2].shift_divide_2, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end)
                    # print("truck ", ar.id)
                    # print("remain hour",ar.remain_shift_hour)                           
                    #print("remain_shift_hour :", ar.remain_shift_hour)
                    #print("shift_hour :", ar.shift_hour)
                    #print(ar)
                    assigned = flat[choice]
                    #print("assign",assigned, len(assigned))
                    orders =[]
                    for order in assigned:
                        # print("assigned", order)
                        to_remove = next((x for x in orders_dict.keys() if (orders_dict[x].quantity == order) and  (orders_dict[x].process == True)), 1)
                        # print("remove",to_remove)
                        if to_remove == 1:
                            break
                        orders.append((to_remove, orders_dict[to_remove]))
                        print(orders)
                        orders_dict[to_remove].process = False
                        # print("orders_dict[to_remove].process", orders_dict[to_remove].process)
                        # del orders_dict[to_remove]
                        #print(orders_dict)
                    orders_copy = deepcopy(orders)
                    if (len(orders_copy) == len(assigned) and ar.can_fit_list(orders_copy)) :
                        if len(resource_dict[choice2].schedule) > 0 :
                            #print("ENTER")
                            #print(resource_dict[choice2].remain_shift_hour)
                            #print(choice2)
                            sublist = []
                            I = []
                            #print("resource before", resource_dict[choice2].schedule)
                            #idx = [i for i,x in enumerate(result_ar) if x.id == choice2]
                            #sublist = [result_ar[i] for i in idx]
                            sublist = resource_dict[choice2].schedule
                            #print("sublist before", sublist)
                            sublist.append(ar)
                            #print("sublist after", sublist)
                            for trip in sublist:
                                I.append(Interval(trip.index, trip.load_time, trip.return_time, trip.weight, 1/len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}) ))
                                #print(trip.index, trip.load_time, trip.return_time, trip.weight, len(orders_dict))
                            O = schedule_weighted_intervals(I) #Checking Overlap Schedule
                            #print("BINGOOOO",O)
                            set1 = set((x.index) for x in sublist)
                            non_overlap = [ x.shipment for x in O if x.shipment in set1 ]
                            # print("non_overlap", non_overlap)
                            if ar.index in non_overlap :
                                selected_shipment = [ x for x in sublist if x.index in non_overlap ]
                                #print("selected", selected_shipment)
                                not_selected_shipment = [ x for x in sublist if x.index not in non_overlap ]
                                result_ar.append(ar)
                                resource_dict[choice2].schedule = []
                                resource_dict[choice2].schedule.append(selected_shipment)
                                resource_dict[choice2].schedule = [val for sublist in resource_dict[choice2].schedule for val in sublist]
                                # print("added schedule",resource_dict[choice2].schedule)
                                resource_dict[choice2].remain_shift_hour -= (ar.total_subtract_time + ar.return_hour)
                                #print("final remain shift", resource_dict[choice2].remain_shift_hour)

                                if len(not_selected_shipment) > 0:
                                    #print("not_selected", not_selected_shipment)
                                    for trip in not_selected_shipment :
                                        #print("resource b4", resource_dict[choice2].remain_shift_hour)
                                        #print("trip details")
                                        #print(trip)
                                        # print(trip.orders)
                                        #print(trip.total_subtract_time + trip.return_hour)
                                        resource_dict[choice2].remain_shift_hour += (trip.total_subtract_time + trip.return_hour)
                                        #print("resource after", resource_dict[choice2].remain_shift_hour)
                                        for i in trip.orders: # change the process back to true
                                            if (i in orders_dict.keys()):
                                                # print("removed order", i)
                                                # print(i.unique_id)
                                                # print(orders_dict)
                                                orders_dict[(i.unique_id)].process = True
                                                # print("not_selected_shipment", orders_dict[i.unique_id])
                                            else:
                                                reverse_drop_shipment.append(i.unique_id)
                                for i in orders_copy:
                                    assigned_orders.append(i)
                                    # print("assigned_orders", assigned_orders)
                            else :
                                resource_dict[choice2].schedule.remove(ar)
                                # print("else",resource_dict[choice2].schedule)
                                for i in orders_copy:
                                    # print("not selected because not enugh weight", orders_dict[i[0]])
                                    orders_dict[i[0]].process = True
                                    trial +=1
                        else :
                            #print("ar before",vars(ar))
                            #else:
                            result_ar.append(ar)
                            resource_dict[choice2].schedule.append(ar)
                            # print("schedule", resource_dict[choice2].schedule)
                            #print(result_ar)
                            assigned_orders = []
                            #print(ar.total_subtract_time)
                            # print("resource_dict[choice2].remain_shift_hour :", resource_dict[choice2].remain_shift_hour)
                            resource_dict[choice2].remain_shift_hour -= (ar.total_subtract_time + ar.return_hour)
                            # print("resource_dict[choice2].remain_shift_hour :", resource_dict[choice2].remain_shift_hour)

                            for i in orders_copy:
                                assigned_orders.append(i)
                    else:
                        for i in orders_copy:
                            orders_dict[i[0]].process = True
                            trial +=1
                            # print(orders_dict[i[0]])
                            # print(orders_dict[i[0]].process)
                            # print("i", i)
                            #print('Find different set of orders can be fit')
                else:
                    #print('no order pattern that can fit the truck')
                    trial += 1
        except ValueError:
            print('not enough order to fully fill truck')

    # process the remaining 3 orders (if cloud has less than 3 orders or less to be processed)
    trial = 0
    while len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}) <4 and trial < len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}) *50:
        less_orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
        print("cloud", orders_dict[list(orders_dict.keys())[0]].cloud)
        print("2nd process original length",len(orders_dict))
        print("2nd process need to be processed",len(less_orders_dict))
        # less_orders_dict = OrderedDict(sorted(less_orders_dict.items(), key=lambda x: x[1].quantity, reverse=False))
        arr = [k[1].quantity for k in less_orders_dict.items()]
        #print("arr", arr)
        resource_dict = {k: v for k, v in resource_dict.items() if resource_dict[k].remain_shift_hour > 0}
        resource_quant = {resource_dict[k].capacity: v for k, v in resource_dict.items()}
        resource_type = sorted(resource_quant.keys(), reverse=True)
        for res_type in resource_type:
            print(res_type)
            trial +=1
            print(trial)
            print("target :", len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}))
            result = [seq for i in range(len(arr), 0, -1) for seq in combinations(arr, i) if sum(seq) == res_type]
            if(len(result)>0):
                #print(result)
                choice = random.choice(list(range(len(result))))
                #print("choice", choice)
                res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type}
                choice2 = random.choice(list(res_dict_type.keys()))
                #print("choice2 :", choice2)
                ar = AssignResource(resource_dict[choice2].id, resource_dict[choice2].capacity,
                                             resource_dict[choice2].sub_resource,
                                             resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].type,
                                             resource_dict[choice2].weight_limit, resource_dict[choice2].shift_hour, resource_dict[choice2].remain_shift_hour,
                                             resource_dict[choice2].shift_divide_2, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end)
                assigned = result[choice]
                #print("assign",assigned, len(assigned))
                orders =[]
                for order in assigned:
                    to_remove = next((x for x in orders_dict.keys() if (orders_dict[x].quantity == order) and  (orders_dict[x].process == True)), 1)
                    #print("remove",to_remove)
                    if to_remove == 1:
                        break
                    orders.append((to_remove, orders_dict[to_remove]))
                    #print(orders)
                    orders_dict[to_remove].process = False
                orders_copy = deepcopy(orders)
                if (len(orders_copy) == len(assigned) and ar.can_fit_list(orders_copy)) :
                    result_ar.append(ar)
                    resource_dict[choice2].schedule.append(ar)
                    #print("schedule", resource_dict[choice2].schedule)
                    #print(ar.total_subtract_time)
                    # print("resource_dict[choice2].remain_shift_hour :", resource_dict[choice2].remain_shift_hour)
                    resource_dict[choice2].remain_shift_hour -= (ar.total_subtract_time + ar.return_hour)
                    # print("resource_dict[choice2].remain_shift_hour :", resource_dict[choice2].remain_shift_hour)
                else:
                    for i in orders_copy:
                        orders_dict[i[0]].process = True

    if (len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True})>0):
        unassign_orders = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
        print("unassigned order in cloud", orders_dict[list(orders_dict.keys())[0]].cloud)
        print("unassigned order count", len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}))
    return result_ar, resource_dict, reverse_drop_shipment


### Post process fit fully function (yet to edit)
def post_fit_fully_truck(orders_dict, resource_dict):
    orders_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
    resource_dict = {k: v for k, v in resource_dict.items() if resource_dict[k].remain_shift_hour > 0}
    resource_quant = {resource_dict[k].capacity: v for k, v in resource_dict.items()}
    #resource_type = sorted(resource_quant.keys(), reverse=True) # try bigger truck first
    resource_type = resource_quant.keys() # try bigger truck first
    result_ar = []
    unassigned_order = []
    previous_orderdict = {}
    sublist = []
    reverse_drop_shipment = []
    trial = 0
    while len(resource_dict) and len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}) >3 and trial < 1000 :
        print("Length of orders", len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}))
        try:
            orders_dict = OrderedDict(sorted(orders_dict.items(), key=lambda x: x[
            1].quantity, reverse=False))
            print("1st order dict",len(orders_dict))
            #print("1st resource dict", len(resource_dict))
            print("cloud", orders_dict[list(orders_dict.keys())[0]].cloud)
            print("trial", trial)
            #randomSize = random.choice(list(resource_type))
            #new_list = [list(orders_dict.items())[0], list(orders_dict.items())[1], list(orders_dict.items())[2], list(orders_dict.items())[-2], list(orders_dict.items())[-1]]
            #print("new list :", new_list[0][1], new_list[1][1], new_list[2][1], new_list[3][1], new_list[4][1])
            new_list = random.sample(list(orders_dict.items()), 3)
            for res_type in resource_type:
            #print("res_type :", res_type)
                res_dict_type = {k: v for k, v in resource_dict.items() if resource_dict[k].capacity == res_type}
                #print("res_dict_type :", res_dict_type)
                #random.shuffle(list(res_dict_type.keys()))
                choice2 = random.choice(list(res_dict_type.keys()))
                print("choice2 :", choice2)
                arr = [k[1].quantity for k in new_list ]
                print("arr :",arr)
                assigned_orders = printAllSubsets(arr, 3, resource_dict[choice2].capacity)
                print("assigned_orders :", assigned_orders)
                print("For truck capacity :", resource_dict[choice2].capacity)
                flat = [k for i in assigned_orders for j in i for k in j if len(k)>0]
                #flat = [m for i in assigned_orders for j in i for k in j for l in k for m in l if len(m)>0]
                print("flat :", flat)
                if len(flat)>0:
                    choice = random.choice(list(range(len(flat))))
                    print(list(range(len(flat))))
                    print("choice of flat",choice)
                    ar = AssignResource(resource_dict[choice2].id, resource_dict[choice2].capacity,
                                                resource_dict[choice2].sub_resource,
                                                resource_dict[choice2].sub_resource_quantity, resource_dict[choice2].type,
                                                resource_dict[choice2].weight_limit, resource_dict[choice2].shift_hour, resource_dict[choice2].remain_shift_hour,
                                                resource_dict[choice2].shift_divide_2, resource_dict[choice2].shift_start, resource_dict[choice2].shift_end)
                    print("truck ", ar.id)
                    print("remain hour",ar.remain_shift_hour)
                    #print("remain_shift_hour :", ar.remain_shift_hour)
                    #print("shift_hour :", ar.shift_hour)
                    #print(ar)
                    assigned = flat[choice]
                    #print("assign",assigned, len(assigned))
                    orders =[]
                    for order in assigned:
                        print("assigned", order)
                        to_remove = next((x for x in orders_dict.keys() if (orders_dict[x].quantity == order) and  (orders_dict[x].process == True)), 1)
                        print("remove",to_remove)
                        if to_remove == 1:
                            break
                        orders.append((to_remove, orders_dict[to_remove]))
                        #print(orders)
                        orders_dict[to_remove].process = False
                        print("orders_dict[to_remove].process", orders_dict[to_remove].process)
                        # del orders_dict[to_remove]
                        #print(orders_dict)
                    orders_copy = deepcopy(orders)
                    if (len(orders_copy) == len(assigned) and ar.can_fit_list(orders_copy)) :
                        print("check schedule", resource_dict[choice2].schedule)
                        if len(resource_dict[choice2].schedule) > 0 :
                            print("ENTER")
                            #print(resource_dict[choice2].remain_shift_hour)
                            #print(choice2)
                            sublist = []
                            I = []
                            print("resource before", resource_dict[choice2].schedule)
                            #idx = [i for i,x in enumerate(result_ar) if x.id == choice2]
                            #sublist = [result_ar[i] for i in idx]
                            sublist = resource_dict[choice2].schedule
                            #print("sublist before", sublist)
                            sublist.append(ar)
                            #print("sublist after", sublist)
                            for trip in sublist:
                                I.append(Interval(trip.index, trip.load_time, trip.return_time, trip.weight))
                                print(trip.index, trip.load_time, trip.return_time, trip.weight)
                            O = schedule_weighted_intervals(I) #Checking Overlap Schedule
                            #print("BINGOOOO",O)
                            set1 = set((x.index) for x in sublist)
                            non_overlap = [ x.shipment for x in O if x.shipment in set1 ]
                            print("non_overlap", non_overlap)
                            if ar.index in non_overlap :
                                selected_shipment = [ x for x in sublist if x.index in non_overlap ]
                                #print("selected", selected_shipment)
                                not_selected_shipment = [ x for x in sublist if x.index not in non_overlap ]
                                result_ar.append(ar)
                                resource_dict[choice2].schedule = []
                                resource_dict[choice2].schedule.append(selected_shipment)
                                resource_dict[choice2].schedule = [val for sublist in resource_dict[choice2].schedule for val in sublist]
                                print("added schedule",resource_dict[choice2].schedule)
                                resource_dict[choice2].remain_shift_hour -= (ar.total_subtract_time + ar.return_hour)
                                #print("final remain shift", resource_dict[choice2].remain_shift_hour)

                                if len(not_selected_shipment) > 0:
                                    #print("not_selected", not_selected_shipment)
                                    for trip in not_selected_shipment :
                                        #print("resource b4", resource_dict[choice2].remain_shift_hour)
                                        print("trip details")
                                        print(trip)
                                        print(trip.orders)
                                        #print(trip.total_subtract_time + trip.return_hour)
                                        resource_dict[choice2].remain_shift_hour += (trip.total_subtract_time + trip.return_hour)
                                        #print("resource after", resource_dict[choice2].remain_shift_hour)
                                        for i in trip.orders: # change the process back to true
                                            if (i in orders_dict.keys()):
                                                print("removed order", i)
                                                print(i.unique_id)
                                                print(orders_dict)
                                                orders_dict[(i.unique_id)].process = True
                                                print("not_selected_shipment", orders_dict[i.unique_id])
                                            else:
                                                reverse_drop_shipment.append(i.unique_id)
                                for i in orders_copy:
                                    assigned_orders.append(i)
                                    print("assigned_orders", assigned_orders)
                            else :
                                resource_dict[choice2].schedule.remove(ar)
                                # print("else",resource_dict[choice2].schedule)
                                for i in orders_copy:
                                    print("not selected because not enugh weight", orders_dict[i[0]])
                                    orders_dict[i[0]].process = True
                                    trial +=1
                        else :
                            #print("ar before",vars(ar))
                            #else:
                            result_ar.append(ar)
                            resource_dict[choice2].schedule.append(ar)
                            print("schedule", resource_dict[choice2].schedule)
                            #print(result_ar)
                            assigned_orders = []
                            #print(ar.total_subtract_time)
                            print("resource_dict[choice2].remain_shift_hour :", resource_dict[choice2].remain_shift_hour)
                            resource_dict[choice2].remain_shift_hour -= (ar.total_subtract_time + ar.return_hour)
                            print("resource_dict[choice2].remain_shift_hour :", resource_dict[choice2].remain_shift_hour)

                            for i in orders_copy:
                                assigned_orders.append(i)
                    else:
                        for i in orders_copy:
                            orders_dict[i[0]].process = True
                            trial +=1
                            print(orders_dict[i[0]])
                            print(orders_dict[i[0]].process)
                            print("i", i)
                            #print('Find different set of orders can be fit')
                else:
                    #print('no order pattern that can fit the truck')
                    trial += 1
        except ValueError:
            print('not enough order to fully fill truck')

    # if len(orders_dict) <= 3:
    #     # combination of three in a list
    if (len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True})>0):
        unassign_orders = {k: v for k, v in orders_dict.items() if orders_dict[k].process == True}
        print("unassigned order in cloud", orders_dict[list(orders_dict.keys())[0]].cloud)
        print("unassigned order count", len({k: v for k, v in orders_dict.items() if orders_dict[k].process == True}))
    return result_ar, resource_dict, reverse_drop_shipment



def simulating_solutions(orders_dict, resource_dict):
    """Args: this function simulating the fit order for many times
        orders_dict: resource_dict:

    Args:
        orders_dict:
        resource_dict:
    """
    cloud_wise_sol = {}
    for cloud in range(1,28):
        or_dict = {k: v for k, v in orders_dict.items() if orders_dict[k].cloud == cloud}
        cloud_wise_sol[cloud], resource_dict = fit_order(or_dict, resource_dict)
    for cloud in range(1, 28):
        ar_dict = {}
        for i in cloud_wise_sol[cloud]:
            ar_dict.update({i.index: i})
            cloud_wise_sol[cloud] = ar_dict
    return cloud_wise_sol, resource_dict


class Heuristic():
    '''
    Class to define heuristic
    :arg
    id = mostly name of heuristic
    '''
    def __int__(self, id):
        self.name = id

    def run(self, instance):            # equivalent to fit_orders
        """
            instance: method to apply heuristic on instance
        """
        ar = []
        if self.name == 'ff':
            # instance.remove_resource(first[0])
            while len(instance.order):
                instance.resource = OrderedDict({k: v for k, v in instance.resource.items()
                                     if
                                     instance.resource[k].remain_shift_hour > 0})
                instance.order = OrderedDict({k: v for k, v in instance.order.items() if
                                  instance.order[k].quantity > 0})
                instance.sort_order()
                instance.sort_resource()
                first = list(instance.resource.items())[0]
                order = list(instance.order.items())[0]
                if len(ar) == 0:
                    resource = first[1]
                    truck = AssignResource(resource.id,
                                       resource.capacity,
                                       resource.sub_resource,
                                       resource.sub_resource_quantity,
                                       resource.type,
                                       resource.weight_limit)
                    if truck.can_fit(order[1]):
                        truck.add_order(order[1])
                        ar.append(truck)
                        resource.remain_shift_hour -= truck.curr_travel_time
                        #truck.update_shift()
                        instance.remove_order(order[0])
                else:
                    last_truck = ar[-1]
                    if last_truck.can_fit(order[1]):
                        last_truck.add_order(order[1])
                        instance.remove_order(order[0])
                        resource.remain_shift_hour -= last_truck.curr_travel_time
                    else:
                        new_truck = AssignResource(
                            resource.id,
                            resource.capacity,
                            resource.sub_resource,
                            resource.sub_resource_quantity,
                            resource.type,
                            resource.weight_limit)
                        if new_truck.can_fit(order[1]):
                            new_truck.add_order(order[1])
                            ar.append(new_truck)
                            resource.remain_shift_hour -= new_truck.curr_travel_time
                            #truck.update_shift() # wrong, shud be new truck
                            instance.remove_order(order[0])
                        # else:
                        #     instance.add_order(order)

        if self.name == 'random':
            while len(instance.order):
                instance.resource = OrderedDict({k: v for k,
                v in instance.resource.items() if instance.resource[k].remain_shift_hour > 0})
                instance.order= OrderedDict({k: v for k,
                                                    v in instance.order.items() if
                               instance.order[k].quantity > 0})
                choice = random.choice(list(instance.order.keys()))
                if len(ar) == 0:
                    choice2 = random.choice(list(instance.resource.keys()))
                    last_resource = AssignResource(instance.resource[choice2].id,
                                                   instance.resource[
                                                       choice2].capacity,
                                                   instance.resource[
                                                       choice2].sub_resource,
                                                   instance.resource[
                                                       choice2].sub_resource_quantity,
                                                   instance.resource[choice2].type,
                                                   instance.resource[choice2].weight_limit)

                    if last_resource.can_fit(instance.order[
                                                 choice]):  # and remaining_orders_quantity > last_resource.capacity:
                        last_resource.add_order(instance.order[choice])
                        ar.append(last_resource)
                        instance.resource[choice2].remain_shift_hour -= last_resource.curr_travel_time
                        #instance.resource[choice2].shift -= 1
                        del instance.order[choice]
                    # else:
                    #     orders_dict.update({choice: orders_dict[choice]})
                else:
                    last_resource = ar[-1]
                    if last_resource.can_fit(instance.order[
                                                 choice]):  # and remaining_orders_quantity > last_resource.capacity:
                        last_resource.add_order(instance.order[choice])
                        del instance.order[choice]
                        instance.resource[choice2].remain_shift_hour -= last_resource.curr_travel_time
                    else:
                        choice2 = random.choice(list(instance.resource.keys()))
                        new_resource = AssignResource(
                            instance.resource[choice2].id,
                            instance.resource[choice2].capacity,
                            instance.resource[choice2].sub_resource,
                            instance.resource[choice2].sub_resource_quantity,
                            instance.resource[choice2].type,
                            instance.resource[choice2].weight_limit)

                        if new_resource.can_fit(instance.order[
                                                    choice]):  # and remaining_orders_quantity > last_resource.capacity:
                            new_resource.add_order(instance.order[choice])
                            ar.append(new_resource)
                            instance.resource[choice2].remain_shift_hour -= new_resource.curr_travel_time
                            #instance.resource[choice2].shift -= 1
                            del instance.order[choice]
                        # else:
                        #     orders_dict.update({choice: orders_dict[choice]})
                        # resource_dict[choice2].shift += 1
        return ar, instance.resource
