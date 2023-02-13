import os
import random
import string
import datetime
import numpy as np
from datetime import datetime, timedelta
import src.rts_routing_modules.rts_route_main as route_generator
import src.rts_scheduling_modules.rts_scheduling_main as milp_scheduling
from src.input_gen_modules.input_generation import InputGen
from src.input_gen_modules.rts_input_handler import InputHandler
from database_connector import DatabaseConnector, YAMLFileConnector, PandasFileConnector
from conf import Config
from return_result import push_result_orderbank
from scheduled_order_analysis import scheduled_order_analysis
from src.utility.db_connection_helper import get_db_connection

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

def main(order_id, region, date):
    print(f"RTS main started with order_id={order_id},region={region},date={date}")
    """
    Args: Pass parameters to ochestrate RTS modules to produce outputs up until before post process, output to individual tables of each modules
        txn_id[string]: randomly generated 6-digits txn id of each Run of main script, randomly seeded upon current system time
        date_time[datetime]: date of dmr order planning (not rapid data date) # in format: datetime(2020, 9, 24)
        region[string] : which region of order data to read as order data 1st input
        loop_1, loop_2[string] : signify which loop the code is in to change inputs into input_generation and outputs from each modules
        module_1, module_2 : signify which module the code is in to change outputs from input_generation
        start_time[datetime] = date and time for each Run
    """
    
    # Select RTS Strategy
    post_process = False
    input_source = Config.INPUT_SOURCE["DMR"]
    rts_strategy = "all_capacity_first" # choices: sequential_capacity, all_capacity_first, multiload_all_capacity_first

    # # Reset so get full traceback next time you run the script and a "real"
    # # exception occurs
    # if hasattr (sys, 'tracebacklimit'):
    #     del sys.tracebacklimit

    # # Raise this class for "soft halt" with minimum traceback.
    # class Stop(Exception):
    #     def __init__ (self):
    #         sys.tracebacklimit = 0   
    
    class NoOrderException(Exception):
        def __init__(self, txn_id):
            self.txn_id = txn_id

    start_time_loop = datetime.now()
    txn_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
    planning_date = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)
    delivery_date = datetime.strptime(date, '%Y-%m-%d')

    print("RTS API call received")

    # Read locked/pre-shipment orders id to remove from received order id of orderbank
    locked_id, _ = InputHandler.get_locked_orders(order_id, region, planning_date)
    order_id_ori = order_id.copy()
    order_id = [x for x in order_id if x not in locked_id]

    # Get list of active tankers' capacities & locked orders(scheduling 2nd loop input)
    truck_data, truck_capacity_awsm, locked_orders, _ = InputGen.load_truck_data(None, "main", region, planning_date, order_id_ori)

    # Get list of unique capacities of final active tankers after above subsetting and cross reference with default tanker capacities of the region
    truck_capacity_check = Config.MODEL_INPUTOUTPUT['truck_capacity'][region]
    truck_capacity = np.intersect1d(truck_capacity_awsm, truck_capacity_check)

    # TODO : to check correct smallest capacity and once capacity has been confirmed remove this chunk
    # indices = np.where(truck_capacity==10800)
    # truck_capacity = np.delete(truck_capacity, indices)
    # truck_capacity = np.array(truck_capacity)

    largest_capacity = int(max(truck_capacity))
    smallest_capacity = int(min(truck_capacity))
    post_process_loop = 0             
    multiload_all_tanker = False
    normal_all_tanker = False
    all_cap_toggle = False
    opo2_first_capacity = [largest_capacity]
    # txn_id = 'RM6GC'
    # truck_capacity = [16380]

    # Multiload Route with All Tanker Capacities or All Route with All Tanker Capacities 
    try :
        if rts_strategy == "multiload_all_capacity_first" or rts_strategy == "all_capacity_first":
            if rts_strategy == "multiload_all_capacity_first" :
                multiload_all_tanker = True
                normal_all_tanker = False
            else :
                multiload_all_tanker = False
                normal_all_tanker = True

            # Select capacity and opo2 first toggle
            all_cap_toggle = True
            opo2_first = False

            # Select True/False to ON/OFF workload balance objective function
            workload_obj = True

            capacity = sorted(truck_capacity,reverse=True)
            loop_1 = "AllCap_First"
            loop_2 = "AllCap_Second"

            # First Loop
            print("Begin route_generator.RouteCompile (loop 1)")
            route_compiler_1 = route_generator.RouteCompile(loop_1, order_id, planning_date, region, txn_id, start_time_loop, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, opo2_first, opo2_first_capacity, truck_capacity, order_id_ori)
            route_compiler_1.route_compile()
            print("End route_generator.RouteCompile (loop 1)")

            print("Begin milp_scheduling.SchedulingLoop (loop 1)")
            scheduler_1 = milp_scheduling.SchedulingLoop(planning_date, loop_1, workload_obj, txn_id, region, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, opo2_first, opo2_first_capacity, order_id, order_id_ori, None)
            scheduler_1.scheduling_loop()
            print("End milp_scheduling.SchedulingLoop (loop 1)")

            # Second Loop
            print("Begin route_generator.RouteCompile (loop 2)")
            route_compiler_2 = route_generator.RouteCompile(loop_2, order_id, planning_date, region, txn_id, start_time_loop, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, False, None, truck_capacity, order_id_ori)
            route_compiler_2.route_compile()
            print("End route_generator.RouteCompile (loop 2)")

            print("Begin milp_scheduling.SchedulingLoop (loop 2)")
            scheduler_2 = milp_scheduling.SchedulingLoop(planning_date, loop_2, workload_obj, txn_id, region, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, False, None, order_id, order_id_ori, locked_orders)
            scheduler_2.scheduling_loop()
            print("End milp_scheduling.SchedulingLoop (loop 2)")
        else :
            print("Entered unexpected else!")
            multiload_all_tanker = False
            normal_all_tanker = False
        
        try :
            scheduled_order_analysis(txn_id, planning_date, region, opo2_first, rts_strategy, truck_data)
        except Exception as err :
            print("[ScheduleAnalysis] Analysis output in {} failed to push to DB due to {}.".format(txn_id, err))

        end_time = datetime.now()
        diff = (end_time - start_time_loop).total_seconds() / 60.0
        print("Total runtime for Auto-scheduling is {} minutes".format(diff))

        order_status_dict = push_result_orderbank(txn_id)
        return order_status_dict
        
    except Exception as err:
        if str(err) == "No UNSCHEDULED and UNMATCHED orders left" or str(err) == "NO orders to be processed":
            raise NoOrderException(txn_id)
        else :
            raise Exception(f"Auto-scheduling failed with {err}")

        end_time = datetime.now()
        diff = (end_time - start_time_loop).total_seconds() / 60.0
        print("Total runtime for Auto-scheduling is {} minutes".format(diff))

    # while post_process_loop < 3 :
    #     if post_process_loop > 1 :
    #         post_process = True
    #     for capacity in sorted(truck_capacity,reverse=True):
    #         if capacity > largest_capacity:
    #             largest_capacity = capacity

    #         current_capacity = capacity.copy()

    #         if post_process:
    #             loop_1 = f"post{capacity}_First"
    #             loop_2 = f"post{capacity}_Second"
    #         else:
    #             loop_1 = f"{capacity}_First"
    #             loop_2 = f"{capacity}_Second"

    #         #First Loop
    #         route_generator.RouteCompile(loop_1, order_id, planning_date, region, txn_id, start_time_loop, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, False, None, truck_capacity)
    #         milp_scheduling.SchedulingLoop(planning_date, loop_1, txn_id, region, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, False, None)

    #         if capacity == largest_capacity and multiload_all_tanker == True : 
    #             multiload_all_tanker = False
    #         if capacity == largest_capacity and normal_all_tanker == True : 
    #             normal_all_tanker = False

    #         # Second Loop
    #         route_generator.RouteCompile(loop_2, order_id, planning_date, region, txn_id, start_time_loop, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, False, None, truck_capacity)
    #         milp_scheduling.SchedulingLoop(planning_date, loop_2, txn_id, region, capacity, largest_capacity, post_process, smallest_capacity, input_source, multiload_all_tanker, normal_all_tanker, all_cap_toggle, rts_strategy, False, None)

    #     post_process_loop += 1

    #     # Read sequential output data to determine if there's orders to post process   
    #     if current_capacity == smallest_capacity and post_process_loop < 3 :
    #         unscheduled_orders = InputHandler.get_unscheduled_data(loop_2, txn_id)
    #         unmatched_orders = InputHandler.get_unmatched_data(loop_2, txn_id)
    #         total_backlog_orders = pd.concat([unscheduled_orders, unmatched_orders], axis=0)
    #         if len(total_backlog_orders) > 0 :
    #             post_process_loop += 1
    #         else :
    #             post_process_loop = 100
    #     else :
    #         pass

if __name__ == "__main__":
    #solver_type = Config.OPTIMISATION_MODELLING_CONFIG['solver_type']
    #order_id = [8475, 8483, 8545, 8633, 8638]
    #import_from = Config.MODEL_INPUTOUTPUT["import_from"]
    #post_process = False
    #input_source = Config.INPUT_SOURCE["DMR"]
    # Strategy selection :
    # sequential_capacity
    # all_capacity_first
    # multiload_all_capacity_first
    #rts_strategy = "all_capacity_first"

    date = Config.MODEL_INPUTOUTPUT['date']
    order_id = DBConnector.load('order_bank', f"select id from order_bank where requested_delivery_date = '{date}' and terminal IN ('M817','M818','M819') and volume > 0 and ds_verified = 'VERIFIED'")['id'].tolist()
    region = Config.MODEL_INPUTOUTPUT['model_region']['NORTHERN']

  
    main(order_id, region, date)
