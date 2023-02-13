import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from src.input_gen_modules import input_generation
import datetime
from src.rts_routing_modules.rts_routing_utilities import order_tagging_numeric

engine = create_engine('postgresql+psycopg2://postgres:petronas@localhost/retail_db')

def scheduling_input(loop, txn_id, date, region, capacity, largest_capacity):

    if loop.split('_')[1] == "First" :
        try:
            order_data = pd.read_sql('select * from "INT8_rts_route_output" where "Txn_ID" = \'{}\' and "Loop_Counter" = \'{}\' '.format(txn_id, loop), con=engine)
            order_data = order_tagging_numeric(order_data)
        except Exception as e:
            order_data = pd.DataFrame()
        #order_data = order_data[order_data['Cloud']==12]
        truck_data = input_generation.load_truck_data(date, region, capacity)
        truck_data = truck_data.reset_index(drop=True)
        truck_data['Vehicle Number'] = truck_data['Vehicle Number'].str.replace(' ', '')
        truck_data['start_shift'] = np.where(truck_data['start_shift']==datetime.time(1, 30), 1.5, 1.5)
        truck_data['end_shift'] = np.where(truck_data['end_shift']==datetime.time(23, 59), 23.95, 14)

    else :
        try:
            order_data = pd.read_sql('select * from "INT8_rts_route_output" where "Txn_ID" = \'{}\' and "Loop_Counter" = \'{}\' '.format(txn_id, loop), con=engine)
            order_data = order_tagging_numeric(order_data)
        except Exception as e:
            order_data = pd.DataFrame()
        truck_data = pd.read_sql('select * from "INT8_rts_updated_truck_data_MILPOutput" where "Txn_ID" = \'{}\' and "Loop_Counter" = \'{}\' '.format(txn_id, f"{capacity}_First"), con=engine)
        if loop == "43680_Second" :
            truck_data = truck_data[truck_data['balance_shift']>=3]
        else:
            pass

    return order_data, truck_data
