"""
INPUT HANDLER CLASS

This class serves as the main data getter class.
All data loading should come from this class to ensure standardised and structured data loading across scripts.
"""
import pandas as pd
import numpy as np
import sys
import os
from database_connector import DatabaseConnector, YAMLFileConnector
from conf import Config
from datetime import timedelta
from src.utility.db_connection_helper import get_db_connection

pd.set_option("max.columns", 20)
pd.set_option("display.width", 2000)

# Loading database credentials from config
DBConnector = get_db_connection(Config.PROJECT_ENVIRONMENT)

class InputHandler:
    """Loading data based on destination of import and default config import settings

    Args:
        import_from (string): Destination of import, either 'database' or 'csv'. Defaults to Config settings.
        import_settings (dict): Settings for import. Defaults to Config settings.

    Returns:
        [dataframe]: imported data
    """

    # TODO : To check date based on week of the day for other region if applicable
    @classmethod
    def get_terminal_dates(cls, region):
        table_name = Config.TABLES['terminal_dates']["tablename"]
        date_from = Config.TABLES['dates_range']["columns"]["date_from"]
        date_to = Config.TABLES['dates_range']["columns"]["date_to"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        sql_statement = "select terminal_dates.field, terminal_dates.terminal, \
                        date_range.type, date_range.time_from, date_range.time_to, date_range.days, date_range.date_from, date_range.date_to \
                        from \
                        terminal_dates LEFT JOIN date_range on terminal_dates.date_range = date_range.id \
                        where terminal_dates.terminal in ({})".format(terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)

        # TODO : To modify this chunk to include more field
        data_df = data_df[data_df['field'] == "terminal operating days"]
        return data_df

    # done
    @classmethod
    def get_terminal_data(cls, region):
        table_name = Config.TABLES['terminal_data']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        sql_statement = "select terminal.code, terminal.name, address.latitude, address.longitude, address.region, address.region_group \
                        from terminal LEFT JOIN address on terminal.address = address.id where terminal.code in ({})".format(
            terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_gps_data(cls, region):
        table_name = Config.TABLES['retail_customer_master']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        sql_statement = "select t1.ship_to_party, t1.latitude, t1.longitude, t1.region, t1.region_group from \
                        (select retail_customer.ship_to_party, retail_customer.address, address.latitude, address.longitude, address.region, address.region_group \
                        from retail_customer LEFT JOIN address on retail_customer.address = address.id) \
                        as t1 \
                        LEFT JOIN retail_customer_storage rc on t1.ship_to_party = rc.retail"
        # TODO : To add the below chunk once status_awsm has been clarified, now too many station inactive
        # where retail_customer.status_awsm = 'Active' and retail_customer_storage.terminal in ({})".format(terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)
        data_df = data_df.drop_duplicates(keep="first").reset_index(drop=True)
        convert_dict = {
            'ship_to_party': int,
        }
        data_df = data_df.astype(convert_dict)
        return data_df

    # done
    @classmethod
    def get_comm_gps_data(cls, region):
        table_name = Config.TABLES['commercial_customer_master']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        sql_statement = "select t1.ship_to_party, t1.latitude, t1.longitude, t1.region, t1.region_group from \
                        (select commercial_customer.ship_to_party, commercial_customer.address, address.latitude, address.longitude, address.region, address.region_group \
                        from commercial_customer LEFT JOIN address on commercial_customer.address = address.id) \
                        as t1 \
                        LEFT JOIN commercial_customer_storage rc on t1.ship_to_party = rc.commercial"
        # TODO : To add the below chunk once status_awsm has been clarified, now too many station inactive
        # where commercial_customer.status_awsm = 'Active' and commercial_customer_storage.terminal in ({})".format(terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)

        data_df = data_df.drop_duplicates(keep="first").reset_index(drop=True)
        convert_dict = {
            'ship_to_party': int,
        }
        data_df = data_df.astype(convert_dict)
        return data_df        

    # done
    @classmethod
    def get_dist_backup(cls):
        table_name = Config.TABLES['distance_backup']["tablename"]
        sql_statement = 'select * from `{}`'.format(table_name)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_dist_matrix(cls, region):
        table_name = Config.TABLES['openroute_distance']["tablename"]
        region = Config.MODEL_INPUTOUTPUT['region'][region]
        sql_statement = 'select * from `{}` where `Region` = \'{}\''.format(table_name, region)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # TODO : To check product_type (MULTIPROD etc)
    @classmethod
    def get_truck_data(cls, region=None, truck_list=None):
        table_name = Config.TABLES['truck_master']["tablename"]
        status_sap = Config.TABLES['truck_master']["columns"]["status_sap"]
        status_awsm = Config.TABLES['truck_master']["columns"]["status_awsm"]
        if region is not None:
            obj_1 = Config.TABLES['truck_master']["columns"]["terminal"]
            terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
            obj_2 = ','.join(f'"{i}"' for i in terminal_list)
        else:
            obj_1 = Config.TABLES['truck_master']["columns"]["vehicle_number"]
            obj_2 = ','.join(f'"{i}"' for i in truck_list)
        sql_statement = 'select * from {} where {} = "Active" and {} = "Active" and {} in ({})'.format(table_name,
                                                                                                       status_sap,
                                                                                                       status_awsm,
                                                                                                       obj_1, obj_2)
        data_df = DBConnector.load(table_name, sql_statement)

        # Add filter to remove all non MULTIPRD RTs
        data_df = data_df.query("product_type_sap == 'MULTIPRD'")
        
        return data_df

    @classmethod
    def get_truck_compartment(cls, truck_list):
        table_name = Config.TABLES['truck_compartment']["tablename"]
        truck_column = Config.TABLES['truck_compartment']["columns"]["road_tanker"]
        truck_list = ','.join(f'"{i}"' for i in truck_list)
        sql_statement = 'select sequence_no, max_volume, road_tanker from {} where {} in ({})'.format(table_name,
                                                                                                      truck_column,
                                                                                                      truck_list)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df          

    # TODO : To merge shift start and shift end based on user shift_type once data available in road_tanker_dates
    @classmethod
    def get_truck_dates(cls, region, truck_active_region_list):
        table_name = Config.TABLES['truck_master_dates']["tablename"]
        date_from = Config.TABLES['dates_range']["columns"]["date_from"]
        date_to = Config.TABLES['dates_range']["columns"]["date_to"]
        status_sap = Config.TABLES['truck_master']["columns"]["status_sap"]
        status_awsm = Config.TABLES['truck_master']["columns"]["status_awsm"]
        truck_id = ','.join(f'"{i}"' for i in truck_active_region_list)
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list_sql = ','.join(f'"{i}"' for i in terminal_list)
        sql_statement = 'select t1.field, t1.road_tanker, t1.date_range, t1.terminal, t1.shift_type, t1.id, t1.type, t1.time_from, t1.time_to, t1.days, t1.date_from, t1.date_to, rt.default_terminal from \
                        (select road_tanker_dates.field, road_tanker_dates.road_tanker, road_tanker_dates.date_range, road_tanker_dates.terminal, road_tanker_dates.shift_type, \
                        date_range.id, date_range.type, date_range.time_from, date_range.time_to, date_range.days, date_range.date_from, date_range.date_to \
                        from \
                        road_tanker_dates LEFT JOIN date_range on road_tanker_dates.date_range = date_range.id where road_tanker_dates.terminal in ({}) or road_tanker_dates.road_tanker in ({})) \
                        as t1 \
                        LEFT JOIN road_tanker rt \
                        on t1.road_tanker = rt.vehicle where rt.{} = "Active" and rt.{} = "Active"'.format(terminal_list_sql, truck_id, status_sap, status_awsm)

        data_df = DBConnector.load(table_name, sql_statement)

        # TODO : Uncomment to test scenario in case dates are switched over wrongly in DB
        # indices = [0]
        # data_df.loc[indices,'date_from'] = "2021-12-01"
        # data_df.loc[indices,'date_to'] = "2021-12-31"

        data_df = data_df[~data_df[date_from].isnull()]
        data_df = data_df[~((data_df['field'] == "shift type") & (data_df['terminal'].isnull()))]



  
        # TODO : Exclude "On" and "OH" in shift type until find out the exact purpose (because only default shift type for OH/Double tanker from tanker master is OH/On or OFF) and decide whether to include for first release
        data_df = data_df[~(data_df['shift_type'] == 'On')]
        data_df = data_df[~(data_df['shift_type'] == 'OH')]

        #data_df = data_df[(data_df['terminal'].isin(terminal_list)) | (data_df['terminal'].isnull())]
        data_df['terminal'] = np.where(data_df['terminal'].isnull(), data_df['default_terminal'], data_df['terminal'])
        # data_df = data_df[data_df['terminal'].isin(terminal_list)]

        def change_date_from_to(row):
            row[[date_from]] = pd.to_datetime(row[[date_from]].astype(str).str.strip(), format='%Y-%m-%d')
            row[[date_to]] = pd.to_datetime(row[[date_to]].astype(str).str.strip(), format='%Y-%m-%d')
            return row

        def change_date_from(row):
            row[[date_from]] = pd.to_datetime(row[[date_from]].astype(str).str.strip(), format='%Y-%m-%d')
            return row

        data_df = data_df.apply(lambda x: change_date_from_to(x) if x['type'] == 'range' else change_date_from(x), axis=1)
        data_df['remove_flag'] = np.where(data_df[date_from].isnull(), True,
                                          np.where(data_df[date_from] > data_df[date_to], True, False))
                                        
        data_df = data_df[data_df['remove_flag'] == False]
                
        # indices = [0]
        # data_df.loc[indices,'road_tanker'] = "AFJ 7732BC"

        return data_df

    # done
    @classmethod
    def get_retail_master(cls, region, order_retail_list, date):
        table_name = Config.TABLES['retail_customer_master']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        retail_list = ','.join(f'"{i}"' for i in order_retail_list)
        delivery_date = Config.TABLES['order_bank']["columns"]["delivery_date"]
        # sql_statement = "select retail_customer.ship_to_party, retail_customer.sold_to_party, retail_customer.status_awsm, retail_customer.road_tanker_accessibility, retail_customer.cloud, \
        #                 retail_customer.road_tanker_requirement, retail_customer_storage.terminal, retail_customer_storage.tank_capacity, retail_customer_storage.product, retail_customer_storage.distance \
        #                 from \
        #                 retail_customer LEFT JOIN retail_customer_storage on retail_customer.ship_to_party = retail_customer_storage.retail \
        #                 where retail_customer_storage.terminal in ({}) and retail_customer.ship_to_party in ({}) ".format(terminal_list, retail_list)
        sql_statement = "select t1.ship_to_party, t1.sold_to_party, t1.status_awsm, t1.road_tanker_accessibility, t1.cloud, \
                        t2.terminal, t2.ld_tag, t1.tank_capacity, t1.product, t1.distance from \
                        (select retail_customer.ship_to_party, retail_customer.sold_to_party, retail_customer.status_awsm, retail_customer.road_tanker_accessibility, retail_customer.cloud, \
                        retail_customer_storage.id, retail_customer_storage.terminal, retail_customer_storage.tank_capacity, retail_customer_storage.product, retail_customer_storage.distance \
                        from \
                        retail_customer LEFT JOIN retail_customer_storage on retail_customer.ship_to_party = retail_customer_storage.retail where retail_customer.ship_to_party in ({})) \
                        as t1 \
                        LEFT JOIN order_bank t2 \
                        on t1.id = t2.retail_storage where t2.requested_delivery_date = \'{}\' and t2.customer_type = 'RETAIL' and t2.terminal in ({})".format(retail_list, (date + timedelta(days=1)).strftime('%Y-%m-%d'), terminal_list)                         

        # TODO : To add the below chunk once status_awsm has been clarified, now too many station inactive
        # where retail_customer.status_awsm = 'Active' and retail_customer_storage.terminal in ({})".format(terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)
        data_df = data_df.drop_duplicates(subset=["ship_to_party", "status_awsm", "road_tanker_accessibility", "terminal", "product"], keep="first").reset_index(drop=True)

        # data_df = data_df[data_df.duplicated(subset = ['ship_to_party'], keep = False)]
        # data_df = data_df.sort_values(by=['ship_to_party'], ascending = True)

        # TODO : temporary fix no cloud station and once data source fixed need to remove this chunk
        # data_df = data_df[~data_df.isnull().any(axis=1)]
        data_df = data_df.replace(r'\s+', np.nan, regex=True).replace('', np.nan)
        data_df['cloud'].fillna(99, inplace=True)

        # data_df['road_tanker_requirement'].fillna("non_LD", inplace=True)

        # Impute ld tag of no value
        data_df['ld_tag'].fillna("non_LD", inplace=True)
        data_df['ld_tag'] = np.where(data_df['ld_tag'] == "", "non_LD", data_df['ld_tag'])

        convert_dict = {
            'status_awsm': str,
            'terminal': str,
            'ship_to_party': int,
            'product': int,
            'cloud': int,
            'ld_tag': str
        }
        data_df = data_df.astype(convert_dict)

        return data_df

    # done
    @classmethod
    def get_commercial_master(cls, region, date):
        table_name = Config.TABLES['commercial_customer_master']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        # delivery_date = Config.TABLES['order_bank']["columns"]["delivery_date"]

        # sql_statement = "select commercial_customer.ship_to_party, commercial_customer.sold_to_party, commercial_customer.status_awsm, commercial_customer.road_tanker_accessibility, \
        #                 commercial_customer.pump_type, commercial_customer_storage.terminal, commercial_customer_storage.tank_capacity, commercial_customer_storage.product, commercial_customer_storage.distance \
        #                 from \
        #                 commercial_customer LEFT JOIN commercial_customer_storage on commercial_customer.ship_to_party = commercial_customer_storage.commercial \
        #                 where commercial_customer_storage.terminal in ({})".format(terminal_list)
        
        sql_statement = "select t1.ship_to_party, t1.sold_to_party, t1.status_awsm, t1.road_tanker_accessibility, \
                        t1.pump_type, t1.id, t2.terminal, t2.ld_tag, t1.tank_capacity, t1.product, t1.distance from \
                        (select commercial_customer.ship_to_party, commercial_customer.sold_to_party, commercial_customer.status_awsm, commercial_customer.road_tanker_accessibility, \
                        commercial_customer.pump_type, commercial_customer_storage.id, commercial_customer_storage.terminal, commercial_customer_storage.tank_capacity, commercial_customer_storage.product, commercial_customer_storage.distance \
                        from \
                        commercial_customer LEFT JOIN commercial_customer_storage on commercial_customer.ship_to_party = commercial_customer_storage.commercial) \
                        as t1 \
                        LEFT JOIN order_bank t2\
                        on t1.id = t2.commercial_storage where t2.requested_delivery_date = \'{}\' and t2.order_type = 'COMM' and t2.terminal in ({})".format((date + timedelta(days=1)).strftime('%Y-%m-%d'), terminal_list) 

        # TODO : To add the below chunk once status_awsm has been clarified, now too many station inactive
        # where commercial_customer.status_awsm = 'Active' and commercial_customer_storage.terminal in ({})".format(terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)
        data_df = data_df.drop_duplicates(keep="first").reset_index(drop=True)

        # Impute ld tag of no value
        data_df['ld_tag'].fillna("non_LD", inplace=True)
        data_df['ld_tag'] = np.where(data_df['ld_tag'] == "", "non_LD", data_df['ld_tag'])

        convert_dict = {
            'status_awsm': str,
            'terminal': str,
            'ship_to_party': int,
            'product': int,
            'pump_type': str,
            'road_tanker_accessibility': int,
            'ld_tag': str
        }
        data_df = data_df.astype(convert_dict)
        return data_df

    # done
    @classmethod
    def row_prods_combo_check(cls, group_count_df, possible_row_prod_combo=[(1,1),(2,2),(3,3),(2,1),(3,2),(4,2),(4,3),(5,3)]):
        """
        Returns an accepted and rejected group_count to later be dealt with in preprocessing script(s) either raise Exception or log info

        :param group_count_df: DataFrame order_data grouped by Ship-to that has number_rows and number_material columns
        :param possible_row_prod_combo: List of tuples where tuple represents (number_row, number_materials) be called as "i_rows_j_prods" in preprocessing script(s)
        """
        keep_group_count = pd.DataFrame()
        for row_prod_combo in possible_row_prod_combo:
            number_row, number_prod = row_prod_combo
            accepted_combos = group_count_df.query(f"number_rows == {number_row} & number_material == {number_prod}").copy()
            keep_group_count = keep_group_count.append(accepted_combos)
        reject_group_count = group_count_df[~group_count_df.apply(tuple,1).isin(keep_group_count.apply(tuple,1))]
        return keep_group_count, reject_group_count    

    # TODO : To include additional no delivery interval after first release
    @classmethod
    def get_retail_dates(cls, retail_active_region_list):
        table_name = Config.TABLES['retail_dates']["tablename"]
        date_from = Config.TABLES['dates_range']["columns"]["date_from"]
        date_to = Config.TABLES['dates_range']["columns"]["date_to"]
        station_list = ','.join(f'"{i}"' for i in retail_active_region_list)
        sql_statement = "select retail_dates.id as date_id, retail_dates.field, retail_dates.retail, retail_dates.date_range, \
                        date_range.id as range_id, date_range.type, date_range.time_from, date_range.time_to, date_range.days, date_range.date_from, date_range.date_to \
                        from \
                        retail_dates LEFT JOIN date_range on retail_dates.date_range = date_range.id \
                        where retail_dates.retail in ({})".format(station_list)
        data_df = DBConnector.load(table_name, sql_statement)
        convert_dict = {
            'retail': int
        }
        data_df = data_df.astype(convert_dict)

        # Split retail station opening time and type 1 & 2 no delivery daily interval  
        retail_opening_time = data_df[data_df['field'] == "delivery open time"]
        retail_no_delivery = data_df[data_df['field'] == "no delivery interval"]

        # Pivot type 1 & 2 no delivery daily interval based on retail station id 
        retail_no_delivery = retail_no_delivery.sort_values('date_id').groupby('retail', as_index=False).head(2)

        tmp = []
        retail_no_delivery['idx'] = retail_no_delivery.groupby('retail').cumcount()
        for var in ['time_from', 'time_to']:
            retail_no_delivery['tmp_idx'] = var + '_' + retail_no_delivery['idx'].astype(str)
            tmp.append(retail_no_delivery.pivot(index='retail', columns='tmp_idx', values=var))
        retail_no_delivery = pd.concat(tmp, axis=1)
        retail_no_delivery.reset_index(inplace=True)

        # TODO : temporary replace time_from and time_to of same time to 00:00:00, remove this chunk when data issue resolved
        time_delta = min(retail_no_delivery['time_from_0'])
        retail_no_delivery.loc[retail_no_delivery['time_from_0'] == retail_no_delivery['time_to_0'], ['time_from_0', 'time_to_0']] = time_delta, time_delta
        retail_no_delivery.loc[retail_no_delivery['time_from_1'] == retail_no_delivery['time_to_1'], ['time_from_1', 'time_to_1']] = time_delta, time_delta        

        # Rename and join opening time and type 1 & 2 no delivery daily interval into single df
        retail_no_delivery = retail_no_delivery.rename(columns={"time_from_0": "no_delivery_start_1",
                                                                "time_from_1": "no_delivery_start_2",
                                                                "time_to_0": "no_delivery_end_1",
                                                                "time_to_1": "no_delivery_end_2"
                                                                })
        retail_opening_time = retail_opening_time.rename(columns={"time_from": "Opening_Time",
                                                                  "time_to": "Closing_Time"
                                                                  })
        data_df = pd.merge(retail_opening_time, retail_no_delivery, 'left', on='retail')
        return data_df


    # done
    @classmethod
    def multi_terminal_station_time_merge(
        cls, data_df, station_time_df, stn_data_terminal_col=None, terminal_95_col=None, terminal_97_col=None, product_col=None, data_df_stn_col='Destinations', station_time_stn_col='Ship-to'):
        """
        Merges order data and station_time according to the respective terminal. This is because when there exists multiple terminals, if merging by station ID only, it will create duplicates. 

        :param data_df: DataFrame to perform overwritting on for the specified columns.   
        :param station_time_df: DataFrame with station opening and closing time information. 
        :param ob_terminal_col: Reference column for the original terminal values from order_bank.
        :param terminal_95_col: Reference column for the terminal values for PRIMAX 95 from station_time.
        :param terminal_97_col: Reference column for the terminal values for PRIMAX 97 from station_time.
        :param product_col: Reference column for product/material column when performing merging. 
        """
        # orders with PRIMAX 95 or BIODIESEL B10
        subset_95_B10 = data_df[
            (data_df[product_col] == Config.DATA['PRODUCT_CODE']['PRIMAX_95']) | 
            (data_df[product_col] == Config.DATA['PRODUCT_CODE']['BIODIESEL_B10'])]
        subset_95_B10 = pd.merge(
            subset_95_B10, station_time_df, 'left', left_on=[data_df_stn_col, terminal_95_col], right_on=[station_time_stn_col, stn_data_terminal_col])
        # orders with PRIMAX 97
        subset_97 = data_df[
            (data_df[product_col] == Config.DATA['PRODUCT_CODE']['PRIMAX_97'])]
        subset_97 = pd.merge(
            subset_97, station_time_df, 'left', left_on=[data_df_stn_col, terminal_97_col], right_on=[station_time_stn_col, stn_data_terminal_col])
        data_df = pd.concat([subset_95_B10, subset_97], axis='index')
        
        return data_df


    # done
    @classmethod    
    def check_product_terminal_restriction(cls, order_data_df, terminal_supply_prod):
        """
        Returns an accepted and rejected orders due to inconsistencies of order_data with business logic to later be 
        dealt with in preprocessing script(s) either raise Exception or log info
        Note: This is a hard constraint such that it will "reject" orders of product_id provided in tuple coming from terminals 
        other than the provided terminal due to data inconsistencies.
        
        :param order_data_df: DataFrame raw order_data from order_bank where 'terminal' and 'Material' columns need to be present
        :param terminal_supply_prod: List of tuples(str, int) where it represents the only terminal supplying the relevant product
        For example [(['M838'], 70000011), (['M838'], 70100346)]
        """
        reject_orders = pd.DataFrame()
        for only_terminal_supplying_product in terminal_supply_prod:
            terminal_id, product_id = only_terminal_supplying_product
            subset_orders_of_prod = order_data_df.loc[order_data_df['Material']==product_id]
            rejected_orders = subset_orders_of_prod[~subset_orders_of_prod['terminal'].isin(terminal_id)]
            reject_orders = reject_orders.append(rejected_orders)
        accept_orders = order_data_df[~order_data_df[['Ship-to', 'Material', 'terminal', 'Qty', 'long_distance']].apply(tuple,1).isin(reject_orders[['Ship-to', 'Material', 'terminal', 'Qty', 'long_distance']].apply(tuple,1))]
        return reject_orders, accept_orders


    # TODO : To include additional no delivery interval after first release
    @classmethod
    def get_commercial_dates(cls, commercial_active_region_list):
        table_name = Config.TABLES['commercial_dates']["tablename"]
        date_from = Config.TABLES['dates_range']["columns"]["date_from"]
        date_to = Config.TABLES['dates_range']["columns"]["date_to"]
        station_list = ','.join(f'"{i}"' for i in commercial_active_region_list)
        sql_statement = "select commercial_dates.id as date_id, commercial_dates.field, commercial_dates.commercial, commercial_dates.date_range, \
                        date_range.id as range_id, date_range.type, date_range.time_from, date_range.time_to, date_range.days, date_range.date_from, date_range.date_to \
                        from \
                        commercial_dates LEFT JOIN date_range on commercial_dates.date_range = date_range.id \
                        where commercial_dates.commercial in ({})".format(station_list)
        data_df = DBConnector.load(table_name, sql_statement)
        convert_dict = {
            'commercial': int
        }
        data_df = data_df.astype(convert_dict)

        # Split commercial location opening time and type 1 & 2 no delivery daily interval  
        commercial_opening_time = data_df[data_df['field'] == "delivery open time"]
        commercial_no_delivery = data_df[data_df['field'] == "no delivery interval"]

        # Pivot type 1 & 2 no delivery daily interval based on commercial location id 
        commercial_no_delivery = commercial_no_delivery.sort_values('date_id').groupby('commercial', as_index=False).head(2)

        tmp = []
        commercial_no_delivery['idx'] = commercial_no_delivery.groupby('commercial').cumcount()
        for var in ['time_from', 'time_to']:
            commercial_no_delivery['tmp_idx'] = var + '_' + commercial_no_delivery['idx'].astype(str)
            tmp.append(commercial_no_delivery.pivot(index='commercial', columns='tmp_idx', values=var))
        commercial_no_delivery = pd.concat(tmp, axis=1)
        commercial_no_delivery.reset_index(inplace=True)

        # TODO : temporary replace time_from and time_to of same time to 00:00:00, remove this chunk when data issue resolved
        time_delta = min(commercial_no_delivery['time_from_0'])
        commercial_no_delivery.loc[commercial_no_delivery['time_from_0'] == commercial_no_delivery['time_to_0'], ['time_from_0', 'time_to_0']] = time_delta, time_delta
        commercial_no_delivery.loc[commercial_no_delivery['time_from_1'] == commercial_no_delivery['time_to_1'], ['time_from_1', 'time_to_1']] = time_delta, time_delta

        # Rename and join opening time and type 1 & 2 no delivery daily interval into single df
        commercial_no_delivery = commercial_no_delivery.rename(columns={"time_from_0": "no_delivery_start_1",
                                                                "time_from_1": "no_delivery_start_2",
                                                                "time_to_0": "no_delivery_end_1",
                                                                "time_to_1": "no_delivery_end_2"
                                                                })
        commercial_opening_time = commercial_opening_time.rename(columns={"time_from": "Opening_Time",
                                                                  "time_to": "Closing_Time"
                                                                  })
        data_df = pd.merge(commercial_opening_time, commercial_no_delivery, 'left', on='commercial')
        # data_df = data_df.rename(columns={"commercial": "Ship To"})
        return data_df        

    # done
    @classmethod
    def get_state_region(cls, order_data, region):
        """ 
        Returns order_data with state and region (from 'address' table) appended to order_data 
        for any further filtering purposes.

        :param order_data: DataFrame preprocessed order data
        :param region: str 'Eastern', 'Central', 'Northern', 'Southern'
        """
        all_customers_list = order_data['Ship-to'].unique() # getting only unique list of customers
        all_customers_list_sql = ','.join(f'{i}' for i in all_customers_list)
        table_name = Config.TABLES['retail_customer_master']["tablename"]
        address_table = Config.TABLES['address']['tablename']
        sql_statement = "select ship_to_party, address, address_table.state, address_table.region_group from retail_customer rc \
                        LEFT JOIN (select id, state, region_group from address where region_group IN ('{}')) \
                        as address_table on address_table.id = rc.address where rc.ship_to_party IN ({})".format(region, all_customers_list_sql)
        data_df = DBConnector.load(table_name, sql_statement)
        convert_dict = {
            'ship_to_party': int
        }
        data_df = data_df.astype(convert_dict)
        order_data = order_data.merge(data_df, left_on='Ship-to', right_on='ship_to_party')
        return order_data
    
    # done
    # To subset Ulu Bernam stations in order processing northern by city 
    @classmethod
    def get_city_region(cls, order_data, region):
        """ 
        Returns order_data with city and region (from 'address' table) appended to order_data 
        for any further filtering purposes of Ulu Bernam city.

        :param order_data: DataFrame preprocessed order data
        :param region: str 'Eastern', 'Central', 'Northern', 'Southern'
        """
        all_customers_list = order_data['Ship-to'].unique() # getting only unique list of customers
        all_customers_list_sql = ','.join(f'{i}' for i in all_customers_list)
        table_name = Config.TABLES['retail_customer_master']["tablename"]
        address_table = Config.TABLES['address']['tablename']
        sql_statement = "select ship_to_party, address, address_table.city, address_table.region_group from retail_customer rc \
                        LEFT JOIN (select id, city, region_group from address where region_group IN ('{}')) \
                        as address_table on address_table.id = rc.address where rc.ship_to_party IN ({})".format(region, all_customers_list_sql)
        data_df = DBConnector.load(table_name, sql_statement)
        convert_dict = {
            'ship_to_party': int
        }
        data_df = data_df.astype(convert_dict)
        order_data = order_data.merge(data_df, left_on='Ship-to', right_on='ship_to_party')
        return order_data

    # done
    # To subset only site_id = RYB0424 for Ulu Bernam station later in order preprocessing northern
    @classmethod
    def get_site_id(cls, order_data):
        """ 
        Returns order_data with site_id (from 'retail customer' table) appended to order_data 
        for any further filtering purposes.

        :param order_data: DataFrame preprocessed order data
        """
        all_customers_list = order_data['Ship-to'].unique() # getting only unique list of customers
        all_customers_list_sql = ','.join(f'{i}' for i in all_customers_list)
        table_name = Config.TABLES['retail_customer_master']["tablename"]
        sql_statement = "select ship_to_party, site_id from retail_customer rc \
                        where rc.ship_to_party IN ({})".format(all_customers_list_sql) 
        data_df = DBConnector.load(table_name, sql_statement)
        convert_dict = {
            'ship_to_party': int
        }
        data_df = data_df.astype(convert_dict)
        order_data = order_data.merge(data_df, left_on='Ship-to', right_on='ship_to_party')
        order_data = order_data.drop(['ship_to_party'], axis=1)
        return order_data


    # done
    @classmethod
    def get_state_from_rc(cls, ship_to_party_no):
        """
        Returns just the state of the ship_to_party from retail_customer table
        """
        table_name = Config.TABLES['retail_customer_master']["tablename"]
        sql_statement = "select address_table.state from retail_customer rc LEFT JOIN (select id, state, region_group from address) \
                        as address_table on address_table.id = rc.address where rc.ship_to_party = {}".format(int(ship_to_party_no))
        state = DBConnector.load(table_name, sql_statement)
        return state.values[0][0]
        
    # done
    @classmethod
    def get_dmr_output_data(cls, order_id, region, date):
        table_name = Config.TABLES['order_bank']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        order_id = ','.join(f'{i}' for i in order_id)
        # delivery_date = Config.TABLES['order_bank']["columns"]["delivery_date"]
        sql_statement = "select t1.id, t1.customer_type, t1.volume, t1.priority, t1.requested_delivery_date, t1.order_type, t1.retain, t1.runout, t1.multi_load_id, t1.terminal, \
                        t1.forecast_sales_d1, t1.opening_inventory_d1, t1.tank_capacity, t1.retail, t1.product, t1.distance, t1.soft_restriction, t1.shipment, t1.ld_tag, rc.cloud, rc.road_tanker_accessibility from \
                        (select order_bank.id, order_bank.customer_type, order_bank.volume, order_bank.priority, order_bank.requested_delivery_date, order_bank.order_type, order_bank.retain, order_bank.runout, \
                        order_bank.multi_load_id, order_bank.terminal, order_bank.forecast_sales_d1, order_bank.opening_inventory_d1, order_bank.soft_restriction, order_bank.shipment, order_bank.ld_tag, \
                        retail_customer_storage.tank_capacity, retail_customer_storage.retail, retail_customer_storage.product, retail_customer_storage.distance \
                        from \
                        order_bank LEFT JOIN retail_customer_storage on order_bank.retail_storage = retail_customer_storage.id where order_bank.customer_type = 'RETAIL' and order_bank.id in ({}) and order_bank.requested_delivery_date = \'{}\') \
                        as t1 \
                        LEFT JOIN retail_customer rc \
                        on t1.retail = rc.ship_to_party and t1.terminal in ({})".format(order_id, (date + timedelta(days=1)).strftime('%Y-%m-%d'), terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)
        data_df = data_df.drop(['distance', 'cloud', 'road_tanker_accessibility'], axis=1)
        
        # IMPORTANT : adjust runout 00:00:00 to 23:59:59, avoid same time 00:00:00 as retain of the same date
        mask = data_df['runout'].dt.time == pd.to_datetime('00:00:00').time()
        data_df.loc[mask, 'runout'] = data_df['runout'].dt.normalize() + pd.Timedelta('23:59:00')         

        # Reassign multiload tag based on station not order wise
        multiload_pairs = data_df[data_df['multi_load_id'] == 'MULTILOAD'].drop_duplicates(['retail', 'multi_load_id'])[
            ['retail', 'multi_load_id']]
        data_df = pd.merge(data_df, multiload_pairs, 'left', on='retail')
        data_df['multi_load_id_x'] = np.where(data_df['order_type'] == 'SMP', "NORMAL", data_df['multi_load_id_x'])
        data_df['multi_load_id_x'] = np.where(
            (data_df['multi_load_id_y'] == 'MULTILOAD') & (data_df['order_type'] == 'ASR'), data_df['multi_load_id_y'],
            data_df['multi_load_id_x'])
        data_df = data_df.rename(columns={'multi_load_id_x': 'multi_load_id'})
        data_df.drop('multi_load_id_y', axis=1, inplace=True)
        data_df.multi_load_id = data_df.multi_load_id.replace({'': "NORMAL"})
        data_df.multi_load_id = data_df.multi_load_id.fillna('NORMAL')
        data_df['multi_load_id'] = data_df['multi_load_id'].replace({'NORMAL': 'Normal', 'MULTILOAD': 'Multiload'})

        # Standardize data type
        data_df['requested_delivery_date'] = pd.to_datetime(data_df['requested_delivery_date'],
                                                            format='%Y-%m-%d %H:%M:%S')
        # Impute ld tag of no value
        data_df['ld_tag'].fillna("non_LD", inplace=True)
        data_df['ld_tag'] = np.where(data_df['ld_tag'] == "", "non_LD", data_df['ld_tag'])

        convert_dict = {'customer_type': str,
                        'volume': int,
                        'order_type': str,
                        'multi_load_id': str,
                        'terminal': str,
                        'retail': int,
                        'product': int,
                        'ld_tag': str,
                        }
        data_df = data_df.astype(convert_dict)

        # Subset order with volume
        data_df = data_df[data_df['volume'] > 0]

        def renaming_product_id(df, column):
            """Renaming product ID to follow the config file."""
            res = dict((k, v) for k, v in Config.DATA['PRODUCT_ID'].items())
            for k, v in Config.DATA['PRODUCT_ID'].items():
                df[column] = df[column].replace(k, v, regex=True)
            return df

        data_df = renaming_product_id(data_df, "product")

        # Remove locked orders based on soft restriction records
        data_df = data_df[data_df['soft_restriction'].isnull()]
        # Remove shipment based on shipment no records
        data_df = data_df[data_df['shipment'].isnull()]
        # Impute NULL priority to Normal
        data_df['priority'].fillna("Normal", inplace=True)

        # Add 1 hour to 2nd multiload order of same station - product with similar retain
        # data_df['check_duplicate'] = data_df.groupby(['retail','product','multi_load_id','retain','runout']).cumcount() 
        # duplicate_check = data_df[['id','retail','product','check_duplicate']]
        # for _, val in duplicate_check.iterrows():
        #     data_df['retain'] = np.where((data_df['id'] == val['id']) & (data_df['product'] == val['product']), data_df['retain'] + timedelta(hours=int(val['check_duplicate'])), data_df['retain'])
        
        return data_df

    # done
    @classmethod
    def get_commercial_data(cls, order_id, region, date):
        table_name = Config.TABLES['order_bank']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        order_id = ','.join(f'{i}' for i in order_id)
        # delivery_date = Config.TABLES['order_bank']["columns"]["delivery_date"]
        sql_statement = "select t1.id, t1.customer_type, t1.volume, t1.priority, t1.requested_delivery_date, t1.order_type, t1.retain, t1.runout, t1.terminal, \
                        t1.soft_restriction, t1.tank_capacity, t1.commercial, t1.product, t1.shipment, t1.ld_tag, cc.cloud, cc.road_tanker_accessibility, cc.pump_type from  \
                        (select order_bank.id, order_bank.customer_type, order_bank.volume, order_bank.priority, order_bank.requested_delivery_date, order_bank.order_type, order_bank.retain,  \
                        order_bank.runout, order_bank.terminal, order_bank.soft_restriction, order_bank.shipment, order_bank.ld_tag, \
                        commercial_customer_storage.tank_capacity, commercial_customer_storage.commercial, commercial_customer_storage.product  \
                        from \
                        order_bank LEFT JOIN commercial_customer_storage on order_bank.commercial_storage = commercial_customer_storage.id where order_bank.customer_type = 'COMMERCIAL' and order_bank.id in ({}) and order_bank.requested_delivery_date = \'{}\') \
                        as t1 \
                        LEFT JOIN commercial_customer cc \
                        on t1.commercial = cc.ship_to_party and t1.terminal in ({})".format(order_id, (date + timedelta(days=1)).strftime('%Y-%m-%d'), terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)
        data_df = data_df.drop(['cloud', 'road_tanker_accessibility'], axis=1)

        # IMPORTANT : adjust runout 00:00:00 to 23:59:59, avoid same time 00:00:00 as retain of the same date
        mask = data_df['runout'].dt.time == pd.to_datetime('00:00:00').time()
        data_df.loc[mask, 'runout'] = data_df['runout'].dt.normalize() + pd.Timedelta('23:59:59')        

        # Standardize data type
        data_df['requested_delivery_date'] = pd.to_datetime(data_df['requested_delivery_date'],
                                                            format='%Y-%m-%d %H:%M:%S')

        # Impute ld tag of no value
        data_df['ld_tag'].fillna("non_LD", inplace=True)
        data_df['ld_tag'] = np.where(data_df['ld_tag'] == "", "non_LD", data_df['ld_tag'])

        convert_dict = {'id': int,
                        'customer_type': str,
                        'volume': int,
                        'order_type': str,
                        'terminal': str,
                        'commercial': int,
                        'product': int,
                        'pump_type': str,
                        'ld_tag': str,
                        }
        data_df = data_df.astype(convert_dict)

        # Subset order with volume
        data_df = data_df[data_df['volume'] > 0]

        def renaming_product_id(df, column):
            """Renaming product ID to follow the config file."""
            res = dict((k, v) for k, v in Config.DATA['PRODUCT_ID'].items())
            for k, v in Config.DATA['PRODUCT_ID'].items():
                df[column] = df[column].replace(k, v, regex=True)
            return df

        data_df = renaming_product_id(data_df, "product")

        # Remove locked orders based on soft restriction records
        data_df = data_df[data_df['soft_restriction'].isnull()]
        # Remove shipment based on shipment no records
        data_df = data_df[data_df['shipment'].isnull()]

        # TODO : Temporary remove bulk orders of commercial
        # data_df = data_df.drop_duplicates(subset=['commercial','product','volume','retain'],keep="first").reset_index(drop=True)

        # TODO : Temporary remove orders with NULL terminal and tank capacity in commercial customer storage until problem resolved
        # data_df = data_df[~data_df['tank_capacity'].isnull()]

        return data_df        

    # done
    @classmethod
    def get_locked_orders(cls, order_id, region, date):
        table_name = Config.TABLES['order_bank']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        order_id = ','.join(f'{i}' for i in order_id)
        # delivery_date = Config.TABLES['order_bank']["columns"]["delivery_date"]
        sql_statement = "select t1.id, t1.route_id, t1.vehicle, t1.planned_load_time, t1.planned_end_time, t1.trip_start_time, t1.soft_restriction, t1.customer_type, t1.volume, t1.priority, t1.requested_delivery_date, t1.order_type, t1.retain, t1.runout, \
                        t1.multi_load_id, t1.terminal, t1.forecast_sales_d1, t1.opening_inventory_d1, t1.tank_capacity, t1.retail, t1.product, t1.distance, t1.shipment, t1.is_locked_order, rc.cloud, rc.road_tanker_accessibility from  \
                        (select order_bank.id, order_bank.customer_type, order_bank.volume, order_bank.priority, order_bank.requested_delivery_date, order_bank.order_type, order_bank.retain, order_bank.runout, \
                        order_bank.multi_load_id, order_bank.terminal, order_bank.forecast_sales_d1, order_bank.opening_inventory_d1, order_bank.route_id, order_bank.vehicle, order_bank.planned_load_time, order_bank.planned_end_time, order_bank.trip_start_time, \
                        order_bank.soft_restriction, order_bank.shipment, order_bank.is_locked_order, retail_customer_storage.tank_capacity, retail_customer_storage.retail, retail_customer_storage.product, retail_customer_storage.distance \
                        from \
                        order_bank LEFT JOIN retail_customer_storage on order_bank.retail_storage = retail_customer_storage.id where order_bank.id in ({})) \
                        as t1 \
                        LEFT JOIN retail_customer rc \
                        on t1.retail = rc.ship_to_party".format(order_id)
        data_df = DBConnector.load(table_name, sql_statement)
        data_df = data_df.drop(['distance', 'cloud', 'road_tanker_accessibility'], axis=1)

        # Filter locked orders based on is_locked_order and shipment
        # data_df = data_df[~(data_df['shipment'].isnull()) | (data_df['is_locked_order'] == 1)]
        data_df = data_df[data_df['is_locked_order'] == 1]

        delivery_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        if len(data_df) > 0:
            data_df['date'] = data_df['trip_start_time'].dt.date
            data_df['date'] = data_df['trip_start_time'].dt.normalize()
            data_df = data_df[data_df['date']==delivery_date]
            locked_id = data_df['id'].unique().ravel().tolist()
            data_df.reset_index(inplace=True)
            data_df = data_df[['id', 'route_id', 'vehicle', 'trip_start_time', 'planned_end_time']]
            data_df = data_df.drop_duplicates(subset=['vehicle', 'trip_start_time', 'planned_end_time'], keep="first")

            # Remove duplicate time intervals to get unique tanker wise trip_start_time and end time
            data_df['count'] = data_df.apply(lambda row: len(data_df[(((
                                                                               row.trip_start_time <= data_df.trip_start_time) & (
                                                                               data_df.trip_start_time <= row.planned_end_time)) \
                                                                      | ((
                                                                                 data_df.trip_start_time <= row.trip_start_time) & (
                                                                                 row.trip_start_time <= data_df.planned_end_time)))
                                                                     & (row.id != data_df.id) & (
                                                                             row.vehicle == data_df.vehicle)]),
                                             axis=1)

            data_df['planned_end_time_max'] = data_df[data_df['count'] > 0].groupby(['vehicle'])[
                'planned_end_time'].transform(max)
            data_df['planned_end_time'] = np.where(data_df['planned_end_time_max'].isnull(),
                                                   data_df['planned_end_time'], data_df['planned_end_time_max'])

            data_df = data_df.rename(columns={'vehicle': 'Vehicle Number', 'trip_start_time': 'Time start route',
                                              'planned_end_time': 'Time complete route'})
            data_df = data_df[['Vehicle Number', 'Time start route', 'Time complete route']]

        else:
            data_df = pd.DataFrame()
            locked_id = []

        return locked_id, data_df

    # done
    @classmethod
    def get_forecast_data(cls, order_id, date):
        table_name = Config.TABLES['order_bank']["tablename"]
        order_id = ','.join(f'{i}' for i in order_id)
        sql_statement = "select order_bank.forecast_sales_d1,  \
                        retail_customer_storage.retail, retail_customer_storage.product  \
                        from  \
                        order_bank LEFT JOIN retail_customer_storage on order_bank.retail_storage = retail_customer_storage.id \
                        where order_bank.customer_type = 'RETAIL' and order_bank.order_type = 'ASR' and order_bank.id in ({}) and order_bank.requested_delivery_date = \'{}\'".format(
            order_id, (date + timedelta(days=1)).strftime('%Y-%m-%d'))
        data_df = DBConnector.load(table_name, sql_statement)
        convert_dict = {'retail': int,
                        'product': int
                        }
        data_df = data_df.astype(convert_dict)
        data_df = data_df.rename(columns={'retail': 'id'})

        def renaming_product_id(df, column):
            """Renaming product ID to follow the config file."""
            res = dict((k, v) for k, v in Config.DATA['PRODUCT_ID'].items())
            for k, v in Config.DATA['PRODUCT_ID'].items():
                df[column] = df[column].replace(k, v, regex=True)
            return df

        data_df = renaming_product_id(data_df, "product")
        return data_df

    # done
    @classmethod
    def get_inventory_data(cls, order_id, date):
        table_name = Config.TABLES['order_bank']["tablename"]
        order_id = ','.join(f'{i}' for i in order_id)
        sql_statement = "select order_bank.opening_inventory_d1,  \
                        retail_customer_storage.retail, retail_customer_storage.product  \
                        from  \
                        order_bank LEFT JOIN retail_customer_storage on order_bank.retail_storage = retail_customer_storage.id \
                        where order_bank.customer_type = 'RETAIL' and order_bank.order_type = 'ASR' and order_bank.id in ({}) and order_bank.requested_delivery_date = \'{}\'".format(
            order_id, (date + timedelta(days=1)).strftime('%Y-%m-%d'))
        data_df = DBConnector.load(table_name, sql_statement)
        convert_dict = {'retail': int,
                        'product': int
                        }
        data_df = data_df.astype(convert_dict)
        data_df = data_df.rename(columns={'retail': 'id'})

        def renaming_product_id(df, column):
            """Renaming product ID to follow the config file."""
            res = dict((k, v) for k, v in Config.DATA['PRODUCT_ID'].items())
            for k, v in Config.DATA['PRODUCT_ID'].items():
                df[column] = df[column].replace(k, v, regex=True)
            return df

        data_df = renaming_product_id(data_df, "product")
        return data_df

    @classmethod
    def get_opo_output_data(cls, import_settings, region, date):

        table_name = import_settings['Tables']['opo_output']['tablename']
        sql_statement = 'select * from `OPO_output` where `Region` = \'{}\' and `Delivery Date` = \'{}\''.format(region,
                                                                                                                 date + timedelta(
                                                                                                                     days=1))
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    @classmethod
    def get_dmr_output_data_old(cls, region, date):

        table_name = "rts_dmr_order"
        sql_statement = 'select * from `{}` where `Region` = \'{}\' and `Delivery Date` = \'{}\''.format(table_name,
                                                                                                         region,
                                                                                                         date + timedelta(
                                                                                                             days=1))
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_processed_ori_orders(cls, txn_id):

        table_name = Config.TABLES['processed_ori_orders']['tablename']
        sql_statement = "select * from `{}` where `Txn_ID` =\'{}\'".format(table_name, txn_id)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_unscheduled_data(cls, previous_capacity_loop, txn_id):

        table_name = Config.TABLES['unscheduled_data']["tablename"]
        sql_statement = "select * from `{}` where `Loop_Counter` = \'{}\' and `Txn_ID` =\'{}\'".format(table_name,
                                                                                                       previous_capacity_loop,
                                                                                                       txn_id)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_unmatched_data(cls, previous_capacity_loop, txn_id):

        table_name = Config.TABLES['unmatched_data']['tablename']
        sql_statement = "select * from `{}` where `Loop_Counter` = \'{}\' and `Txn_ID` =\'{}\'".format(table_name,
                                                                                                       previous_capacity_loop,
                                                                                                       txn_id)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_scheduled_data(cls, txn_id):

        table_name = Config.TABLES['scheduled_data']['tablename']
        sql_statement = "select * from `{}` where `Txn_ID` =\'{}\'".format(table_name, txn_id)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_retain_data(cls, region):
        region = Config.MODEL_INPUTOUTPUT['retain_region'][region]
        table_name = Config.TABLES['retain_data']["tablename"]
        region_column = Config.TABLES['retain_data']["columns"]["region"]
        sql_statement = "select * from `{}` where `{}` = \'{}\'".format(table_name, region_column, region)
        data_df = DBConnector.load(table_name, sql_statement)
        data_df['perct sales rate'] = data_df['perct sales rate'] / 100
        return data_df

    # done
    @classmethod
    def get_cross_cloud_paring_data(cls, region):
        table_name = Config.TABLES['retail_customer_master']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        sql_statement = "select retail_customer.ship_to_party, retail_customer.status_awsm, retail_customer.alternative_cluster, \
                        retail_customer_storage.terminal \
                        from \
                        retail_customer LEFT JOIN retail_customer_storage on retail_customer.ship_to_party = retail_customer_storage.retail \
                        where retail_customer_storage.terminal in ({})".format(terminal_list)
        # TODO : To add the below chunk once status_awsm has been clarified, now too many station inactive
        # where retail_customer.status_awsm = 'Active' and retail_customer_storage.terminal in ({})".format(terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)

        data_df = data_df[~data_df.isnull().any(axis=1)]
        data_df = data_df.replace(r'\s+', np.nan, regex=True).replace('', np.nan)
        data_df['alternative_cluster'] = data_df['alternative_cluster'].str.replace('cc', '', regex=True)
        data_df['alternative_cluster'] = data_df['alternative_cluster'].str.replace('c', '', regex=True)

        convert_dict = {
            'status_awsm': str,
            'terminal': str,
            'ship_to_party': int
        }
        data_df = data_df.astype(convert_dict)
        data_df = data_df.drop_duplicates(keep="first").reset_index(drop=True)
        return data_df

    # done
    @classmethod
    def get_product_density_data(cls, region):
        table_name = Config.TABLES['terminal_storage']["tablename"]
        terminal_list = Config.DATA['SUPPLY_SOURCES'][region]['terminal_id']
        terminal_list = ','.join(f'"{i}"' for i in terminal_list)
        sql_statement = "select * from `{}` where `terminal` in ({})".format(table_name, terminal_list)
        data_df = DBConnector.load(table_name, sql_statement)

        # Have to convert product to integer before mapping
        convert_dict = {
            'product': int
        }
        data_df = data_df.astype(convert_dict)

        def renaming_product_id(df, column):
            """Renaming product ID to follow the config file."""
            res = dict((k, v) for k, v in Config.DATA['PRODUCT_ID'].items())
            for k, v in Config.DATA['PRODUCT_ID'].items():
                df[column] = df[column].replace(k, v, regex=True)
            return df

        data_df = renaming_product_id(data_df, "product")

        data_df = data_df.rename(columns={'product': 'Product', 'terminal_product_density': 'Density_values'})
        data_df['Density_values'] = data_df['Density_values'] / 1000
        return data_df

    # done
    @classmethod
    def get_route_output_data(cls, txn_id, loop_status):

        table_name = Config.TABLES['route_output']["tablename"]
        sql_statement = 'select * from `{}` where `Txn_ID` = \'{}\' and `Loop_Counter` = \'{}\' '.format(table_name,
                                                                                                         txn_id,
                                                                                                         loop_status)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    # done
    @classmethod
    def get_updated_truck_data(cls, txn_id, previous_loop, capacity):

        table_name = Config.TABLES['updated_truck']["tablename"]
        sql_statement = 'select * from `{}` where `Txn_ID` = \'{}\' and `Loop_Counter` = \'{}\' '.format(table_name,
                                                                                                         txn_id,
                                                                                                         previous_loop)
        data_df = DBConnector.load(table_name, sql_statement)

        data_df = data_df[data_df['Vehicle maximum vol.'].isin(capacity)]
        truck_capacity = data_df['Vehicle maximum vol.'].unique()

        return data_df, truck_capacity

    # done
    @classmethod
    def get_openroute_distance(cls, region):
        table_name = Config.TABLES['openroute_distance']["tablename"]
        region = Config.MODEL_INPUTOUTPUT['region'][region]
        sql_statement = 'select * from {} where "Region" = \'{}\''.format(table_name, region)
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    @classmethod
    def get_delivery_note(cls, import_settings, region, date):
        table_name = import_settings['Tables']['delivery_note']['tablename']
        sql_statement = 'select * from {} where `Region` = \'{}\' and `PlnStLdDt` =\'{}\' '.format(table_name, region,
                                                                                                   date + timedelta(
                                                                                                       days=1))
        data_df = DBConnector.load(table_name, sql_statement)
        return data_df

    

if __name__ == "__main__":
    truck_data = InputHandler.get_product_density_data(
        Config.MODEL_INPUTOUTPUT["import_settings"].get(Config.MODEL_INPUTOUTPUT['import_from']), 'central')
    print(truck_data.head(5))
