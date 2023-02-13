import logging
import sys
import pyodbc
import pandas as pd


# set logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('extractdata.log')
file_handler.setFormatter(formatter)
stream_handler =logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# credentials to SOFEIA server 
server = 'ptazsg-4feiatestdb02.database.windows.net'
database = 'PIData'
username = 'sofeiauser'
password = 'Welcome@12345'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server +
                    ';DATABASE='+database+';UID='+username+';PWD=' + password)

# cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server +
#                      ';DATABASE='+database+';UID='+username+';PWD=' + password)

#select column based on furnace id (F-21201, F-21202, F-21251, F21252 in manner) 
sql_query = "SELECT [TimeStamp],[FEIA_F-21201_Efficiency_Actual],[21FC082.DACA.PV],[21FC089.DACA.PV],[21TC035.DACA.PV],[21FC084X.PV],[21FC087X.PV],[21QC001.DACA.PV]\
            ,[FEIA_F-21202_Efficiency_Actual],[21FC217.DACA.PV],[21FC218.DACA.PV],[21FC219.DACA.PV],[21FC220.DACA.PV],[21TC218.DACA.PV],[21PC117.PIDA.PV],[21QI051B.DACA.PV]\
            ,[FEIA_F-21251_Efficiency_Actual],[21TC527.DACA.PV],[21FC561.DACA.PV],[21FC607.DACA.PV],[21FC580.DACA.PV],[21FI609.DACA.PV],[21QC501.DACA.PV]\
            ,[FEIA_F-21252_Efficiency_Actual],[21PC913.PIDA.PV],[21QRI900.DACA.PV]\
            FROM PIData.dbo.SOFEIA_MRC_Hourly\
            WHERE [TimeStamp] >= '2020-01-01'\
            ORDER BY [TimeStamp]"

get_data = pd.read_sql_query(sql_query, cnxn)
pd.set_option('display.max_columns', 500)
logger.info('Get new data is completed. ')
