
import pyodbc
import pandas as pd

# credentials to SOFEIA server 
server = 'ptazsg-4feiatestdb02.database.windows.net'
database = 'PIData'
username = 'sofeiauser'
password = 'Welcome@12345'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server +
                      ';DATABASE='+database+';UID='+username+';PWD=' + password)

#select column based on furnace id
sql_query = "SELECT [TimeStamp],[FEIA_F-21202_Efficiency_Actual],[21FC217.DACA.PV],[21FC218.DACA.PV],[21FC219.DACA.PV],[21FC220.DACA.PV],[21TC218.DACA.PV],[21PC117.PIDA.PV]\
            FROM PIData.dbo.SOFEIA_MRC_Hourly\
            WHERE [TimeStamp] >= '2020-01-01'\
            ORDER BY [TimeStamp]"

get_data = pd.read_sql_query(sql_query, cnxn)
pd.set_option('display.max_columns', 500)
print('Get new data is completed')

