import pandas as pd
import numpy as np

df = pd.read_csv("../input/input_data_f21202.csv")

class DataPreprocessing:

    def __init__(self, data) -> pd.DataFrame:
        self.data = data

    def clean_data(data):
        data = data.dropna(axis=0, how='any', thresh=None, subset=None)
        data = data[(data['Actual_Efficiency'] >= 0) &
                    (data['Actual_Efficiency'] <= 100)]
        
        # remove the plant trips
        data = data[(data['DateTime'] < '2020-02-20 10:00:00') | (data['DateTime'] >= '2020-02-20 17:00:00')]
        data = data[(data['DateTime'] < '2020-04-03 09:00:00') | (data['DateTime'] >= '2020-04-06 14:00:00')]
        data = data[(data['DateTime'] < '2020-07-09 14:00:00') | (data['DateTime'] >= '2020-07-10 01:00:00')]
        data = data[(data['DateTime'] < '2020-08-08 04:00:00') | (data['DateTime'] >= '2020-08-09 04:00:00')]
        data = data[(data['DateTime'] < '2020-09-07 08:00:00') | (data['DateTime'] >= '2020-09-08 00:00:00')]
        data = data[(data['DateTime'] < '2020-11-08 02:00:00') | (data['DateTime'] >= '2020-11-09 21:00:00')]
        data = data[(data['DateTime'] < '2021-02-10 20:00:00') | (data['DateTime'] >= '2021-02-12 10:00:00')]
        data = data[(data['DateTime'] <= '2021-02-23 11:00:00')| (data['DateTime'] >= '2021-02-25 02:00:00')]
        data = data[(data['DateTime'] <= '2021-06-26 10:00:00') | (data['DateTime'] >= '2021-06-29 02:00:00')] # remove high error in model
        data = data[(data['DateTime'] <= '2021-10-13 15:00:00') | (data['DateTime'] >= '2021-10-13 18:00:00')]  # remove high error in model
        data = data[(data['DateTime'] <= '2021-10-26 22:00:00') | (data['DateTime'] >= '2021-11-05 23:00:00')] # remove high error in model
        data = data[(data['DateTime'] <= '2021-11-15 17:00:00') | (data['DateTime'] >= '2021-11-28 22:00:00')] # remove high error in model

        # Columns that required 
        # select specific column for F-21201 
        # data = data.loc[:, ['DateTime','Actual_Efficiency', 'AirCellA', 'AirCellB', 'CrudeOut_Temp', 'FuelGas_CellA', 'FuelGas_CellB',
        #                     'Stack_O2']]
        # select specific column for F-21202
        data = data.loc[:, ['DateTime', 'Actual_Efficiency', 'CrudePass1Flow', 'CrudePass2Flow', 'CrudePass3Flow', 'CrudePass4Flow', 'CrudeOutTemp', 'FuelPressure']]


        return data

    new_data = clean_data(df)
    new_data.to_csv("../input/clean_data_f21202.csv")
    print('Clean data is ready ...')


  




