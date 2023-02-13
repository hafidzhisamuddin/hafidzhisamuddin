from dataclasses import dataclass
import pandas as pd
import numpy as np
import logging

x = pd.read_csv("../input/input_data_F-21202.csv", index_col=[0], encoding='unicode_escape')

# set logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('datapreprocessing.log')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

class DataProcessing:

    def __init__(self, furnace_id,data:pd.DataFrame):
        self.furnace_id = furnace_id
        self.data = data

    def clean_data(self):
        """ 
        Reads input data for selected furnace
        Return cleaned data based on allowable efficiency range
        
        """
        try:
            data = self.data
            data = data.dropna(axis=0, how='any', thresh=None, subset=None)
            data = data[(data['Actual_Efficiency'] >= 0) &
                        (data['Actual_Efficiency'] <= 100)]
            return data
        except Exception:
            logger.error('Check for valid data ')
        
    def remove_plant_trip(self,clean_data):
        """ 
        Remove parts of invalid data during TA & High error in the model
        
        """
        data = clean_data
        data = data[(data['DateTime'] < '2020-02-20 10:00:00') | (data['DateTime'] >= '2020-02-20 17:00:00')]
        data = data[(data['DateTime'] < '2020-04-03 09:00:00') | (data['DateTime'] >= '2020-04-06 14:00:00')]
        data = data[(data['DateTime'] < '2020-07-09 14:00:00') | (data['DateTime'] >= '2020-07-10 01:00:00')]
        data = data[(data['DateTime'] < '2020-08-08 04:00:00') | (data['DateTime'] >= '2020-08-09 04:00:00')]
        data = data[(data['DateTime'] < '2020-09-07 08:00:00') | (data['DateTime'] >= '2020-09-08 00:00:00')]
        data = data[(data['DateTime'] < '2020-11-08 02:00:00') | (data['DateTime'] >= '2020-11-09 21:00:00')]
        data = data[(data['DateTime'] < '2021-02-10 20:00:00') | (data['DateTime'] >= '2021-02-12 10:00:00')]
        data = data[(data['DateTime'] <= '2021-02-23 11:00:00')| (data['DateTime'] >= '2021-02-25 02:00:00')]
        data = data[(data['DateTime'] <= '2021-06-26 10:00:00') | (data['DateTime'] >= '2021-06-29 02:00:00')]
        data = data[(data['DateTime'] <= '2021-10-13 15:00:00') | (data['DateTime'] >= '2021-10-13 18:00:00')]
        data = data[(data['DateTime'] <= '2021-10-26 22:00:00') | (data['DateTime'] >= '2021-11-05 23:00:00')]
        data = data[(data['DateTime'] <= '2021-11-15 17:00:00') | (data['DateTime'] >= '2021-11-28 22:00:00')]
        logger.debug(f'Input data removing plant trip...')
        return data

    def get_cleanup_data(self):
        data = self.data
        clean_data = self.clean_data()
        cleaned_data = self.remove_plant_trip(clean_data)
        cleaned_data.to_csv("../input/clean_data_"+ self.furnace_id + ".csv")
        logger.debug(f'Clean data is ready for modelling.')

        return cleaned_data


if __name__ == "__main__":
    new_data = DataProcessing('F-21202', x)
    cleaned_data = new_data.get_cleanup_data()






