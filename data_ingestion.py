import logging
import sys
import pandas as pd
from typing import List, Dict
from get_data_windows import get_data
pd.options.display.float_format = '{:,.2f}'.format

# set logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('dataloading.log')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

if '../input/' not in sys.path:
    sys.path.append('../input/')


class DataLoading:
    def __init__(self, furnace_id):
        self.furnace_id = furnace_id

    def get_input_data(self):
        """Reads the raw input data
        """
        try:
            raw_data = get_data
            return raw_data
        except FileNotFoundError:
            logger.error(f"Check for the File Path")

    def get_data_dict(self):
        """Reads the data dictionary    
        """
        try:
            processdata_dict = pd.read_csv("../input/Data_Dictionary_all.csv", index_col=[0],
                                           encoding='unicode_escape')
            logger.debug(f"The num of rows in data dict are {processdata_dict.shape[0]}")
            return processdata_dict
        except FileNotFoundError:
            logger.error(f"Check for the File Path")


    def mapcolumnnames(self, processdata_dict: pd.DataFrame,
                       input_data: pd.DataFrame) -> List:
        """Reads the raw input data and cleans the column names based on the data dictionary

        Args:
            processdata_dict ([DataFrame]): Data Dictionary
            input_data ([DataFrame]): Raw Input Data

        Returns:
            [List]: Cleaned Column Names
        """
        new_names = []
        colnames = dict(zip(processdata_dict['PI_Tag'],
                            processdata_dict['Name_In_Model']))
        for name in list(input_data):
            try:
                new_names.append(colnames[name])
            except KeyError:
                logger.error(name + " NotFound")
        return new_names


    def get_cleaned_data(self):
        """
        Returns:
            [DataFrame]: Returns dataframe for only selected furnace id with the cleaned columnn names 
        """
        raw_data = self.get_input_data()
        processdata_dict = self.get_data_dict()
        furnacedata_dict = processdata_dict.loc[processdata_dict['Equipment'] == self.furnace_id]
        furnacedata_list = furnacedata_dict['PI_Tag'].values.tolist()
        furnacedata_list.append('TimeStamp')
        new_data = raw_data[raw_data.columns[raw_data.columns.isin(furnacedata_list)]]
        new_data.columns = self.mapcolumnnames(processdata_dict, new_data)
        new_data.to_csv("../input/input_data_" + self.furnace_id + ".csv")
        return new_data


if __name__ == "__main__":
    data = DataLoading('F-21202')
    input_data = data.get_cleaned_data()
    logger.debug(f"Data ingestion complete.")
    
