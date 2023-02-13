from datetime import datetime
import numpy as np
from numpy import dtype
import pandas as pd
import logging
from data_ingestion import DataLoading
from data_preprocessing import DataProcessing
from rfr_baselinemodel import ModelBaselining

import pandas as pd
import json
from evidently.dashboard import Dashboard
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.dashboard.tabs import DataDriftTab, NumTargetDriftTab
from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection, NumTargetDriftProfileSection
from apscheduler.schedulers.background import BackgroundScheduler

def sensor():
    # set apscheduler
    sched.print_jobs()

    # set logging configuration
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler('main.log')
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Looping for all furnaces 
    furnaces = ['F-21201', 'F-21202', 'F-21251', 'F-21252']

    for id in furnaces: 

        def main():

            trained_data = pd.read_csv("../input/clean_data_" + id + ".csv", index_col=[0])
            trained_data = trained_data.drop(['DateTime'], axis=1)

            # Data Extraction from database
            data = DataLoading(id)
            input_data = data.get_cleaned_data()
            logger.debug(f"Data ingestion complete.")

            # Data Preprocessing and Data Cleaning
            new_data = DataProcessing(id, input_data)
            cleaned_data = new_data.get_cleanup_data()
            test_data = cleaned_data.drop(['DateTime'], axis=1)

            ref_data_sample = trained_data
            prod_data_sample = test_data

            # Column mapping
            target = 'Actual_Efficiency'
            column_mapping = ColumnMapping()
            column_mapping.target = target
            column_mapping.numerical_features = cleaned_data.iloc[:, 1:].columns

            # Run data drift and target drift analysis using Evidently AI
            drift_profile = Profile(sections=[DataDriftProfileSection(),NumTargetDriftProfileSection()])
            drift_profile.calculate(ref_data_sample,prod_data_sample,column_mapping=column_mapping)
            drift_profile.json()

            # Create dashboard for visualization
            drift_dashboard = Dashboard(tabs=[DataDriftTab(),NumTargetDriftTab()])
            drift_dashboard.calculate(ref_data_sample,prod_data_sample,column_mapping=column_mapping)
            drift_dashboard.save("../reports/sofeia_drift_report_" + id + ".html")

            # Extracting calculation results from Profile
            temp_json = json.loads(drift_profile.json())
            drift_run_date = temp_json['timestamp']
            data_drifted = temp_json['data_drift']['data']['metrics']['dataset_drift']

            # Retrained the model in the occurence of data drift
            if data_drifted == True:
                print('Data drift occur for ' + id + ' at ' + drift_run_date)
                df = pd.read_csv("../input/clean_data_{}.csv".format(id), index_col=[0])
                rfr_model = ModelBaselining(id, df)
                models = rfr_model.model()
                return models
            else:
                print('Model for furnace '+ id +' is good')

        if __name__ == "__main__":
            main()


sched = BackgroundScheduler()
sched.add_job(sensor,'interval', days=1)
sched.start()


