import pandas as pd
import json

from evidently.dashboard import Dashboard
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.dashboard.tabs import DataDriftTab, NumTargetDriftTab

from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection, NumTargetDriftProfileSection


sofeia_df = pd.read_csv("../input/clean_data_F-21202.csv",index_col=[0], encoding='unicode_escape')
sofeia_df = sofeia_df.drop(['DateTime'], axis=1)
print(sofeia_df.head(2))
ref_data_sample = sofeia_df[600:]
prod_data_sample = sofeia_df[:600]

# Column mapping

target = 'Actual_Efficiency'
column_mapping = ColumnMapping()
column_mapping.target = target
column_mapping.numerical_features = sofeia_df.iloc[:, 1:].columns
print(column_mapping.target)
print(column_mapping.numerical_features)

# Run data drift and target drift analysis using Evidently AI
drift_profile = Profile(sections=[DataDriftProfileSection(),NumTargetDriftProfileSection()])
drift_profile.calculate(ref_data_sample, prod_data_sample, column_mapping=column_mapping)
drift_profile.json()

# Create dashboard for visualization
drift_dashboard = Dashboard(tabs=[DataDriftTab(), NumTargetDriftTab()])
drift_dashboard.calculate(ref_data_sample, prod_data_sample, column_mapping=column_mapping)
drift_dashboard.save("../reports/sofeia_drift_report_.html")


# Extracting calculation results from Profile
temp_json = json.loads(drift_profile.json())
drift_run_date = temp_json['timestamp']
data_drifted = temp_json['data_drift']['data']['metrics']['dataset_drift']
data_drift_magnitude = temp_json['data_drift']['data']['metrics']['share_drifted_features']
data_drift_metrics = [(feat+'_pvalue', temp_json['data_drift']['data']['metrics'][feat]['p_value']) for feat in sofeia_df.iloc[:, 1:].columns]
data_drift_metrics = {k.replace('(', '').replace(')', ''): round(v, 3) for k, v in data_drift_metrics}

if data_drifted == True:
    print('Data drift occur at '+ drift_run_date)
    print('Please retrain the model!')
else:
    print('Model is good!')
