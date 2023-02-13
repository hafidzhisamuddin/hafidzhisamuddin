from pyexpat import features
import matplotlib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn import set_config 
from math import sqrt # Evaluation of model
from sklearn.metrics import mean_squared_error # Evaluation of model
from sklearn.metrics import mean_absolute_error
import matplotlib.pyplot as plt
import pickle
import shap
import os

inputpath = '../input/'
modelpath =  '../models/'
plotpath = '../doc/feature_importance/'
dir = os.listdir(inputpath)
dir = [f for f in dir if (f.startswith("clean_data") and f.lower().endswith(".csv"))]
mdir = ['_f21201']
print('\nList of target files: \n\t{}\n'.format(dir))

df = pd.read_csv("../input/clean_data.csv", index_col=[0])
class FeatureImportance:
    def __init__(self, data) -> pd.DataFrame:
        self.data = data
        
    def plot(data):

        data = data.set_index(data.DateTime)
        data = data.drop(['DateTime'], axis=1)

        # remove Date and target variable from dataset
        X = data.drop(['Actual_Efficiency'],axis =1)

        # #get list of importance variables
        # importance_rfr = baselinemodel_pkl.feature_importances_
        # #sorted_idx = importance_rfr.argsort()
        # indices = importance_rfr.argsort()
        # num_features = 6 # customized number 
        # features = df.columns

        # # only plot the customized number of features
        # plt.barh(range(num_features), importance_rfr[indices[-num_features:]], color='b', align='center')
        # plt.yticks(range(num_features), [features[i] for i in indices[-num_features:]])
        # plt.xlabel('Random Forest Feature Importance')
        # plt.savefig('importance_rfr.png')

        print('SHAP Feature Plot is running ...')
        shap_values = shap.TreeExplainer(finalmodel_pkl).shap_values(X)
        # f = shap.force_plot(explainer.expected_value,shap_values[-1:],features=X_test.iloc[-1:],
        #     matplotlib=True, show = False)
        # f = shap.summary_plot(shap_values, X_test, plot_type="bar",show =False, max_display= 10)
        # fig = plt.gcf()

        fig = shap.summary_plot(shap_values, X, plot_type="bar",show =False, max_display= 10)
        fig = plt.title( 'SHAP Impactful Parameters to F-21202 Furnace Efficiency')
        # fig = plt.figure(facecolor='white')

        ppath =  plotpath + 'shap_rfrplot' + furnace_id + '.png'
        fig =  plt.savefig(ppath, bbox_inches='tight')
        # fig =  plt.savefig('../doc/feature_importance/f21202_shap_rfrplot.png', bbox_inches='tight')
        # fig = plt.show()

        # plt.title( 'SHAP Impactful Parameters to Furnace Efficiency')
        # plt.figure(facecolor='white')
        # plt.show()
        # plt.savefig('testing_shap_rfr_fullparameters.jpeg')
        print('SHAP Feature Plot is for {} saved ...'.format(furnace_id))
        return fig

    Result_rfr = plot(df)

