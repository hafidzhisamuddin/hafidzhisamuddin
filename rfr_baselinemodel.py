from fileinput import filename
from pyexpat import model
from re import I
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from math import sqrt # Evaluation of model
from sklearn.metrics import mean_squared_error # Evaluation of model
from sklearn.metrics import mean_absolute_error
import pickle
import numpy as np
import matplotlib.pyplot as plt
import shap
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('baseline.log')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

modelpath =  '../models/'
datepath = '../doc/'
plotpath = '../doc/feature_importance/'
df = pd.read_csv("../input/clean_data_F-21201.csv", index_col=[0])

class ModelBaselining:
    
    def __init__(self, furnace_id,data:pd.DataFrame):
        self.furnace_id = furnace_id
        self.data = data
        
    def model(self):

        data = self.data
        # set index DateTime 
        min_date = min(data['DateTime'])
        max_date = max(data['DateTime'])
        data = data.set_index(data.DateTime)
        data = data.drop(['DateTime'], axis=1)

        # remove Date and target variable from dataset
        Y = data.Actual_Efficiency
        X = data.drop(['Actual_Efficiency'],axis =1)

        minmax = {"startTime": min_date, "endTime": max_date}
        minmaxpath = datepath + 'minmax_date_'+ self.furnace_id + '.pkl'
        with open(minmaxpath, 'wb') as files:
                        pickle.dump(minmax, files)

        # random forest model baselining
        final_rfr = RandomForestRegressor(n_estimators = 100, random_state = 0)
        logger.info(f'Model is running ...')

        final_rfr.fit(X, Y)

        # create an iterator object with write permission - model.pkl

        # for furnace_id in mdir:

        mpath =  modelpath + 'model_' + self.furnace_id + '.pkl'
        with open(mpath , 'wb') as files:
            pickle.dump(final_rfr, files)

        # load saved model
        with open(mpath, 'rb') as files:
            finalmodel_pkl = pickle.load(files)

        logger.info(f'Model is saved ...')
        ypred = finalmodel_pkl.predict(X)
                            

        MAE = mean_absolute_error(Y, ypred)
        print(f'Mean Absolute Error is: ', MAE,'for {}'.format(self.furnace_id))

        RMSE = sqrt(mean_squared_error(Y, ypred))
        print(f'Root Mean Squared Error is: ', RMSE,'for {}'.format(self.furnace_id))

        ytest = np.array(Y)
        predictions = np.array(ypred)
        MAPE = np.mean(np.abs((ytest - predictions) / predictions)) * 100
        print(f'Mean Absolute Percentage Error  is: ', MAPE,'for {}' .format(self.furnace_id))
        print(f'SHAP Feature Plot is running ...')
        shap_values = shap.TreeExplainer(finalmodel_pkl).shap_values(X)

        fig = plt.figure(facecolor='white')
        fig = shap.summary_plot(shap_values, X, plot_type="bar",show =False, max_display= 10, color = "#00A19C")
        fig = plt.title('SHAP Impactful Parameters to {} Furnace Efficiency'.format(self.furnace_id))
        

        ppath =  plotpath + 'shap_rfrplot_' + self.furnace_id + '.png'
        fig =  plt.savefig(ppath, bbox_inches='tight')


        return MAE,RMSE,MAPE,fig

if __name__ == "__main__":
    rfr_model = ModelBaselining('F-21201',df)
    models = rfr_model.model()

