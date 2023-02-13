# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 13:00:00 2021
@author: Data Scientist SOFEIA
"""

# 1. Library imports
import azure.functions as func
from http_asgi import AsgiMiddleware
from unicodedata import decimal
import uvicorn
from fastapi import FastAPI
import numpy as np
import pickle
import pandas as pd
from Data import ControllableParameters
from PIL import Image
import matplotlib.pyplot as plt

from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

# 2. Create the app object

app = FastAPI()
origins = ["http://localhost:3000",
           "https://ptsg-4feia-ap01.azurewebsites.net"]

app.add_middleware(
            CORSMiddleware,
                allow_origins=origins,
    		allow_credentials=True,
    		allow_methods=["*"],
    		allow_headers=["*"]
                )
pickle_in = open("../models/model_F-21201.pkl","rb")
rfrmodel = pickle.load(pickle_in)

f21202_pickle_in = open("../models/model_F-21202.pkl", "rb")
f21202_rfrmodel = pickle.load(f21202_pickle_in)

f21251_pickle_in = open("../models/model_F-21251.pkl", "rb")
f21251_rfrmodel = pickle.load(f21251_pickle_in)

f21252_pickle_in = open("../models/model_F-21252.pkl", "rb")
f21252_rfrmodel = pickle.load(f21252_pickle_in)

@app.post('/predict')
def predict(data:ControllableParameters):
    data = data.dict()
    Furnace_Id = data['Furnace_Id']
    # F-21201 Parameter 
    AirCellA = data['AirCellA']
    AirCellB = data['AirCellB']
    CrudeOut_Temp = data['CrudeOut_Temp']
    FuelGas_CellA = data['FuelGas_CellA']
    FuelGas_CellB = data['FuelGas_CellB']
    Stack_O2 = data['Stack_O2']
    # F-21202 Parameter
    CrudePass1Flow = data['CrudePass1Flow']
    CrudePass2Flow = data['CrudePass2Flow']
    CrudePass3Flow = data['CrudePass3Flow']
    CrudePass4Flow = data['CrudePass4Flow']
    CrudeOutTemp = data['CrudeOutTemp']
    FuelPressure = data['FuelPressure']
    Excess_02 = data['Excess_02']
    # F-21251 Parameter
    AtmResidueOutTemp = data['AtmResidueOutTemp']
    FuelGasCellAFlow = data['FuelGasCellAFlow']
    FuelGasCellBFlow = data['FuelGasCellBFlow']
    AirCellAFlow = data['AirCellAFlow']
    AirCellBFlow = data['AirCellBFlow']
    Excess_O2 = data['Excess_O2']
    # F-21252 Parameter
    FuelPressure_2 = data['FuelPressure_2']
    ExcessO2 = data['ExcessO2']


    if Furnace_Id == 'F-21201':
        prediction = rfrmodel.predict([[AirCellA, AirCellB, CrudeOut_Temp, FuelGas_CellA, FuelGas_CellB, Stack_O2]])
        #arr = np.array(prediction)
        # arr = np.round(arr, decimals=2)
        # apply method
        final_prediction = np.round(prediction, 2)
        return {'Predicted Efficiency': f'{final_prediction[0]}'}, {'Furnace': f'{Furnace_Id}'}

    elif Furnace_Id == 'F-21202':
        prediction = f21202_rfrmodel.predict(
            [[CrudePass1Flow, CrudePass2Flow, CrudePass3Flow, CrudePass4Flow, CrudeOutTemp, FuelPressure,Excess_02]])
        arr = np.array(prediction)
        arr = np.round(arr, decimals=2)
        # apply method
        final_prediction = np.round(prediction, 2)
        return {'Predicted Efficiency': f'{final_prediction[0]}'}, {'Furnace': f'{Furnace_Id}'}

    elif Furnace_Id == 'F-21251':
        prediction = f21251_rfrmodel.predict([[AtmResidueOutTemp, FuelGasCellAFlow, FuelGasCellBFlow, AirCellAFlow, AirCellBFlow,Excess_O2]])
        arr =np.array(prediction)
        arr = np.round(arr, decimals=2)
        # apply method
        final_prediction = np.round(prediction, 2)
        return {'Predicted Efficiency': f'{final_prediction[0]}'}, {'Furnace': f'{Furnace_Id}'}

    elif Furnace_Id == 'F-21252':
        prediction = f21252_rfrmodel.predict([[FuelPressure_2,ExcessO2]])
        arr = np.array(prediction)
        arr = np.round(arr, decimals=2)
        # apply method
        final_prediction = np.round(prediction, 2)
        return {'Predicted Efficiency': f'{final_prediction[0]}'}, {'Furnace': f'{Furnace_Id}'}
    return {'Please insert correct furnace id'}

@app.post('/plot')
async def index(data:ControllableParameters):

    data = data.dict()
    Furnace_Id = data['Furnace_Id']

    if Furnace_Id == 'F-21201':
        return FileResponse('../doc/feature_importance/shap_rfrplot_F-21201.png')
    elif Furnace_Id == 'F-21202':
        return FileResponse('../doc/feature_importance/shap_rfrplot_F-21202.png')
    elif Furnace_Id == 'F-21251':
        return FileResponse('../doc/feature_importance/shap_rfrplot_F-21251.png')
    elif Furnace_Id == 'F-21252':
        return FileResponse('../doc/feature_importance/shap_rfrplot_F-21252.png')
    return {'Please insert correct furnace id'}

@app.post('/datetime')
async def index(data:ControllableParameters):

    data = data.dict()
    Furnace_Id = data['Furnace_Id']

    if Furnace_Id == 'F-21201':
        pickle_in = open("../doc/minmax_date_F-21201.pkl","rb")
        f21201_my_list = pickle.load(pickle_in)
        return f21201_my_list
    elif Furnace_Id == 'F-21202':
        pickle_in = open("../doc/minmax_date_F-21202.pkl","rb")
        f21202_my_list = pickle.load(pickle_in)
        return f21202_my_list
    elif Furnace_Id == 'F-21251':
        pickle_in = open("../doc/minmax_date_F-21251.pkl","rb")
        f21251_my_list = pickle.load(pickle_in)
        return f21251_my_list
    elif Furnace_Id == 'F-21252':
        pickle_in = open("../doc/minmax_date_F-21252.pkl","rb")
        f21252_my_list = pickle.load(pickle_in)
        return f21252_my_list
    return {'Please insert correct furnace id'}

def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return AsgiMiddleware(app).handle(req, context)

# # 5. Run the API with uvicorn
# #    Will run on http://127.0.0.1:8000
# if __name__ == '__main__':
#     # uvicorn.run(app, host='172.25.52.253', port=8000)
#     uvicorn.run(app, host='127.0.0.1', port=8000)
    
# #uvicorn app:app --reload