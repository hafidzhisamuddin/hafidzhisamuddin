# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 13:00:00 2021
@author: Data Scientist SOFEIA
"""
# 2. Controllable parameter adjustment into DASHBOARD

from pydantic import BaseModel

class ControllableParameters(BaseModel):
    Furnace_Id: str
    AirCellA:float
    AirCellB:float
    CrudeOut_Temp:float
    FuelGas_CellA:float
    FuelGas_CellB:float
    Stack_O2:float
    CrudePass1Flow:float
    CrudePass2Flow:float
    CrudePass3Flow:float
    CrudePass4Flow:float
    CrudeOutTemp:float
    FuelPressure:float
    Excess_02:float
    AtmResidueOutTemp:float
    FuelGasCellAFlow:float
    FuelGasCellBFlow:float
    AirCellAFlow:float
    AirCellBFlow:float
    Excess_O2:float
    FuelPressure_2:float
    ExcessO2:float

