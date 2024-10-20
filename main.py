from fastapi import FastAPI, HTTPException
from typing import Optional
import pandas as pd
from DatabaseContext import ExtractDBData, CreateDBData, DataModel
import json
from pydantic import BaseModel

app = FastAPI(redoc_url=None)

@app.get("/fetch_prediction_province_policestation/")
def read_provinces01_data():

    fetch_prediction_province_policestation_data = ExtractDBData.fetch_prediction_province_policestation()

    if fetch_prediction_province_policestation_data is None:
        return {"error": "Province data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_prediction_province_policestation_data.to_dict(orient='records')

@app.get("/fetch_all_provinces/")
def read_provinces_data():

    fetch_province_data = ExtractDBData.fetch_all_provinces()

    if fetch_province_data is None:
        return {"error": "Province data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_province_data.to_dict(orient='records')

@app.get("/fetch_policestation_per_provinces/")
def read_policestation_data(provincecode : str ):

    #= Query(..., description="Province code")
    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_poilcestation_data = ExtractDBData.fetch_policestation_per_provinces(provincecode)

    if fetch_poilcestation_data is None:
        return {"error": "Police station data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_poilcestation_data.to_dict(orient='records')

@app.get("/fetch_stats_province_year_quarterly/")
def read_stats_province_quarterly(provincecode : str ):
   
    print(provincecode)
    
    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_crime_data = ExtractDBData.read_stats_province_quarterly(provincecode)

    if fetch_crime_data is None:
        return {"error": "Crime data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_crime_data.to_dict(orient='records')

## New end points
@app.get("/fetch_predition_province_policestation_quarterly_algorithm/")
def read_predition_province_policestation_year_quarterly_algorithm(provincecode: str, policestationcode: str, quarter: str,  algorithm: str):
   
    province_policestation_year_quarterly_algorithm = ExtractDBData.fetch_province_policestation_year_quarterly_algorithm(provincecode,policestationcode,quarter,algorithm)

    if province_policestation_year_quarterly_algorithm is None:
        return {"error": "Prediction not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return province_policestation_year_quarterly_algorithm.to_dict(orient='records')

@app.get("/fetch_training_predition_per_police_station/")
def read_training_predition_province_policestation_year_quarterly_algorithm(provincecode: str, policestationcode: str, quarter: str,  algorithm: str):
   
    province_policestation_year_quarterly_algorithm = ExtractDBData.fetch_training_province_policestation_year_quarterly_algorithm(provincecode,policestationcode,quarter,algorithm)

    if province_policestation_year_quarterly_algorithm is None:
        return {"error": "Prediction not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return province_policestation_year_quarterly_algorithm.to_dict(orient='records')


@app.get("/fetch_all_trained_predition/")
def read_all_trained_predition():
   
    all_predition_after_model_training = ExtractDBData.fetch_all_trained_predition()

    if all_predition_after_model_training is None:
        return {"error": "Prediction not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return all_predition_after_model_training.to_dict(orient='records')


@app.get("/fetch_all_predition_after_model_training/")
def read_all_predition_after_model_training():
   
    all_predition_after_model_training = ExtractDBData.fetch_all_predition_after_model_training()

    if all_predition_after_model_training is None:
        return {"error": "Prediction not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return all_predition_after_model_training.to_dict(orient='records')

@app.get("/fetch_stats_province_policestation_quarterly/")
def read_stats_province_policestation_quarterly(provincecode: str, policestationcode: str, quarter: Optional[int] = None):

    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_crime_data = ExtractDBData.read_stats_province_policestation_quarterly(provincecode, policestationcode, quarter)

    if fetch_crime_data is None:
        return {"error": "Crime data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_crime_data.to_dict(orient='records')

#fetch_training_metrics(scenario: str)
@app.get("/fetch_training_metrics/")
def read_training_metrics():

    fetch_training_metrics_data = ExtractDBData.fetch_training_metrics()

    if fetch_training_metrics_data is None:
        return {"error": "Scenario data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_training_metrics_data.to_dict(orient='records')

# Define the FastAPI endpoint to add metrics
@app.post("/add_metricss/")
async def create_metricss(data: DataModel.MerticData):
    # Call the insert_prediction_to_db function to handle the database insertion
    return CreateDBData.insert_metrics_to_db(data)


# Define the FastAPI endpoint to add predictions
@app.post("/add_all_predictions/")
async def create_predictions(data: DataModel.PredictionData):
    # Call the insert_prediction_to_db function to handle the database insertion
    return CreateDBData.insert_all_prediction_to_db(data)

# Define the FastAPI endpoint to add predictions
@app.post("/add_trianed_predictions/")
async def create_trianed_predictions(data: DataModel.PredictionData):
    # Call the insert_prediction_to_db function to handle the database insertion
    return CreateDBData.insert_trianed_prediction_to_db(data)


