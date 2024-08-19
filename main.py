from fastapi import FastAPI
from typing import Optional
import pandas as pd
from DatabaseContext import ExtractDBData

app = FastAPI()


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

@app.get("/fetch_stats_province_policestation/",deprecated=True)
def read_stats_province_policestation(provincecode: str, policestationcode: str):

    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_crime_data = ExtractDBData.read_stats_province_policestation(provincecode, policestationcode)

    if fetch_crime_data is None:
        return {"error": "Crime data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    print('Final Dataset : ')
    return fetch_crime_data.to_dict(orient='records')

@app.get("/fetch_stats_province_policestation_quarterly/",deprecated=True)
def read_stats_province_policestation_quarterly(provincecode: str, policestationcode: str, quarter: Optional[int] = None):

    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_crime_data = ExtractDBData.read_stats_province_policestation_quarterly(provincecode, policestationcode, quarter)

    if fetch_crime_data is None:
        return {"error": "Crime data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_crime_data.to_dict(orient='records')

@app.get("/fetch_stats_province_year_quarterly/")
def read_stats_province_quarterly(provincecode : str, quarter: Optional[int] = None ):
   
    print(provincecode)
    print(quarter)
    
    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_crime_data = ExtractDBData.read_stats_province_quarterly(provincecode, quarter)

    if fetch_crime_data is None:
        return {"error": "Crime data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_crime_data.to_dict(orient='records')


@app.post("/fetch_predition_province_policestation_year_quarterly/")
def read_predition_province_policestation_year_quarterly():
   
       # Call function from ExtractDBData.py to fetch data based on parameters
    
    fetch_prediction_province_policestation = ExtractDBData.fetch_prediction_province_policestation_data()

    if fetch_prediction_province_policestation is None:
        return {"error": "Province data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_prediction_province_policestation.to_dict(orient='records')


@app.post("/fetch_best_predictions_province_policestation_year_quarterly/", deprecated=True)
def read_best_predictions_province_policestation_year_quarterly(provincecode : str ):
   
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return 0

@app.post("/add_prediction_knn_province_policestation_year_quarterly/", deprecated=True)
def create_prediction_knn_province_policestation_year_quarterly(provincecode : str ):
   
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return 0

@app.post("/add_prediction_mlpregressor_province_policestation_year_quarterly/", deprecated=True)
def create_prediction_mlpregressor_province_policestation_year_quarterly(provincecode : str ):
   
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return 0

@app.post("/add_prediction_svr_province_policestation_year_quarterly/", deprecated=True)
def create_prediction_svr_province_policestation_year_quarterly(provincecode : str ):
   
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return 0

@app.post("/add_prediction_rfm_province_policestation_year_quarterly/",deprecated=True)
def create_prediction_rfm_province_policestation_year_quarterly(provincecode : str ):
   
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return 0

@app.post("/add_prediction_xgboost_province_policestation_year_quarterly/",deprecated=True)
def create_prediction_xgboost_province_policestation_year_quarterly(provincecode : str ):
   
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return 0

@app.post("/update_best_predictions/" ,deprecated=True)
def update_best_prediction(provincecode : str ):
   # will set a flag for the best 2 priction AcceptPredict: 1 = true , 0 = false
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return 0