from fastapi import FastAPI
from typing import Optional
import pandas as pd
from DatabaseContext import ExtractDBData

app = FastAPI()

@app.get("/items_crime_stats/")
def read_crime_data(provincecode: str, policestation: str, year: Optional[int] = None, quarter: Optional[int] = None):
    
    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_crime_data = ExtractDBData.fetch_crime_stats(provincecode, policestation, year, quarter)

    if fetch_crime_data is None:
        return {"error": "Crime data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_crime_data.to_dict(orient='records')

@app.get("/items_fetch_all_provinces/")
def read_provinces_data():

    # Call function from ExtractDBData.py to fetch data based on parameters

    fetch_province_data = ExtractDBData.fetch_all_provinces()

    if fetch_province_data is None:
        return {"error": "Province data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_province_data.to_dict(orient='records')

@app.get("/items_policestation_per_provinces/")
def read_policestation_data(provincecode : str ):

    #= Query(..., description="Province code")
    # Call function from ExtractDBData.py to fetch data based on parameters
    fetch_poilcestation_data = ExtractDBData.fetch_policestation_per_provinces(provincecode)

    if fetch_poilcestation_data is None:
        return {"error": "Police station data not found"}
    
    # Convert DataFrame to JSON serializable format (list of dictionaries)
    return fetch_poilcestation_data.to_dict(orient='records')