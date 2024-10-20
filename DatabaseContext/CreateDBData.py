import pyodbc
import os
import sys
import pandas as pd  # Import pandas for DataFrame handling
import config  # Import config module for database connection details
from DatabaseContext import DataModel
from fastapi import HTTPException

def connect_to_database():
    try:
        # Get the current script path
        current_path = os.path.dirname(os.path.abspath(__file__))
        print("Current path:", current_path)
    except NameError:
        # Fallback if __file__ is not defined (e.g., in Jupyter)
        current_path = os.getcwd()
        print("Current path:", current_path)

    # Add the parent directory to the system path
    sys.path.append(os.path.dirname(current_path))

    # Database connection parameters from config
    server = config.DefaultConnection['server']
    database = config.DefaultConnection['database']
    username = config.DefaultConnection['username']
    password = config.DefaultConnection['password']

    # Establish database connection
    cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    return cnxn

   # Function to add prediction to the database
def insert_all_prediction_to_db(data: DataModel.PredictionData):
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        # Execute the stored procedure with the data from the request
        cursor.execute("""
            EXEC sp_Add_Predictions_ZAWC 
            @Prediction=?, 
            @TrueValue=?, 
            @Algorithm=?, 
            @Scenario=?, 
            @CrimeCategoryCode=?, 
            @ProvinceCode=?, 
            @PoliceStationCode=?, 
            @Quarter=?, 
            @PredictionYear=?
        """, (data.Prediction, data.TrueValue, data.Algorithm, data.Scenario, data.CrimeCategoryCode,
              data.ProvinceCode, data.PoliceStationCode, data.Quarter, data.PredictionYear))

        # Commit the transaction
        cnxn.commit()

        return {"message": "Prediction added successfully!"}

    except pyodbc.Error as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    finally:
        # Close the database connection
        cursor.close()
        cnxn.close()


   # Function to add prediction to the database
def insert_trianed_prediction_to_db(data: DataModel.PredictionData):
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        # Execute the stored procedure with the data from the request
        cursor.execute("""
            EXEC sp_Add_Training_Predictions_ZAWC
            @Prediction=?, 
            @TrueValue=?, 
            @Algorithm=?, 
            @Scenario=?, 
            @CrimeCategoryCode=?, 
            @ProvinceCode=?, 
            @PoliceStationCode=?, 
            @Quarter=?, 
            @PredictionYear=?
        """, (data.Prediction, data.TrueValue, data.Algorithm, data.Scenario, data.CrimeCategoryCode,
              data.ProvinceCode, data.PoliceStationCode, data.Quarter, data.PredictionYear))

        # Commit the transaction
        cnxn.commit()

        return {"message": "Train-Prediction added successfully!"}

    except pyodbc.Error as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    finally:
        # Close the database connection
        cursor.close()
        cnxn.close()
def insert_metrics_to_db(data: DataModel.MerticData):
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        # Execute the stored procedure with the data from the request
        cursor.execute("""
            EXEC sp_Add_Metrics_ZAWC
            @Algorithm=?,
            @Scenario=?,
            @PredictedYear=?,
            @MAE=?,
            @MSE=?,
            @MAPE=?,
            @RSquare=?,
            @ARS=?
        """, (data.Algorithm, data.Scenario, data.PredictedYear, data.MAE,
              data.MSE, data.MAPE, data.RSquare, data.ARS))

        # Commit the transaction
        cnxn.commit()

        return {"message": "Mertics added successfully!"}

    except pyodbc.Error as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    finally:
        # Close the database connection
        cursor.close()
        cnxn.close()

