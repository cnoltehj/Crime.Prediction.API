import pyodbc
import os
import sys
import pandas as pd  # Import pandas for DataFrame handling
import config  # Import config module for database connection details

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
    cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes')
    #cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};TrustServerCertificate=yes;Encrypt=yes')
    return cnxn

#region GENERIC DB-CALLS

def fetch_all_provinces():
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:   
        cursor.execute("EXEC sp_Generic_GetAllProvinces")

        # Fetch the results after executing the stored procedure
        rows = cursor.fetchall()

        # Fetch the column descriptions (headings)
        headings = [column[0] for column in cursor.description]

        # Reshape the rows data to match the expected shape
        rows = [list(row) for row in rows]

        # Create a DataFrame from the fetched rows and headings
        df = pd.DataFrame(rows, columns=headings)

        # Replace null values with 0's
        df.fillna(0, inplace=True)

        return df  # Return the DataFrame

    except pyodbc.Error as e:
        # Print an error message if there's an exception
        print("Error executing SQL query:", e)
        return None  # Return None if there's an error

    finally:
        # Close the cursor and connection
        cursor.close()
        cnxn.close()

def fetch_policestation_per_provinces(provincecode):
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        cursor.execute("EXEC sp_Generic_GetPoliceStationsPerProvince ?", (provincecode.strip()))

        # Fetch the results after executing the stored procedure
        rows = cursor.fetchall()

        # Fetch the column descriptions (headings)
        headings = [column[0] for column in cursor.description]

        # Reshape the rows data to match the expected shape
        rows = [list(row) for row in rows]

        # Create a DataFrame from the fetched rows and headings
        df = pd.DataFrame(rows, columns=headings)

        # Replace null values with 0's
        df.fillna(0, inplace=True)

        return df  # Return the DataFrame

    except pyodbc.Error as e:
        # Print an error message if there's an exception
        print("Error executing SQL query:", e)
        return None  # Return None if there's an error

    finally:
        # Close the cursor and connection
        cursor.close()
        cnxn.close()    

def read_all_stats_per_province_quarterly(provincecode, quarter=None):
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        print(f"ProvinceCode={provincecode}, Quarter={quarter}")

        if quarter is not None:
            cursor.execute(
                "EXEC sp_Generic_Get_All_Stats_Province_Quarterly ?, ?",
                (provincecode.strip(), int(quarter))
            )
        else:
            cursor.execute(
                "EXEC sp_Generic_Get_All_Stats_Province_Quarterly ?, ?",
                (provincecode.strip(), None)  # pass NULL if quarter not provided
            )

        rows = cursor.fetchall()
        headings = [column[0] for column in cursor.description]
        rows = [list(row) for row in rows]

        df_stats = pd.DataFrame(rows, columns=headings)
        df_stats.fillna('Nan', inplace=True)

        return df_stats

    except Exception as e:
        print("SQL execution error:", e)
        raise
    finally:
        cursor.close()
        cnxn.close()



#endregion

#region EXPERIMENT DB-CALLS


#endregion

#region PREDICTION DB-CALLS

#endregion

#region WEB-UI DB-CALLS

#endregion


# def fetch_province_policestation_year_quarterly_algorithm(provincecode: str, policestationcode: str, quarter: str,  algorithm: str):
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:   
#         # cursor.execute("EXEC sp_Get_Prediction_Province_PoliceStation_Quarter_Algorithm ?, ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip))

#         cursor.execute("EXEC sp_Get_Prediction_Province_PoliceStation_Quarter_Algorithm ? , ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip()))
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# def fetch_training_province_policestation_year_quarterly_algorithm(provincecode: str, policestationcode: str, quarter: str,  algorithm: str):
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:   
#         # cursor.execute("EXEC sp_Get_Prediction_Province_PoliceStation_Quarter_Algorithm ?, ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip))

#         cursor.execute("EXEC sp_Get_Training_Prediction_Province_PoliceStation_Quarter_Algorithm ? , ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip()))
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# ## New additions
# def fetch_training_metrics():
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:   
#         cursor.execute("EXEC sp_Get_All_Metrics")
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# def fetch_train_predictions():
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:  
#         cursor.execute("EXEC sp_Get_All_Train_Prediction")
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# def fetch_all_predition_after_model_training():
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:  
#         cursor.execute("EXEC sp_Get_All_Train_Prediction_After_Model_Training")
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# def fetch_all_trained_predition():
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:  
#         cursor.execute("EXEC sp_Get_All_Prediction")
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# def fetch_initial_province_policestation_year_quarterly(provincecode: str, policestationcode: str, quarter: str):
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:   
#         # cursor.execute("EXEC sp_Get_Prediction_Province_PoliceStation_Quarter_Algorithm ?, ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip))

#         cursor.execute("EXEC sp_Get_Initial_Prediction_Province_PoliceStation_Quarter? , ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter)))
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# def fetch_metrics_best_model_per_scenario():
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()
    
#     try:  
#         cursor.execute("EXEC sp_Get_Metrics_Best_Model_Per_Scenario")
#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()
#         print(rows)

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df.fillna(0, inplace=True)

#         return df  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()


# def read_stats_province_policestation(provincecode, policestationcode):
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()

#     try:
#         # Execute the stored procedure with the parameters
#         cursor.execute("EXEC sp_Get_Stats_Province_PoliceStation ?, ?", (provincecode.strip(), policestationcode.strip())) 

#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df_stats = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df_stats.fillna(0, inplace=True)

#         return df_stats  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

# def read_stats_province_policestation_quarterly(provincecode, policestationcode, quarter=None):
#     # Connect to the database
#     cnxn = connect_to_database()
#     cursor = cnxn.cursor()

#     try:
#         # Execute the stored procedure with the parameters
#         # if year:  # Check if the year parameter is not empty
#         #     cursor.execute("EXEC sp_GetQuartilyCrimeStatsPerQuarter ?, ?, ?, ?", (provincecode.strip(), policestation.strip(), int(year), int(quarter)))
#         # else:
#         #     cursor.execute("EXEC sp_GetQuartilyCrimeStatsPerQuarter ?, ?, ?, ?", (provincecode.strip(), policestation.strip(), None, int(quarter)))


#         cursor.execute("EXEC sp_Get_Stats_Province_PoliceStation_Quarterly ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter)))

#         # Fetch the results after executing the stored procedure
#         rows = cursor.fetchall()

#         # Fetch the column descriptions (headings)
#         headings = [column[0] for column in cursor.description]

#         # Reshape the rows data to match the expected shape
#         rows = [list(row) for row in rows]

#         # Create a DataFrame from the fetched rows and headings
#         df_stats = pd.DataFrame(rows, columns=headings)

#         # Replace null values with 0's
#         df_stats.fillna(0, inplace=True)

#         return df_stats  # Return the DataFrame

#     except pyodbc.Error as e:
#         # Print an error message if there's an exception
#         print("Error executing SQL query:", e)
#         return None  # Return None if there's an error

#     finally:
#         # Close the cursor and connection
#         cursor.close()
#         cnxn.close()

