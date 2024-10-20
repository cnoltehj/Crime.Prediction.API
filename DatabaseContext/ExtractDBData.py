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
    cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    return cnxn


def fetch_all_provinces():
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:   
        cursor.execute("EXEC sp_GetAllProvinces")

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
        cursor.execute("EXEC sp_GetPoliceStationsPerProvince ?", (provincecode.strip()))

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

def fetch_province_policestation_year_quarterly_algorithm(provincecode: str, policestationcode: str, quarter: str,  algorithm: str):
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()
    
    try:   
        # cursor.execute("EXEC sp_Get_Prediction_Province_PoliceStation_Quarter_Algorithm ?, ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip))

        cursor.execute("EXEC sp_Get_Prediction_Province_PoliceStation_Quarter_Algorithm ? , ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip()))
        # Fetch the results after executing the stored procedure
        rows = cursor.fetchall()
        print(rows)

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


def fetch_training_province_policestation_year_quarterly_algorithm(provincecode: str, policestationcode: str, quarter: str,  algorithm: str):
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()
    
    try:   
        # cursor.execute("EXEC sp_Get_Prediction_Province_PoliceStation_Quarter_Algorithm ?, ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip))

        cursor.execute("EXEC sp_Get_Training_Prediction_Province_PoliceStation_Quarter_Algorithm ? , ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter), algorithm.strip()))
        # Fetch the results after executing the stored procedure
        rows = cursor.fetchall()
        print(rows)

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

def read_stats_province_policestation(provincecode, policestationcode):
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        # Execute the stored procedure with the parameters
        cursor.execute("EXEC sp_Get_Stats_Province_PoliceStation ?, ?", (provincecode.strip(), policestationcode.strip())) 

        # Fetch the results after executing the stored procedure
        rows = cursor.fetchall()

        # Fetch the column descriptions (headings)
        headings = [column[0] for column in cursor.description]

        # Reshape the rows data to match the expected shape
        rows = [list(row) for row in rows]

        # Create a DataFrame from the fetched rows and headings
        df_stats = pd.DataFrame(rows, columns=headings)

        # Replace null values with 0's
        df_stats.fillna(0, inplace=True)

        return df_stats  # Return the DataFrame

    except pyodbc.Error as e:
        # Print an error message if there's an exception
        print("Error executing SQL query:", e)
        return None  # Return None if there's an error

    finally:
        # Close the cursor and connection
        cursor.close()
        cnxn.close()

def read_stats_province_policestation_quarterly(provincecode, policestationcode, quarter=None):
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        # Execute the stored procedure with the parameters
        # if year:  # Check if the year parameter is not empty
        #     cursor.execute("EXEC sp_GetQuartilyCrimeStatsPerQuarter ?, ?, ?, ?", (provincecode.strip(), policestation.strip(), int(year), int(quarter)))
        # else:
        #     cursor.execute("EXEC sp_GetQuartilyCrimeStatsPerQuarter ?, ?, ?, ?", (provincecode.strip(), policestation.strip(), None, int(quarter)))


        cursor.execute("EXEC sp_Get_Stats_Province_PoliceStation_Quarterly ?, ?, ?", (provincecode.strip(), policestationcode.strip(), int(quarter)))

        # Fetch the results after executing the stored procedure
        rows = cursor.fetchall()

        # Fetch the column descriptions (headings)
        headings = [column[0] for column in cursor.description]

        # Reshape the rows data to match the expected shape
        rows = [list(row) for row in rows]

        # Create a DataFrame from the fetched rows and headings
        df_stats = pd.DataFrame(rows, columns=headings)

        # Replace null values with 0's
        df_stats.fillna(0, inplace=True)

        return df_stats  # Return the DataFrame

    except pyodbc.Error as e:
        # Print an error message if there's an exception
        print("Error executing SQL query:", e)
        return None  # Return None if there's an error

    finally:
        # Close the cursor and connection
        cursor.close()
        cnxn.close()

def read_stats_province_quarterly(provincecode):
    # Connect to the database
    cnxn = connect_to_database()
    cursor = cnxn.cursor()

    try:
        print("Province: " ,provincecode)

        cursor.execute("EXEC sp_Get_Stats_Province_Quarterly ?", (provincecode.strip()))

        # Fetch the results after executing the stored procedure
        rows = cursor.fetchall()

        # Fetch the column descriptions (headings)
        headings = [column[0] for column in cursor.description]

        # Reshape the rows data to match the expected shape
        rows = [list(row) for row in rows]

        # Create a DataFrame from the fetched rows and headings
        df_stats = pd.DataFrame(rows, columns=headings)

        # Replace null values with 0's
        df_stats.fillna(0, inplace=True)

        print(df_stats)

        return df_stats  # Return the DataFrame

    except pyodbc.Error as e:
        # Print an error message if there's an exception
        print("Error executing SQL query:", e)
        return None  # Return None if there's an error

    finally:
        # Close the cursor and connection
        cursor.close()
        cnxn.close()