import os
import mysql.connector
from mysql.connector import Error
import config as conf
import logging

# If DB is not already exist create new.
def create_database(mysql_connection):
    try:
        # check DB already exists
        if mysql_connection.is_connected():
            
            with mysql_connection.cursor() as cursor:
            
                cursor.execute(f"SHOW DATABASES LIKE '{conf.DB_NAME}';")
                result = cursor.fetchone()
                
                if not result:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {conf.DB_NAME};")
                    print(f"Database '{conf.DB_NAME}' created successfully.")
                    logging.info(f"Database '{conf.DB_NAME}' created successfully.")
                else:
                    print(f"Database '{conf.DB_NAME}' already exists.")
                    logging.info(f"Database '{conf.DB_NAME}' already exists.")

                mysql_connection.commit()
            return True
        else:
            return False    
    except Error as e:
        print(f"Error while creating MySQL {conf.DB_NAME} Database: {e}")
        logging.error(f"Error while creating MySQL {conf.DB_NAME} Database: {e}")
        return False  

# Function to create MySQL tables.
def create_my_sql_table(connection, table_name, query):
    # Create all MySQL tables as per the data model.
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()
            print(f"Table - {table_name} is either exist or created successfully \n")
            logging.info(f"Table - {table_name} is either exist or created successfully")   
            return True
    except Error as e:
        print(f"Failed to create {table_name} table: {e} \n")
        logging.error(f"Failed to create {table_name} table: {e}")
        return False    

# Function main - this to be called from the data pipeline or Script can be run individually.
def main():
    try:

        # connectint to the db and get db connection handle
        logging.info(f"Wind Turbine - Databse Setup Starts")
        mysql_connection = conf.get_mysql_connection()
        print(f" MySQL Connection {mysql_connection} \n")
        
        if mysql_connection is None:
            print(f"MySQL connection failed.")     
            logging.info(f"MySQL Connection failed - check mysql_connection function in config.py")
            return
        
        # Creating DB if not exist
        if not create_database(mysql_connection):
            print("Failed to create database, aborting...\n")
            logging.error("Failed to create database, aborting...")
            return 
        
        # get DB connection
        print(f"get DB connection\n")
        logging.info(f"get DB connection")
        connection = conf.get_db_connection()

        if connection is None:
            print(f"MySQL DB connection failed. \n")     
            logging.error(f"DB Connection failed - check get_db_connection function in config.py")
            return
        
        # create tables
        tables = {
            conf.RAW_DATA_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {conf.RAW_DATA_TABLE} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    turbine_id INT NOT NULL,
                    wind_speed FLOAT,
                    wind_direction FLOAT,
                    power_output FLOAT,
                    insertion_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY (timestamp, turbine_id)
                );
            """,
            
            conf.INGESTION_TRACKER_TABLE: f'''
                        CREATE TABLE IF NOT EXISTS {conf.INGESTION_TRACKER_TABLE} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        data_insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        file_name VARCHAR(255),
                        last_record_timestamp DATETIME,
                        last_record_csv_row_number INT
                    );
                    ''',

            conf.CLEAN_DATA_TABLE: f'''
            CREATE TABLE IF NOT EXISTS {conf.CLEAN_DATA_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL,
                turbine_id INT NOT NULL,
                wind_speed FLOAT,
                wind_direction FLOAT,
                power_output FLOAT,
                insertion_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY (timestamp, turbine_id)
            );
            ''',

            conf.ANOMALIES_TABLE: f'''
            CREATE TABLE IF NOT EXISTS {conf.ANOMALIES_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL,
                turbine_id INT NOT NULL,
                wind_speed FLOAT,
                wind_direction FLOAT,
                power_output FLOAT,
                insertion_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY (timestamp, turbine_id)
            );
            ''',

            conf.MMM_TABLE: f'''
            CREATE TABLE IF NOT EXISTS {conf.MMM_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                period VARCHAR(50),

                wind_speed_mean FLOAT,
                wind_speed_median FLOAT,
                wind_speed_mode FLOAT,

                wind_direction_mean FLOAT,
                wind_direction_median FLOAT,
                wind_direction_mode FLOAT,

                power_output_mean FLOAT,
                power_output_median FLOAT,
                power_output_mode FLOAT,
                calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ''',

            conf.SUMMARY_STATS_TABLE: f'''
            CREATE TABLE IF NOT EXISTS {conf.SUMMARY_STATS_TABLE} (
                day DATE,
                turbine_id INT NOT NULL,
                min_power_output float,
                max_power_output float,
                avg_power_output float,
                insertion_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY (day, turbine_id)
            );
            ''',
        }

        # Create Tables 
        for table_name, query in tables.items():
            # print(table_name)
            # print(query)
            create_my_sql_table(connection, table_name, query)

    except Exception as e:  
        logging.error(f"Database Setup - Unexpected error occurred: {e}\n")
        print(f"Database Setup - Unexpected error occurred:: {e}")
        return
    finally:
        if connection:
            connection.close()
            logging.info("DB Connection closed.") 
            mysql_connection.close()
            logging.info("MySQL Connection closed.") 
 
        if mysql_connection:
            mysql_connection.close()
            logging.info("DB mysql_connection closed.") 
 

if __name__ == "__main__":
    #calling main()
    main()
