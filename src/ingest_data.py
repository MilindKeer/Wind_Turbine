import logging
import os
import shutil
import traceback
import pandas as pd
# import mysql.connector
from mysql.connector import Error
from datetime import datetime
import config as conf


def move_csv_to_archive(file_path):
    # #print(f"move_csv_to_archive function called.... \n")
    logging.info(f"move_csv_to_archive function called....\n")
    try:
        # Move the file to archive folder
        archive_file = os.path.join(conf.ARCHIVE_FOLDER, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(file_path)}")
        shutil.move(file_path, archive_file)
        #print(f"Processed and archived: {file_path} \n")
        logging.info(f"{file_path} moved to Archive folder")
    except (OSError, IOError) as e:
        #print(f"Error while moving {file_path} csv to the Archive folder: {e} \n")
        logging.info(f"Error while moving {file_path} csv to the Archive folder: {e}")
        return False 


def get_last_processed_info(connection,file_name):
    
    # #print(f"get_last_processed_info function called.... \n")
    logging.info(f"get_last_processed_info function called....\n")

    """ Data is supplied via daily appending CSVs, meaning previously processed records will 
        also appear in the same CSV. This function is written to fetch the information of the
        last record. i.e.
        last_record_timestamp &
        last_record_csv_row_number 

        Note: 'wind_turbine_ingestion_tracke' table is built to keep track of every processed CSV.

    """
    try:
        with connection.cursor() as cursor:
            query = f"""
            SELECT last_record_timestamp, last_record_csv_row_number 
                FROM {conf.INGESTION_TRACKER_TABLE} 
                WHERE file_name = %s
            ORDER BY data_insertion_date DESC 
            LIMIT 1
            """
            logging.info(f"SQL Query for get_last_processed_info - {query}")

            cursor.execute(query, (file_name,))
            result = cursor.fetchone()
            #print(f"Last record from previous load: {result}\n")
            logging.info(f"Last record from previous load: {result}")

            if result is None:
                logging.info(f"No previous records found for {file_name}")
                return None 

            return result
    except Error as e:
        #print(f"Error fetching last processed info: {e}\n")
        logging.error(f"Error fetching last processed info: {e}")
        return None
    
def update_wind_turbine_ingestion_tracker(connection, file_name, last_record_timestamp,last_record_csv_row_number):
    
    #print(f"update_wind_turbine_ingestion_tracker function called.... \n")
    logging.info(f"update_wind_turbine_ingestion_tracker function called....\n")

    """ Function to update last processed timestamp 
        This function keeps a track of every CSV load.
    """
    try:
        with connection.cursor() as cursor:
            insert_query = f"""
            INSERT INTO {conf.INGESTION_TRACKER_TABLE} (file_name, last_record_timestamp, last_record_csv_row_number)
            VALUES (%s, %s, %s)
            """
            
            logging.info(f"SQL query for update_wind_turbine_ingestion_tracker: {insert_query}")
            
            cursor.execute(insert_query, (file_name, last_record_timestamp, last_record_csv_row_number))
            connection.commit()
            logging.info(f"Table {conf.INGESTION_TRACKER_TABLE} updated")
        return True    
    except Error as e:
        #print(f"Error updating ingestion tracker: {e}")
        logging.error(f"Error updating ingestion tracker: {e}")
        return False

def ingest_csv(connection, file_path):
    
    # #print(f"ingest_csv function called.... \n")
    logging.info(f"ingest_csv function called....\n")
    
    """Ingest CSV data into the raw table, skipping already loaded rows."""
    try:
        # get the cursor
        cursor = connection.cursor()
        # get the only file name from the file path
        file_name_only = os.path.basename(file_path)
        #print(f"CSV only file name without path: {file_name_only} \n")
        
        """ Data is supplied via daily appending CSVs, meaning previously processed records will 
            also appear in the same CSV. Therefore, the last processed information needs to be collected."""

        last_csv_processed_info = get_last_processed_info(connection,file_name_only)
        #print(f"last_csv_processed_info: {last_csv_processed_info} \n")
        logging.info(f"last_csv_processed_info: {last_csv_processed_info}")
        
        if last_csv_processed_info is None:
            #print(f"first run for {file_path}. Processing all records.\n")
            #here reading the entire file and creating Panda's dataframe
            new_data = pd.read_csv(file_path)
            new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])

            # captyring last record timestamp and row numbet to update 'wind_turbine_ingestion_tracker' table.
            last_record_timestamp = new_data.iloc[-1]['timestamp']
            last_record_row_number = len(new_data)
            #print(f"last_record_timestamp: {last_record_timestamp} \n")
            #print(f"last_record_row_number: {last_record_row_number} \n")
        else:
            # Use the tracking info to find the new data/rows in the CVS file
            last_record_timestamp, last_record_row_number = last_csv_processed_info
            # Already processed records are ignored and only new data will be read by skipping earlier processed records
            new_data = pd.read_csv(file_path, skiprows=range(1, last_record_row_number + 1))  
            new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])

            if not new_data.empty:
                last_record_timestamp = new_data.iloc[-1]['timestamp']
                last_record_row_number = last_record_row_number + len(new_data)
                #print(f"new last_record_timestamp: {last_record_timestamp} \n")
                #print(f"new last_record_row_number: {last_record_row_number} \n")
       
        # If no new data provided in the CSV
        if new_data.empty:
            # Note - no new data found but still moving file to the archive folder
            #print(f"no new data found in the {file_path} csv but still file moved to the archive folder \n")
            logging.info(f"no new data found in the {file_path} csv but still file moved to the archive folder")
            # Move the file to archive folder
            move_csv_to_archive(file_path)
            return False
        else:
            #print(f"Processing new data {len(new_data)} new rows for {file_path}.\n")
            logging.info(f"Processing new data {len(new_data)} new rows for {file_path}")
                            
            for row in new_data.itertuples(index=False, name=None):       
                if len(row) == 5 and row[0] is not None and row[1] is not None:
                    wind_speed = row[2] if row[2] else None
                    wind_direction = row[3] if row[3] else None
                    power_output = row[4] if row[4] else None    
                
                    insert_query = f'''
                    INSERT INTO {conf.RAW_DATA_TABLE} (timestamp, turbine_id, wind_speed, wind_direction, power_output)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE wind_speed=VALUES(wind_speed), wind_direction=VALUES(wind_direction), power_output=VALUES(power_output);
                    '''
                    cursor.execute(insert_query, (
                        row[0], row[1], wind_speed,wind_direction, power_output
                    ))
                    
                else:
                    #print(f"Data loading to Raw Data table - Skipping invalid row: {row}")
                    logging.warning(f"Data loading to Raw Data table - Skipping invalid row: {row}")    
            
            connection.commit()
            cursor.close()
            logging.info(f"Data load ({len(new_data)} records) for {file_path} is Successful")

            # Update wind turbine load tracker table for future reference.
            if last_record_timestamp and last_record_row_number:
                update_wind_turbine_ingestion_tracker(connection, file_name_only, last_record_timestamp, last_record_row_number)
                # Move the file to archive folder
                move_csv_to_archive(file_path)
                
                #print(f"\nCSV file: {file_name_only} processing ends \n")
            
            return True
        
    except Exception as e:
        #print(f"Error ingesting CSV {file_path}: {e}")
        # rolling back
        connection.rollback()
        traceback.print_exc() 
        logging.error(f"Error ingesting CSV {file_path}: {e}")
        return False
        
def ingest_all_csvs(connection):
    # #print(f"ingest_all_csvs function called.... \n")
    logging.info(f"ingest_all_csvs function called....\n")

    try:
        """ Get CSVs from the raw data folder 
            Here, check the name of the CSV as in Prefix and also extension 'csv' all other files
            (if any) will be ignored.
            also later (once file is processed it will be moved to the Archive folder)
        """
        csv_files = [f for f in os.listdir(conf.RAW_DATA_FOLDER) if f.startswith(conf.SOURCE_DATA_CSV_PREFIX) and f.endswith('.csv')]
        
        if not csv_files:
            #print(f"\nThere are no new CSV files found in the data/raw folder\n")
            logging.warning(f"There are no new CSV files found in the data/raw folder")
            return False
        else:    
            success = True
            for file in csv_files:
                #print(f"\nCSV file: {file} processing starts \n")
                logging.info(f"CSV file: {file} processing starts")

                # calling ingest_csv function to ingest the data
                result = ingest_csv(connection, os.path.join(conf.RAW_DATA_FOLDER, file))
                
                if not result:
                    logging.error(f"Failed to process CSV: {file}")
                    success = False
                    
                logging.info(f"CSV file: {file} processing ends")
            return success
    except Error as e:
        #print(f"error ingest_all_csvs: {e}")
        logging.error(f"error ingest_all_csvs: {e}")
    
    except (OSError, IOError) as fe:
        #print(f"Error processing CSV files: {fe}")
        logging.error(f"Error processing CSV files: {fe}")    

def main():
    # #print(f"main function called.... \n")
    # logging.info(f"indest_data - main function called....")
    try:
        # connectint to the db and get db connection handle
        logging.info(f"Wind Turbine - Data ingestion Starts \n")
        logging.info(f"Step 1 - get DB connection")
        connection = conf.get_db_connection()
        #print(f" DB Connection {connection} \n")
        if connection is None:
            #print(f"MySQL DB connection failed.")     
            logging.info(f"DB Connection failed - check get_db_connection function in config.py")
            return False
        else:
            # start data ingestion process
            logging.info(f"Step 2 - start ingesting CSVs")
            if not ingest_all_csvs(connection):
                logging.error(f"Failed to ingest one or all CSVs")
                return False  
            
            return True
    
    except Error as e:
        #print(f"error get_db_connection: {e} \n")
        logging.error(f"error get_db_connection: {e}")
        return False
    finally:
        if connection:
            connection.close()  
            logging.info(f"DB connection closed.")  

if __name__ == "__main__":
    result = main()
    logging.info(f"Data Ingestion - completed \n")
    