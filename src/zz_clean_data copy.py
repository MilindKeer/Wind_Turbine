import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import logging

# for mode calculation
from scipy import stats
import config as conf

# def create_clean_tables(connection):
#     """Create tables for clean data, mean median mode info and anomalies if they do not exist.
#        Note: Create DB table could be written as Utility Utility function but since number of tables are less didn't build 
#        a separate utility function 
#     """
#     try:
#         with connection.cursor() as cursor:
#             create_clean_table_query = f'''
#             CREATE TABLE IF NOT EXISTS {conf.CLEAN_DATA_TABLE} (
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 timestamp DATETIME NOT NULL,
#                 turbine_id INT NOT NULL,
#                 wind_speed FLOAT,
#                 wind_direction FLOAT,
#                 power_output FLOAT,
#                 insertion_date DATETIME DEFAULT CURRENT_TIMESTAMP,
#                 UNIQUE KEY (timestamp, turbine_id)
#             );
#             '''
#             cursor.execute(create_clean_table_query)
#             logging.info(f"Table - {conf.CLEAN_DATA_TABLE} is either exist or created sussefully")

#             create_anomalies_table_query = f'''
#             CREATE TABLE IF NOT EXISTS {conf.ANOMALIES_TABLE} (
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 timestamp DATETIME NOT NULL,
#                 turbine_id INT NOT NULL,
#                 wind_speed FLOAT,
#                 wind_direction FLOAT,
#                 power_output FLOAT,
#                 insertion_date DATETIME DEFAULT CURRENT_TIMESTAMP,
#                 UNIQUE KEY (timestamp, turbine_id)
#             );
#             '''
#             cursor.execute(create_anomalies_table_query)
#             logging.info(f"Table - {conf.ANOMALIES_TABLE} is either exist or created sussefully")

#             create_mean_median_mode_table_query = f'''
#             CREATE TABLE IF NOT EXISTS {conf.MMM_TABLE} (
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 period VARCHAR(50),

#                 wind_speed_mean FLOAT,
#                 wind_speed_median FLOAT,
#                 wind_speed_mode FLOAT,

#                 wind_direction_mean FLOAT,
#                 wind_direction_median FLOAT,
#                 wind_direction_mode FLOAT,

#                 power_output_mean FLOAT,
#                 power_output_median FLOAT,
#                 power_output_mode FLOAT,
#                 calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             );
#             '''
            
#             cursor.execute(create_mean_median_mode_table_query)
#             connection.commit()
#             logging.info(f"Table - {conf.MMM_TABLE} is either exist or created sussefully")
        
#         return True
    
#     except Error as e:
#         print(f"Error creating tables for cleaning data process: {e}")
#         logging.error(f"Error creating tables for cleaning data process: {e}")
#         return False

def get_max_timestamp(connection):
    # Fetch the maximum (latest) timestamp from the raw data table.
    try:
        with connection.cursor() as cursor:
            query = f"SELECT MAX(timestamp) FROM {conf.RAW_DATA_TABLE};"
            cursor.execute(query)
            max_timestamp = cursor.fetchone()[0]  # Fetch the latest timestamp
            return max_timestamp
        
    except Error as e:
        print(f"Error fetching max timestamp: {e}")
        return None  


def detect_and_store_anomalies(connection):
    """ Identify and store anomalies and store them in the anomalies table.
        anomalies : turbines whose output is outside of 2 standard deviations from the mean
    """
    try:
        with connection.cursor() as cursor:
            
            """ The mean would represent the central value of the data, providing a measure of the typical 
                power output for all turbines in the dataset.
                The standard deviation would measure how spread out the values are from the above mean.
                     
                Also, Check if clean data table exists; if yes, calculate mean and std based on clean data. 
                If not, fall back to raw data table.
            """
            check_clean_data_query = f"SHOW TABLES LIKE '{conf.CLEAN_DATA_TABLE}'"
            cursor.execute(check_clean_data_query)
            clean_data_exists = cursor.fetchone()  

            # Check whether clean table has data
            check_empty_query = f"SELECT COUNT(*) FROM {conf.CLEAN_DATA_TABLE};"
            cursor.execute(check_empty_query)
            row_count = cursor.fetchone()[0]

            if clean_data_exists and row_count > 0 :
                query_stats = f"""
                    SELECT AVG(power_output), STD(power_output) FROM {conf.CLEAN_DATA_TABLE};
                """    
                logging.info(f"Using {conf.CLEAN_DATA_TABLE} to calculate AVG and STD values")
            else:    
                """
                    if the first load of the historical data is out of propotion then we may have to use default values
                    for mean_power and std_power
                    and in that case we may need to modify below code with default standard values.
                """
                query_stats = f"""
                    SELECT AVG(power_output), STD(power_output) FROM {conf.RAW_DATA_TABLE};
                """
                logging.info(f"Using {conf.RAW_DATA_TABLE} to calculate AVG and STD values")


            cursor.execute(query_stats)
            mean_power, std_power = cursor.fetchone()

            print(f"mean_power: {mean_power}")
            print(f"std_power: {std_power}")
            logging.info(f"mean_power: {mean_power}")
            logging.info(f"std_power: {std_power}")

            if mean_power is None or std_power is None or std_power == 0:
                print(f"No data available to compute anomalies or can not calculate it")
                logging.warning(f"No data available to compute anomalies or can not calculate it")
                return

            """ The below logic is based on the empirical rule (also called the 68-95-99.7 rule) 
                which states that for a normal distribution:
                    68% of the data lies within 1 standard deviation of the mean.
                    95% of the data lies within 2 standard deviations of the mean.
                    99.7% of the data lies within 3 standard deviations of the mean.
                Here, as per instructions, we are considering 2 standard deviations
            """
            
            lower_bound = mean_power - 2 * std_power
            upper_bound = mean_power + 2 * std_power    

            print(f"lower_bound: {lower_bound}")
            print(f"upper_bound: {upper_bound}")
            logging.info(f"lower_bound: {lower_bound}")
            logging.info(f"upper_bound: {upper_bound}")


            """ Note - 'ON DUPLICATE KEY UPDATE' used to attempt an INSERT operation, 
                so that if a record already exists with the same unique key, it will update 
                the existing record instead of inserting a new one
            """

            # Check if the ANOMALIES_TABLE has records
            check_anomalies_table_query = f"SELECT COUNT(*) FROM {conf.ANOMALIES_TABLE};"
            cursor.execute(check_anomalies_table_query)
            anomalies_table_row_count = cursor.fetchone()[0]

            if anomalies_table_row_count > 0:
                
                
                insert_anomalies_query = f"""
                    INSERT INTO {conf.ANOMALIES_TABLE} (timestamp, turbine_id, wind_speed, wind_direction, power_output)
                    SELECT timestamp, turbine_id, wind_speed, wind_direction, power_output
                        FROM {conf.RAW_DATA_TABLE}
                        WHERE power_output < %s OR power_output > %s and timestamp > %s
                        ON DUPLICATE KEY UPDATE 
                            wind_speed = VALUES(wind_speed),
                            wind_direction = VALUES(wind_direction),
                            power_output = VALUES(power_output),
                            insertion_date = CURRENT_TIMESTAMP;
                    """
                

            else:
                # skipping timestamp > %s in the where clause
                insert_anomalies_query = f"""
                    INSERT INTO {conf.ANOMALIES_TABLE} (timestamp, turbine_id, wind_speed, wind_direction, power_output)
                    SELECT timestamp, turbine_id, wind_speed, wind_direction, power_output
                        FROM {conf.RAW_DATA_TABLE}
                        WHERE power_output < %s OR power_output > %s
                        ON DUPLICATE KEY UPDATE 
                            wind_speed = VALUES(wind_speed),
                            wind_direction = VALUES(wind_direction),
                            power_output = VALUES(power_output),
                            insertion_date = CURRENT_TIMESTAMP;
                    """
                logging.info(f"Skipping timestamp filter for anomalies data insertion (first time run or table empty).")

            cursor.execute(insert_anomalies_query, (lower_bound, upper_bound))
            
            connection.commit()
            
            print(f"anomalies detected and stored successfully.")
            logging.info(f"anomalies detected and stored successfully.")

            return True
    except Error as e:
        print(f"Error detecting anomalies: {e}\n")
        logging.error(f"Error detecting and storing anomalies: {e}")
        return False

def get_filtered_data(connection, period_start):
    """Fetch data from the raw table filtered by the given time period."""
    try:
        
        with connection.cursor(dictionary=True) as cursor:
            
            """ Pandas .mean() and .median() handle NaN by default, but mode might return an unexpected result.
                hence filtering NULLs
                Also, excluding anomalies to get more accurate data
            """

            query = f"""
                SELECT wind_speed, wind_direction, power_output 
                    FROM {conf.RAW_DATA_TABLE} 
                WHERE 
                    wind_speed IS NOT NULL 
                    AND wind_direction IS NOT NULL 
                    AND power_output IS NOT NULL
                    AND (timestamp, turbine_id) NOT IN (SELECT timestamp, turbine_id FROM {conf.ANOMALIES_TABLE})    
                """
            
            print(f"period_start: {period_start} \n")
            logging.info(f"period_start: {period_start}")

            if period_start:
                # data set as per given period
                cursor.execute(query + " AND timestamp >= %s", (period_start,))
            else:
                # full data set
                cursor.execute(query)
            
            data = cursor.fetchall()
            
            # return the dataframe
            return pd.DataFrame(data)
        
    except Error as e:
        print(f"Error fetching filtered data from the raw data table: {e}")
        return pd.DataFrame() # shall we still return the datframe?
        logging.error(f"Error fetching filtered data from the raw data table: {e}")

    except Exception as e:
        print(f"get_filtered_data failed with Unexpected error: {e}")
        logging.error(f"get_filtered_data failed with Unexpected error: {e}")
        return None

def calculate_statistics(df):
    # function to calculate stats.
    try:
        # Calculate mean, median, and mode for wind_speed, wind_direction, and power_output.
        stats_dict = {}
        if df.empty:
            return {
                "wind_speed": {"mean": None, "median": None, "mode": None},
                "wind_direction": {"mean": None, "median": None, "mode": None},
                "power_output": {"mean": None, "median": None, "mode": None}
            }

        for col in ["wind_speed", "wind_direction", "power_output"]:
            # check whether mode value is valid
            mode_result = stats.mode(df[col].dropna(), keepdims=True)
            mode_value = mode_result.mode[0] if mode_result.count[0] > 0 else None
            
            stats_dict[col] = {
                "mean": df[col].mean(),
                "median": df[col].median(),
                "mode": mode_value
            }

        return stats_dict
    
    except ValueError as e:
        print(f"calculate_statistics failed with ValueError: {e}")
        logging.error(f"calculate_statistics failed with ValueError: {e}")
        return None

    except TypeError as e:
        print(f"calculate_statistics failed with TypeError: {e}")
        logging.error(f"calculate_statistics failed with TypeError: {e}")
        return None

    except Exception as e:
        print(f"calculate_statistics failed with Unexpected error: {e}")
        logging.error(f"calculate_statistics failed with Unexpected error: {e}")
        return None


def store_statistics(connection, period_name, stats_dict):
    # Store calculated statistics in the database.
    try:
        with connection.cursor() as cursor:
                
            insert_query = f"""
            INSERT INTO {conf.MMM_TABLE} (period, wind_speed_mean, wind_speed_median, wind_speed_mode, 
                                            wind_direction_mean, wind_direction_median, wind_direction_mode,
                                            power_output_mean, power_output_median, power_output_mode, calculation_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE 
                wind_speed_mean = VALUES(wind_speed_mean), wind_speed_median = VALUES(wind_speed_median), wind_speed_mode = VALUES(wind_speed_mode),
                wind_direction_mean = VALUES(wind_direction_mean), wind_direction_median = VALUES(wind_direction_median), wind_direction_mode = VALUES(wind_direction_mode),
                power_output_mean = VALUES(power_output_mean), power_output_median = VALUES(power_output_median), power_output_mode = VALUES(power_output_mode),
                calculation_timestamp = VALUES(calculation_timestamp);
            """
            cursor.execute(insert_query, (
                period_name,
                stats_dict["wind_speed"]["mean"], stats_dict["wind_speed"]["median"], stats_dict["wind_speed"]["mode"],
                stats_dict["wind_direction"]["mean"], stats_dict["wind_direction"]["median"], stats_dict["wind_direction"]["mode"],
                stats_dict["power_output"]["mean"], stats_dict["power_output"]["median"], stats_dict["power_output"]["mode"]
            ))
            connection.commit()
        
    except mysql.connector.Error as e:
        print(f"Error inserting statistics: {e}")

def process_statistics(connection):
    
    """Function to calculate and store statistics for different periods."""
    try:
        # get the max timestamp from the raw data table.
        max_timestamp = get_max_timestamp(connection)
        
        if max_timestamp is None:
            print("No data available for processing statistics.")
            return

        """ pulling the data for below mentioned periods
            this would give flexibilty to use most relevant when it comes to updating anomalies and missing
            data of the turbine.
        """
        periods = {
            "full_dataset": None,
            "last_4_weeks": max_timestamp - timedelta(weeks=4),
            "last_2_weeks": max_timestamp - timedelta(weeks=2),
            "last_1_week": max_timestamp - timedelta(weeks=1),
            "last_1_day": max_timestamp - timedelta(days=1)
        }

        for period_name, period_start in periods.items():
            print(f"Processing statistics for: {period_name}")
            
            # get the filtered data for the given period 
            print(f"Getting the filtered data for : {period_name}")
            
            df = get_filtered_data(connection, period_start)

            if not df.empty:
                stats_dict = calculate_statistics(df)
                # Skip storing if stats_dict is None
                if stats_dict is None:
                    print(f"Skipping {period_name} due to empty stats.")
                    continue  
                else:    
                    store_statistics(connection, period_name, stats_dict)
        return True            
                        
    except Error as e:
        print(f"Error processing statistics: {e}")
        return False

def update_clean_table(connection):
    """Create a clean data table by imputing missing values and anomalies."""
    try:
        with connection.cursor() as cursor:
            
            """using INSERT IGNORE, to ensure no duplicates are inserted and only new records
            will be applied.

            -- Using median for wind_speed
            -- Using median for wind_direction
            -- Using mean for power_output 
            """

            query = f"""
            INSERT IGNORE INTO {conf.CLEAN_DATA_TABLE} (timestamp, turbine_id, wind_speed, wind_direction, power_output)
            SELECT 
                timestamp, turbine_id, 
                COALESCE(wind_speed, (SELECT wind_speed_median FROM {conf.MMM_TABLE} WHERE period= '{conf.PERIOD_FOR_STATS}')),  
                COALESCE(wind_direction, (SELECT wind_direction_median FROM {conf.MMM_TABLE} WHERE period= '{conf.PERIOD_FOR_STATS}')),  
                COALESCE(power_output, (SELECT power_output_mean FROM {conf.MMM_TABLE} WHERE period= '{conf.PERIOD_FOR_STATS}'))     
            FROM {conf.RAW_DATA_TABLE}
            WHERE NOT EXISTS (
                SELECT 1 FROM {conf.ANOMALIES_TABLE} o
                WHERE {conf.RAW_DATA_TABLE}.timestamp = o.timestamp
                AND {conf.RAW_DATA_TABLE}.turbine_id = o.turbine_id
            );
            """

            # print(query)

            cursor.execute(query)
            connection.commit()
            print(f"Clean data updated successfully")
            logging.info(f"Clean data updated successfully")

        return True    
    except Error as e:
        print(f"Error updating clean table: {e}\n")
        logging.error(f"Error updating clean table: {e}")
        return False

def main():
    try:
        # connectint to the db and get db connection handle
        logging.info(f"Wind Turbine - Data Cleaning Starts")
        
        # get DB connection
        print(f"Step 1 - get DB connection\n")
        logging.info(f"Step 1 - get DB connection")
        connection = conf.get_db_connection()

        if connection is None:
            print(f"MySQL DB connection failed. \n")     
            logging.error(f"DB Connection failed - check get_db_connection function in config.py")
            return
        else:
            print(f"MySQL DB connection is successful. \n")     
            logging.info(f"MySQL DB connection is successful.")
            
        """ Start data cleaning process
            Create wind_turbine_clean_data, wind_turbine_anomalies tables and wind_turbine_mean_median_mode_stats tables
        """
        print(f"Step 2 - Data cleaning process starts\n")
        logging.info(f"Step 2 - Data cleaning process starts")
        
        # if not create_clean_tables(connection):
        #     print("Failed to create clean tables, aborting...")
        #     logging.error("Failed to create clean tables, aborting...")
        #     return  
        # else:
        #     print(f"All tables required for cleaning process either exist or created successfully \n")
        #     logging.info(f"All tables required for cleaning process either exist or created successfully")


        """ First, identify and store anomalies / anomalies: turbines whose output is outside of 2 
            standard deviations from the mean 
        """

        print(f"Step 3 - Detect & Store anomalies \n")
        logging.info(f"Step 3 - Detect & Store anomalies")
        if not detect_and_store_anomalies(connection):
            print("Failed to identify and store anomalies, aborting...")
            logging.error("Failed to identify and store anomalies, aborting...")
            return  
        
        # process statistics
        print(f"Step 4 - Process Statistics i.e. process and store stats for different period \n")
        logging.info(f"Step 4 - Process Statistics i.e. process and store stats for different period")
        if not process_statistics(connection):
            print("Failed to process statistic, aborting...")
            logging.error("Failed to process statistic, aborting...")
            return  
        
        # update clean table
        logging.info(f"Step 5 - Update clean data table for missing and anomalies values")
        if not update_clean_table(connection):
            print("Failed to update clean data, aborting...")
            logging.error("Failed to update clean data, aborting...")
            return  
        
        # we will call from Data Pipeline hence included return here.
        return True
    
    except Exception as e:  
        logging.error(f"Cleaning data process - Unexpected error occurred: {e}\n")
        print(f"Cleaning data process - Unexpected error occurred: {e}")
        return
    finally:
        if connection:
            connection.close()
            logging.info("DB Connection closed.") 

if __name__ == "__main__":
    main()
