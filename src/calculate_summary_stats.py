from mysql.connector import Error
from numpy import empty
import pandas as pd
from datetime import datetime, timedelta
import logging
import config as conf


def drop_and_create_summary_table(connection, cursor, turbine_ids):
    logging.info(f"drop_and_create_summary_table function called....\n")
    try:
        # Drops and recreates the anomaly summary table dynamically based on turbine IDs.
        cursor.execute(f"DROP TABLE IF EXISTS {conf.SUMMARY_ANOMALIES_STATS_TABLE}")
        
        if not turbine_ids:
            raise ValueError("Turbine IDs list is empty. Cannot create table without columns.")
            

        # create new table
        columns = ", ".join([f"`Turbine_ID_{tid}` INT DEFAULT 0" for tid in turbine_ids])
        
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {conf.SUMMARY_ANOMALIES_STATS_TABLE} (
                day DATE PRIMARY KEY,
                {columns}
            )
        """
        cursor.execute(create_table_query)
        connection.commit()
        return True
    except Error as e:
       #print(f"drop_and_create_summary_table function failed: {e}")
        logging.error(f"drop_and_create_summary_table function failed: {e}")
        return False

def generate_summary_stats_query(turbine_ids):
    logging.info(f"generate_summary_stats_query function called....\n")
    insert_query = ""
    try:
        select_columns = []
        
        # Create the CASE WHEN clauses for each turbine
        for turbine_id in turbine_ids:
            ##print(f"turbine_id - {turbine_id}")
            column_name = f"Turbine_ID_{turbine_id}"
            select_columns.append(f"COUNT(CASE WHEN turbine_id = {turbine_id} THEN 1 END) AS {column_name}")
        
        # Combine all parts into the final query
        select_columns_str = ", ".join(select_columns)
        ##print(f"select_columns_str: {select_columns_str}")

        query = f"""
        SELECT 
            DATE(timestamp) AS day, 
            {select_columns_str}
        FROM 
            {conf.ANOMALIES_TABLE}
        GROUP BY 
            day
        ORDER BY 
            day;
        """
        ##print(query)

        insert_query = f"""
            INSERT INTO {conf.SUMMARY_ANOMALIES_STATS_TABLE} (day, {", ".join([f"Turbine_ID_{tid}" for tid in turbine_ids])})
            {query}
        """

        return insert_query

    except Exception as e:
       #print(f"generate_summary_stats_query function failed: {e}") 
        logging.error(f"generate_summary_stats_query function failed: {e}")
        return ""

def get_anomalies_summary_stats(connection):
    logging.info(f"get_anomalies_summary_stats function called...\n")
    try:
        with connection.cursor() as cursor:
            # Get distinct turbine IDs
            cursor.execute(f"SELECT DISTINCT turbine_id FROM {conf.ANOMALIES_TABLE}")
            turbine_ids = [row[0] for row in cursor.fetchall()]
            ##print(f"turbine_ids: {turbine_ids}")
            
            if not turbine_ids:
               #print("No anomaly data found. Skipping summary update.")
                return

            """
                drop existint table and create new.
                we never know howmany turbined would appeared in the anomolies table hence 
                have to drop and re-create tables as columns are dynamic.
            """
            if not drop_and_create_summary_table(connection,cursor, turbine_ids):
               #print(f"drop_and_create_summary_table function failed")
                logging.error(f"drop_and_create_summary_table function failed")
                return
            
            # Generate dynamic SQL query
            query = generate_summary_stats_query(turbine_ids)
            ##print(query)

            if query.strip():
                cursor.execute(query)
                connection.commit()
                #print("Anomaly summary inserted successfully.")
                logging.info("Anomaly summary insertion successful.")
                return True
            else:
                #print("Anomaly summary insertion failed.")
                logging.error("Anomaly summary insertion failed due to an empty or invalid query.")
                return False
            
    except Error as e:
       #print(f"get_anomalies_summary_stats function failed: {e}")
        logging.error(f"get_anomalies_summary_stats function failed: {e}")
        return False

def calculate_summary_stats(connection):
    logging.info(f"calculate_summary_stats function called...\n")
    try:
        with connection.cursor(dictionary=True) as cursor:

            """ Calculates summary statistics: For each turbine, calculate the minimum, maximum, and average 
                power output over a given time period (e.g., 24 hours) 
                this is calculate per day. 
            """
            
            query = f"""
                    SELECT DATE(timestamp) AS day, turbine_id, power_output FROM {conf.CLEAN_DATA_TABLE}
                """
            cursor.execute(query)
            clean_data = cursor.fetchall()

            if not clean_data:
               #print("No data available in the {conf.CLEAN_DATA_TABLE} table.")
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(clean_data)

            # Aggregate statistics (Min, Max, Avg Power Output)
            summary_df = df.groupby(["day", "turbine_id"])["power_output"].agg(
                min_power_output="min",
                max_power_output="max",
                avg_power_output="mean"
            ).reset_index()
            
            # Insert summary stats into the database
            insert_query = f"""
                INSERT INTO {conf.SUMMARY_STATS_TABLE} (day, turbine_id, min_power_output, max_power_output, avg_power_output)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                min_power_output = VALUES(min_power_output),
                max_power_output = VALUES(max_power_output),
                avg_power_output = VALUES(avg_power_output)
            """
            
            ##print(insert_query)

            summary_records = [
                (row["day"], row["turbine_id"], row["min_power_output"], row["max_power_output"], row["avg_power_output"])
                for _, row in summary_df.iterrows()
            ]
            
            # Execute batch insert
            if summary_records: 
                cursor.executemany(insert_query, summary_records)
                connection.commit()
                logging.info("Summary statistics updated successfully.")
                return True
            else:
                logging.warning("No summary records to insert.")
                return False
            
        #print("Summary statistics updated successfully.")
        return True        
    
    except Exception as e:
       #print(f"calculate_summary_stats failed with Unexpected error: {e}")
        logging.error(f"calculate_summary_stats failed with Unexpected error: {e}")
        return False 

def main():

    try: 
        # connectint to the db and get db connection handle
        logging.info(f"Wind Turbine - calculate summary stats starts \n")
        
        # get DB connection
       #print(f"Step 1 - get DB connection\n")
        logging.info(f"Step 1 - get DB connection")
        connection = conf.get_db_connection()

        if connection is None:
           #print(f"MySQL DB connection failed. \n")     
            logging.error(f"DB Connection failed - check get_db_connection function in config.py")
            return False
        else:
           #print(f"MySQL DB connection is successful. \n")     
            logging.info(f"MySQL DB connection is successful.")

       #print(f"Step 2 - calculate summary stats\n")
        logging.info(f"Step 2 - calculate summary stats")
        
        if not calculate_summary_stats(connection):
           #print("Failed to calculate_summary_stats, aborting...")
            logging.error("Failed to calculate_summary_stats, aborting...")
            return False  
        else:
           #print(f"calculate_summary_stats is successful \n")
            logging.info(f"calculate_summary_stats is successful")

        if not get_anomalies_summary_stats(connection):
           #print("Failed to get_anomalies_summary_stats, aborting...")
            logging.error("Failed to get_anomalies_summary_stats, aborting...")
            return False  
        else:
           #print(f"get_anomalies_summary_stats is successful \n")
            logging.info(f"get_anomalies_summary_stats is successful")

        return False
    except Exception as e:  
        logging.error(f"calculate_summary_stats - Unexpected error occurred: {e}\n")
       #print(f"calculate_summary_stats - Unexpected error occurred: {e}")
        return False
    finally:
        if connection:
            connection.close()
            logging.info("DB Connection closed.") 
    
if __name__ == "__main__":
    result = main() 
    logging.info(f"Summary Calculation- completed \n")
    