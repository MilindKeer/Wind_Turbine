# from configparser import Error
import logging
import config as conf
from mysql.connector import Error


try:
    ''' connectint to the db and get db connection handle'''
    logging.info("Wind Turbine - Data ingestion Starts")
    connection = conf.get_db_connection()
    print(connection)
    if connection is None:
        print("DB connection failed.")     
    else:
        ''' start data ingestion process '''
        # ingest_all_csvs(connection)
        connection.close()
except Error as e:
        print(f"error get_db_connection: {e}")