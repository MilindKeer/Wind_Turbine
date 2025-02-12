from datetime import datetime
import logging
import os

import mysql

# Database Credentials
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "milind"  # Replace with your MySQL username
DB_PASSWORD = "Arnav@123"  # Replace with your MySQL password
DB_NAME = "wind_turbine_db"

# Source Data CSV Prefix
SOURCE_DATA_CSV_PREFIX = "data_group_"

# Table Names
RAW_DATA_TABLE = "wind_turbine_raw_data"
INGESTION_TRACKER_TABLE = "wind_turbine_ingestion_tracker"
ANOMALIES_TABLE = "wind_turbine_anomalies"
MMM_TABLE = "wind_turbine_mean_median_mode_stats"
CLEAN_DATA_TABLE = "wind_turbine_clean_data"
SUMMARY_STATS_TABLE = "wind_turbine_summary_stats"
SUMMARY_ANOMALIES_STATS_TABLE = "wind_turbine_anomalies_summary_stats"


# Folder Names
RAW_DATA_FOLDER = 'data/raw_data'
ARCHIVE_FOLDER = 'data/archive'
LOGS_DIR = "logs"

os.makedirs(RAW_DATA_FOLDER, exist_ok=True)
os.makedirs(ARCHIVE_FOLDER, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# choose appropriate Period for stats
PERIOD_FOR_STATS = "full_dataset"
# PERIOD_FOR_STATS = "last_4_weeks"
# PERIOD_FOR_STATS = "last_2_weeks"
# PERIOD_FOR_STATS = "last_1_week"
# PERIOD_FOR_STATS = "last_1_day"


# Logging Configuration
# LOG_FILE = f"script{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
LOG_FILE = f"script_{datetime.now().strftime('%Y-%m-%d T %H-%M-%S')}.log"

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)  # Create logs folder if not exists

# Generate log filename with timestamp
log_filename = os.path.join(LOGS_DIR, LOG_FILE)

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",  
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Other Constants (if needed later)
def get_db_connection():
    """Establish and return a MySQL database connection."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        # print(connection)
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL DB: {e}")
        logging.error("Error connecting to MySQL DB: {e}")
        return None
    
def get_mysql_connection():
    """Establish and return a MySQL connection without DB"""
    try:
        mysql_connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # print(mysql_connection)
        return mysql_connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        logging.error("Error connecting to MySQL: {e}")
        return None    