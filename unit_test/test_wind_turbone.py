import pytest
from unittest.mock import MagicMock
from unittest.mock import patch, MagicMock
import os
import csv
import sys
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error


# Get the parent directory (for src)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT_DIR, "src"))

import config  
import ingest_data  

# Mock DB table names
MOCK_RAW_DATA_TABLE = 'mock_wind_turbine_raw_data'
MOCK_INGESTION_TRACKER_TABLE = 'mock_wind_turbine_ingestion_tracker'

file_path = os.path.join(ROOT_DIR, "mock_data", "raw", "mock_file.csv")

@pytest.fixture
def mock_db_connection():
    """Fixture to mock database connection"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    return mock_connection, mock_cursor


@patch("mysql.connector.connect")
def test_get_db_connection_success(mock_connect, mock_db_connection):
    """Test successful database connection."""
    mock_connect.return_value = mock_db_connection
    connection = config.get_db_connection()
    
    assert connection is not None
    mock_connect.assert_called_once_with(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_NAME
    )

@patch("mysql.connector.connect", side_effect=mysql.connector.Error("Connection failed"))
def test_get_db_connection_failure(mock_connect):
    """Test database connection failure."""
    connection = config.get_db_connection()
    
    assert connection is None
    mock_connect.assert_called_once()

@patch("mysql.connector.connect")
def test_get_mysql_connection_success(mock_connect, mock_db_connection):
    """Test successful MySQL connection without DB selection."""
    mock_connect.return_value = mock_db_connection
    connection = config.get_mysql_connection()
    
    assert connection is not None
    mock_connect.assert_called_once_with(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )

@patch("mysql.connector.connect", side_effect=mysql.connector.Error("Connection failed"))
def test_get_mysql_connection_failure(mock_connect):
    """Test MySQL connection failure without DB selection."""
    connection = config.get_mysql_connection()
    
    assert connection is None
    mock_connect.assert_called_once()


@pytest.fixture
def setup_mock_db(mock_db_connection):
    """Fixture to create mock tables in the database"""
    mock_connection, mock_cursor = mock_db_connection

    mock_cursor.execute("""
        CREATE TABLE IF NOT EXISTS mock_wind_turbine_raw_data (
            timestamp DATETIME,
            turbine_id INT,
            wind_speed FLOAT,
            wind_direction FLOAT,
            power_output FLOAT
        );
    """)
    mock_cursor.execute("""
        CREATE TABLE IF NOT EXISTS mock_wind_turbine_ingestion_tracker (
            file_name VARCHAR(255),
            last_record_timestamp DATETIME,
            last_record_csv_row_number INT
        );
    """)
    mock_connection.commit()
    yield mock_cursor

    mock_cursor.execute("DROP TABLE IF EXISTS mock_wind_turbine_raw_data;")
    mock_cursor.execute("DROP TABLE IF EXISTS mock_wind_turbine_ingestion_tracker;")
    mock_connection.commit()

@pytest.fixture
def mock_csv_data():
    """Fixture for mocked CSV data"""
    return pd.DataFrame({
        'timestamp': [datetime.strptime('01/03/2022 00:00:00', '%d/%m/%Y %H:%M:%S') for _ in range(2)],
        'turbine_id': [1, 2],
        'wind_speed': [10.5, 15.2],
        'wind_direction': [100, 200],
        'power_output': [1500, 2000],
    })

@pytest.fixture
def save_mock_data_to_csv(mock_csv_data):
    """Fixture to create and return the mock CSV file path"""
    data = mock_csv_data
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data.columns)
        writer.writeheader()
        writer.writerows(data.to_dict(orient="records"))

    return file_path

def test_mock_csv_creation(save_mock_data_to_csv):
    """Test to check if mock CSV file is created"""
    assert os.path.exists(save_mock_data_to_csv), f"❌ File not found: {save_mock_data_to_csv}"


def test_ingest_csv(mock_db_connection, save_mock_data_to_csv):
    """Test the ingest_csv function from ingest_data.py"""
    
    mock_connection, mock_cursor = mock_db_connection  # Get mock DB connection
    file_path = save_mock_data_to_csv  # Get file path from fixture
    
    # Mock the behavior of `fetchone` to return a tuple (e.g., (1,))
    mock_cursor.fetchone.return_value = (1,)

    # Call the ingest function (assuming it takes file_path and db_connection)
    # ingest_data.ingest_csv(mock_connection, file_path, MOCK_RAW_DATA_TABLE, MOCK_INGESTION_TRACKER_TABLE)
    ingest_data.ingest_csv(mock_connection, file_path)

    # Check if data was inserted
    mock_cursor.execute(f"SELECT COUNT(*) FROM {MOCK_RAW_DATA_TABLE};")
    row_count = mock_cursor.fetchone()[0]
    
    assert row_count > 0, "Failed - No records inserted into the table!"

    print(f"Passed - {row_count} rows successfully inserted into {MOCK_RAW_DATA_TABLE}")



if __name__ == "__main__":
    pytest.main()
    