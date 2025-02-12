const fs = require('fs');
const path = require('path');

// Function to get folder structure
function getFolderStructure(dirPath) {
  let structure = '';
  const files = fs.readdirSync(dirPath); // Get the list of files/folders in the directory

  files.forEach(file => {
    const fullPath = path.join(dirPath, file);  // Full path to the file or folder
    const stats = fs.statSync(fullPath); // Get file stats (whether it's a directory or a file)

    if (stats.isDirectory()) {
      structure += `- **${file}**/\n`;  // Add directory with a slash
      structure += getFolderStructure(fullPath); // Recursively add subfolders
    } else {
      structure += `  - ${file}\n`;  // Add file (without a slash)
    }
  });

  return structure;
}

// Directory to start with (current directory in this case)
const dirPath = __dirname;  // This refers to the current directory

// Get the folder structure starting from the current directory
const folderStructure = getFolderStructure(dirPath);

// for folder structure
// ${folderStructure}

// Full content for the README.md file
const readmeContent = `
# Wind Turbine Data Pipeline


## Description

You are a data engineer for a renewable energy company that operates a farm of wind turbines. The turbines generate power based on wind speed and direction, and their output is measured in megawatts (MW). Your task is to build a data processing pipeline that ingests raw data from the turbines and performs the following operations:

- Cleans the data: The raw data contains missing values and outliers, which must be removed or imputed.
- Calculates summary statistics: For each turbine, calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
-	Identifies anomalies: Identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
-	Stores the processed data: Store the cleaned data and summary statistics in a database for further analysis.


### **Key Operations:**
---
1. **Data Ingestion**: Reads raw CSV files, loads data into a database, and tracks ingestion details.
2. **Data Cleaning**: Removes outliers / anomalies and impute missing values.
3. **Anomaly Detection**: Identifies turbines deviating significantly from expected power output.
4. **Summary Statistics**: Computes min, max, and average power output per turbine per day.
5. **Database Storage**: Saves cleaned data and summary statistics for further analysis.

---

## Solution Design
- The solution is built using **Python (Pandas, MySQL) and SQL** in **VS Code**. 
- The pipeline ensures scalability and efficiency by optimizing data ingestion and transformation.


## Installation

To install this project, follow these steps:

1. Clone this repository:
   \`\`\`
   git clone https://github.com/MilindKeer/Colibri_Digital
   \`\`\`

2. Navigate into the project directory e.g.:
   \`\`\`
   cd WIND_TURBINE
   \`\`\`

3. Install the required dependencies:

    Run the following command to install required packages:
    \`\`\`
      bash
      pip install -r requirements.txt
    \`\`\`
    
    OR use the provided script:

    \`\`\`
    bash
    python install_packages.py
    \`\`\`
  
    Also, Make sure you have MySQL installed, 
    Open src/config file from the project and update MySQL connection details
    
    e.g.
    \`\`\`
    Database Credentials
    DB_HOST = "localhost"
    DB_PORT = 3306
    DB_USER = "milind"  # Replace with your MySQL username
    DB_PASSWORD = "password@123"  # Replace with your MySQL password
    DB_NAME = "wind_turbine_db"
    \`\`\`

    Source files: source CSV files should be stored at
    \`\`\`
    /data/raw_data/
    
    \`\`\`

## Usage

Ensure that you have the necessary environment variables set up (if any e.g. DB User ID n Password) 
before running the project.

To run the data pipeline, go to the project path and run the following command:
\`\`\`
Python data_pipeline/wind_turbine_data_pipeline.py
\`\`\`

## Folder Structure

The folder structure of this project is as follows:

\`\`\`
- data/
- archive/
  - 20250211_231812_data_group_1.csv
  - 20250211_231817_data_group_2.csv
  - 20250211_231823_data_group_3.csv
  - 20250211_232308_data_group_1.csv
- raw_data/
- data_pipeline/
  - wind_turbine_data_pipeline.py  
- logs/
  - script_2025-02-11 T 23-18-01.log
  - script_2025-02-11 T 23-23-03.log
  - script_2025-02-12 T 09-35-41.log
- mock_data/
- raw/
  - mock_file.csv
  - mock_file1.csv   
- src/
  - calculate_summary_stats.py
  - clean_data.py
  - config.py
  - ingest_data.py
  - install_packages.py
  - setup_database.py  
- unit_test/
  - test_wind_turbone.py
- generate-folder-structure.js
- READMe.md
- requirements.txt
\`\`\`

## **Detailed Workflow**

### **Data Ingestion (\`ingest_data.py\`)**  
- Reads all CSV files from \`data/raw/\` and loads them into the **Raw Data Table (\`wind_turbine_raw_data\`)**.  
- The **Raw Data Table** acts as the **source of truth**, storing data exactly as it appears in the CSV files without modifications.  
- Tracks ingestion using an **Ingestion Tracker Table (\`wind_turbine_ingestion_tracker\`)**:  
  - Captures the **last processed timestamp** and **row number per file**.  
  - Ensures **only new data** is read in subsequent runs, significantly improving **scalability**.  
    - This approach reduces **redundant processing**, ensuring that as data volume grows, **only incremental records** are ingested, minimizing storage and computational overhead.  
    - Enables **efficient handling of large datasets**, as older records are not reprocessed, optimizing resource utilization.  
- Moves processed CSVs to \`data/archive/\` with a timestamped filename (e.g., \`20250211_231812_data_group_1.csv\`).  
- Designed to **scale efficiently** as more turbines and larger datasets are introduced.  

### **Data Cleaning (\`clean_data.py\`)**
- Identifies and removes **outliers/anomalies** based on mean & standard deviation (i.e turbines whose output is outside of 2 standard deviations from the mean).
- Stores anomalies separately in an **Anomalies Table (\`wind_turbine_anomalies\`)**.
- Computes **mean, median, and mode** for \`wind_speed\`, \`wind_direction\`, and \`power_output\` over multiple periods:
  - Full dataset
  - Last 4 weeks 
  - 2 weeks 
  - 1 week
  - 1 day
- Stores above in the **Mean Median Mode table (\`wind_turbine_mean_median_mode_stats\`) **  
- Uses these statistics to **impute missing values** in the cleaned dataset (currently handles only missing values but logic can be extended to handle for other invalid data e.g. negative values).
- Ensures the **Clean Data Table** is free of missing values and anomolies are removed.
- Updates **Clean Data Table (\`wind_turbine_clean_data\`) **

### **Summary Statistics (\`calculate_summary_stats.py\`)**
- Computes **minimum, maximum, and average power output per turbine per day** and stores in **stats table (\`wind_turbine_summary_stats\`)**.
- Generates a **daily anomaly count per turbine** and store in a **stats table (\`wind_turbine_anomalies_summary_stats\`)**

---

## **Database Schema Overview**

### **Raw Data Table (\`wind_turbine_raw_data\`)**
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| turbine_id | INT | Unique ID for each turbine |
| timestamp | DATETIME | Data record timestamp |
| wind_speed | FLOAT | Wind speed measurement |
| wind_direction | FLOAT | Wind direction measurement |
| power_output | FLOAT | Power output in MW |

**Unique Key**: The combination of \`turbine_id\` + \`timestamp\` serve as unique identifiers for each data record.


### **Ingestion Tracker Table (\`wind_turbine_ingestion_tracker\`)**
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| file_name | VARCHAR | CSV file name |
| last_record_timestamp | DATETIME | Last processed timestamp |
| last_record_csv_row_number | INT | Last processed row number |
| data_insertion_date | DATETIME | data insertion date |


### **Clean Data Table (\`wind_turbine_clean_data\`)**
Same as **Raw Data Table**, but with **missing values imputed** and **anomalies removed**.

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| turbine_id | INT | Unique ID for each turbine |
| timestamp | DATETIME | Data record timestamp |
| wind_speed | FLOAT | Wind speed measurement |
| wind_direction | FLOAT | Wind direction measurement |
| power_output | FLOAT | Power output in MW |

**Unique Key**: The combination of \`turbine_id\` + \`timestamp\` serve as unique identifiers for each data record.


### **Anomalies Table (\`wind_turbine_anomalies\`)**
Same as **Raw Data Table**, but contains only identified **anomalies**.

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| turbine_id | INT | Unique ID for each turbine |
| timestamp | DATETIME | Data record timestamp |
| wind_speed | FLOAT | Wind speed measurement |
| wind_direction | FLOAT | Wind direction measurement |
| power_output | FLOAT | Power output in MW |

**Unique Key**: The combination of \`turbine_id\` + \`timestamp\` serve as unique identifiers for each data record.

### **Mean, Median, Mode Table (\`wind_turbine_mean_median_mode_stats\`)**
| Column | Type | Description |
|--------|------|-------------|
| id | INT (AUTO_INCREMENT) | Primary key |
| period | VARCHAR(50) | Time period (e.g., last 2 weeks, full dataset) |
| wind_speed_mean | FLOAT | Mean wind speed |
| wind_speed_median | FLOAT | Median wind speed |
| wind_speed_mode | FLOAT | Mode wind speed |
| wind_direction_mean | FLOAT | Mean wind direction |
| wind_direction_median | FLOAT | Median wind direction |
| wind_direction_mode | FLOAT | Mode wind direction |
| power_output_mean | FLOAT | Mean power output |
| power_output_median | FLOAT | Median power output |
| power_output_mode | FLOAT | Mode power output |
| calculation_timestamp | TIMESTAMP | Record insertion time (default: current timestamp) |


### **Summary Table (\`wind_turbine_summary_stats\`)**
| Column          | Type     | Description                              |
|----------------|---------|------------------------------------------|
| day           | DATE     | Summary date (part of unique key)       |
| turbine_id    | INT      | Unique ID for each turbine (part of unique key) |
| min_power_output | FLOAT   | Minimum power output                     |
| max_power_output | FLOAT   | Maximum power output                     |
| avg_power_output | FLOAT   | Average power output                     |
| insertion_date | DATETIME | Timestamp when the record was inserted (default: \`CURRENT_TIMESTAMP\`) |

**Unique Key**: The combination of \`day\` and \`turbine_id\` ensures that each turbine has only one summary record per day.

### **Anomalies Summary Table (\`wind_turbine_anomalies_summary\`)**
| Column      | Type  | Description |
|------------|------|-------------|
| day        | DATE  | Summary date (part of unique key) |
| turbine_id_1 | INT  | Number of anomalies for Turbine 1 |
| turbine_id_2 | INT  | Number of anomalies for Turbine 2 |
| ...        | ...  | ... |
| turbine_id_n | INT  | Number of anomalies for Turbine N |

## **Testing & Validation**
### **Unit Tests (\`tests/\`)**
- **\`test_wind_turbone.py\`** – Unit Test Script

Run tests using:
\`\`\`bash
pytest unite_test/test_wind_turbone.py
\`\`\`

---

## **Configuration (\`config.py\`)**
This file defines **global variables** such as:
- Database Credentials
- Source Data CSV Prefix
- Table Names
- Folder Names
- File paths
- Period for stats
- Logging configuration

---

## **Key Assumptions**
- Each turbine's data is always in the same CSV (e.g., \`data_group_1.csv\`). If this changes, the code needs to handle it appropriately.
- All CSV files are expected to have the same last timestamp. If timestamps vary across files, the ingestion logic must account for this.
- CSVs contain historical data; **only new rows are ingested**, ensuring efficient scaling as data volume grows.
- The **Raw Data Table is the source of truth**, and data is loaded exactly as it appears in the CSV files. **Historical data will never change**.
- Anomalies are defined as **values outside ±2 standard deviations**.
- Missing values are imputed using **median values from a configurable period**.
- The pipeline runs **daily**, processing only new data to reduce computational load and improve scalability.

---

## **Contact**
For any questions, reach out at [technoacelimited@gmail.com](mailto:technoacelimited@gmail.com)

---

> **Author**: Milind Keer  
> **Version**: 1.0  

`;


// Write the README content to the file
fs.writeFileSync('README.md', readmeContent, 'utf8');

console.log('README.md file has been generated!');
