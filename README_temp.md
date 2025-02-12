# Wind Turbine Data Pipeline

## **Project Overview**
This project is a **scalable and testable data pipeline** for a renewable energy company operating wind turbines. The pipeline processes daily CSV data from turbines, cleans the data, calculates summary statistics, identifies anomalies, and stores processed data in a database for further analysis.

### **Key Operations:**
1. **Data Ingestion**: Reads raw CSV files, loads data into a database, and tracks ingestion details.
2. **Data Cleaning**: Removes outliers / anomalies and impute missing values.
3. **Anomaly Detection**: Identifies turbines deviating significantly from expected power output.
4. **Summary Statistics**: Computes min, max, and average power output per turbine per day.
5. **Database Storage**: Saves cleaned data and summary statistics for further analysis.

---

## Solution Design
The solution is built using **Python (Pandas, MySQL) and SQL** in **VS Code**. The pipeline ensures scalability and efficiency by optimizing data ingestion and transformation.

### **Project Structure**
```
├── data/                      # Data directory
│   ├── raw/                   # Raw CSV files i.e. source files
│   ├── archive/               # Processed CSVs (archived with timestamp)
├── src/                       # Python scripts
│   ├── config.py              # Configuration file (database, logging, global variables)
│   ├── setup_database.py      # Creates database and tables (pre-requisite script)
│   ├── ingest_data.py         # Reads and inserts raw CSV data into the database
│   ├── clean_data.py          # Cleans data, removes outliers, imputes missing values
│   ├── calculate_summary_stats.py # Computes daily summary statistics
│   ├── install_packages.py    # Installs required dependencies
├── 
├── data_pipeline/             # Pipeline orchestration
│   ├── wind_turbine_data_pipeline.py  # Orchestrates all steps
├── requirements.txt           # List of dependencies
├── README.md                  # Project documentation
```

---

## ⚙️ **Installation & Setup**

### **1️⃣ Prerequisites**
- Python 3.8+
- MySQL Database
- Required Python libraries (Pandas, MySQL Connector, etc.)

### **2️⃣ Install Dependencies**
Run the following command to install required packages:
```bash
pip install -r requirements.txt
```
OR use the provided script:
```bash
python install_packages.py
```

### **3️⃣ Database Setup**
Before running the pipeline, set up the MySQL database and tables:
```bash
python setup_database.py
```
If this step fails, the pipeline will not proceed.

### **4️⃣ Running the Pipeline**
Once the setup is complete, execute the main pipeline script:
```bash
python pipeline/wind_turbine_data_pipeline.py
```
This script orchestrates the entire workflow, including ingestion, cleaning, and summary computation.

---

## 🚀 **Detailed Workflow**

### 1️⃣ **Data Ingestion (`ingest_data.py`)**
- Reads all CSV files from `data/raw/` and loads them into the **Raw Data Table**.
- Tracks ingestion using an **Ingestion Tracker Table**:
  - Captures last processed timestamp and row number per file.
  - Ensures only new data is read in subsequent runs.
- Moves processed CSVs to `data/archive/` with a timestamped filename.

### 2️⃣ **Data Cleaning (`clean_data.py`)**
- Identifies and removes **outliers/anomalies** based on mean & standard deviation.
- Stores anomalies separately in an **Anomalies Table**.
- Computes **mean, median, and mode** for `wind_speed`, `wind_direction`, and `power_output` over multiple periods:
  - Full dataset
  - Last 4 weeks, 2 weeks, 1 week, and 1 day
- Uses these statistics to **impute missing values** in the cleaned dataset.
- Ensures the **Clean Data Table** is free of missing values.

### 3️⃣ **Summary Statistics (`calculate_summary_stats.py`)**
- Computes **minimum, maximum, and average power output per turbine per day**.
- Generates a **daily anomaly count per turbine**.
- Saves results into the **Summary Table** for further analysis.

---

## 📊 **Database Schema Overview**

### **Raw Data Table (`wind_turbine_raw_data`)**
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| turbine_id | INT | Unique ID for each turbine |
| timestamp | DATETIME | Data record timestamp |
| wind_speed | FLOAT | Wind speed measurement |
| wind_direction | FLOAT | Wind direction measurement |
| power_output | FLOAT | Power output in MW |

### **Clean Data Table (`wind_turbine_clean_data`)**
Same as **Raw Data Table**, but with **missing values imputed** and **anomalies removed**.

### **Anomalies Table (`wind_turbine_anomalies`)**
Same as **Raw Data Table**, but contains only identified **anomalies**.

### **Summary Table (`wind_turbine_summary_stats`)**
| Column | Type | Description |
|--------|------|-------------|
| turbine_id | INT | Unique ID for each turbine |
| date | DATE | Summary date |
| min_power | FLOAT | Minimum power output |
| max_power | FLOAT | Maximum power output |
| avg_power | FLOAT | Average power output |
| anomaly_count | INT | Number of anomalies detected |

### **Ingestion Tracker Table (`wind_turbine_ingestion_tracker`)**
| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| file_name | VARCHAR | CSV file name |
| last_timestamp | DATETIME | Last processed timestamp |
| last_csv_row_number | INT | Last processed row number |
| number_of_records | INT | Total records processed |

---

## 🧪 **Testing & Validation**
### **Unit Tests (`tests/`)**
- **`test_ingest_data.py`** – Tests CSV ingestion logic.
- **`test_clean_data.py`** – Tests data cleaning and anomaly detection.
- **`test_summary_stats.py`** – Tests summary statistics calculations.

Run tests using:
```bash
pytest tests/
```

---

## 🔧 **Configuration (`config.py`)**
This file defines **global variables** such as:
- Database connection settings
- File paths
- Logging configuration
- Anomaly detection thresholds
- Data imputation settings

---

## 📌 **Key Assumptions**
- Each turbine's data is always in the same CSV (e.g., `data_group_1.csv`).
- CSVs contain historical data; **only new rows are ingested**.
- Anomalies are defined as **values outside ±2 standard deviations**.
- Missing values are imputed using **median values from a configurable period**.
- Pipeline runs **daily**.

---

## 📜 **Future Enhancements**
✅ Automate pipeline execution using **Apache Airflow** or **Prefect**.
✅ Implement a **dashboard** (Power BI/Tableau) for real-time monitoring.
✅ Use **cloud storage (AWS S3/Azure Blob)** for better scalability.

---

## 🤝 **Contributing**
Feel free to contribute by opening issues or submitting pull requests.
For major changes, please open an issue first to discuss what you would like to change.

---

## 📧 **Contact**
For any questions, reach out at [your.email@example.com](mailto:your.email@example.com)

---

> **Author**: [Your Name]  
> **Company**: [Renewable Energy Co.]  
> **Version**: 1.0  
> **Last Updated**: [YYYY-MM-DD]



## Project Folder Structure

- **.git**/
  - COMMIT_EDITMSG
  - config
  - description
  - HEAD
- **hooks**/
  - applypatch-msg.sample
  - commit-msg.sample
  - fsmonitor-watchman.sample
  - post-update.sample
  - pre-applypatch.sample
  - pre-commit.sample
  - pre-merge-commit.sample
  - pre-push.sample
  - pre-rebase.sample
  - pre-receive.sample
  - prepare-commit-msg.sample
  - push-to-checkout.sample
  - update.sample
  - index
- **info**/
  - exclude
- **logs**/
  - HEAD
- **refs**/
- **heads**/
  - main
- **objects**/
- **64**/
  - 52625ded92a8d9c06926cba61d0fc5b8387b2e
- **91**/
  - b1c992a7159b93d0cedcd39d914c11f1c1fb00
- **9d**/
  - 4c4e7b987ae69a050b770bde7f4c8baf21bdd9
- **df**/
  - e0770424b2a19faf507a501ebfc23be8f54e7b
- **info**/
- **pack**/
- **refs**/
- **heads**/
  - main
- **tags**/
  - .gitattributes
- **.pytest_cache**/
  - .gitignore
  - CACHEDIR.TAG
  - README.md
- **v**/
- **cache**/
  - lastfailed
  - nodeids
  - stepwise
- **data**/
- **archive**/
  - 20250211_231812_data_group_1.csv
  - 20250211_231817_data_group_2.csv
  - 20250211_231823_data_group_3.csv
  - 20250211_232308_data_group_1.csv
- **raw_data**/
- **data_pipeline**/
  - wind_turbine_data_pipeline.py
  - generate-folder-structure.js
- **logs**/
  - script_2025-02-11 T 23-18-01.log
  - script_2025-02-11 T 23-23-03.log
  - script_2025-02-12 T 09-35-41.log
  - script_2025-02-12 T 09-36-16.log
  - script_2025-02-12 T 09-54-13.log
  - script_2025-02-12 T 10-01-08.log
  - script_2025-02-12 T 10-02-19.log
  - script_2025-02-12 T 10-02-36.log
  - script_2025-02-12 T 10-13-25.log
  - script_2025-02-12 T 10-14-09.log
- **mock_data**/
- **raw**/
  - mock_file.csv
  - mock_file1.csv
  - READMe.md
  - README_temp.md
  - requirements.txt
  - requirements_all.txt
- **src**/
  - calculate_summary_stats.py
  - clean_data.py
  - config.py
  - ingest_data.py
  - install_packages.py
  - setup_database.py
  - test.py
  - zz_clean_data copy.py
- **__pycache__**/
  - calculate_summary_stats.cpython-312.pyc
  - clean_data.cpython-312.pyc
  - config.cpython-312.pyc
  - ingest_data.cpython-312.pyc
  - setup_database.cpython-312.pyc
- **temp**/
  - data_group_1.csv
  - data_group_2.csv
  - data_group_3.csv
- **unit_test**/
  - test_wind_turbone.py
- **__pycache__**/
  - ingest_data_unit_test.cpython-312-pytest-8.3.4.pyc
  - test_database_setup.cpython-312-pytest-8.3.4.pyc
  - test_ingest_data.cpython-312-pytest-8.3.4.pyc
  - test_wind_turbone.cpython-312-pytest-8.3.4.pyc
