""" 
    Note:
    Please make sure MySQL installed and connection credentials are updated in the Config.py script
    I have purposly not set environment variables.

    Also, please make sure all the packages listed in the requirements.txt are installed.

    if not, run below command before running this pipeline (one off task)

    pip install -r requirements.txt

    Or

    run "install_packages.py" script   
"""

import sys
import logging
import os

# Get the parent directory (for src)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# Add 'src' to sys.path
sys.path.insert(0, os.path.join(ROOT_DIR, "src"))

from setup_database import main as setup_database
from ingest_data import main as ingest_data
from clean_data import main as clean_data
from calculate_summary_stats import main as calculate_summary_stats
import config as conf

def run_step(step_func, step_name):
    """Run a pipeline step and handle errors."""
    try:
        logging.info(f"Starting: {step_name}")
        status = step_func()
        
        if status is None:
            logging.error(f"Failed: {step_name} returned None")
            return False 
        
        logging.info(f"Completed: {step_name}\n")
        return True
    except Exception as e:
        logging.error(f"Failed: {step_name} - {e}")
        return False
    
def main():
    logging.info("****Starting Wind Turbine Data Pipeline...****\n")

    if not run_step(setup_database, "Database Setup"):
        return  # Stop execution if setup fails

    if not run_step(ingest_data, "Data Ingestion"):
        return  # Stop execution if ingestion fails

    if not run_step(clean_data, "Data Cleaning"):
        return  # Stop execution if cleaning fails

    if not run_step(calculate_summary_stats, "Summary Statistics Calculation"):
        return  # Stop execution if summary stats fail

    logging.info("****Wind Turbine Data Pipeline ran successfully...****")

if __name__ == "__main__":
    main()
