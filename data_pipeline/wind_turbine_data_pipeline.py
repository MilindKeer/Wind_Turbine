""" 
    Please make sure all the packages listed in the requirements.txt are installed.

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
        step_func()
        logging.info(f"Completed: {step_name}")
    except Exception as e:
        logging.error(f"Failed: {step_name} - {e}")
        sys.exit(1)  # Stop pipeline execution on failure

def main():
    logging.info("Starting Wind Turbine Data Pipeline...")

    run_step(setup_database, "Database Setup")
    run_step(ingest_data, "Data Ingestion")
    run_step(clean_data, "Data Cleaning")
    run_step(calculate_summary_stats, "Summary Statistics Calculation")

    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
