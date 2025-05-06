#!/bin/bash
# Description: Orchestrates ETL pipeline tasks.
set -e  # Exit immediately if a command exits with a non-zero status

# Source the virtual environment
source /home/yostfundsadmin/etl_workflow/venv/bin/activate

# Run ETL scripts
echo "Starting ETL pipeline at $(date)"
python3 /home/yostfundsadmin/etl_workflow/scripts/scrape.py || { echo "[ERROR] Scraping failed"; exit 1; }
python3 /home/yostfundsadmin/etl_workflow/scripts/convert_xlsx.py || { echo "[ERROR] XLSX conversion failed"; exit 1; }
python3 /home/yostfundsadmin/etl_workflow/scripts/load_to_db.py || { echo "[ERROR] Database loading failed"; exit 1; }
python3 /home/yostfundsadmin/etl_workflow/scripts/send_email.py || { echo "[ERROR] Email reporting failed"; exit 1; }
echo "ETL pipeline completed at $(date)"