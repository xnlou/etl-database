#!/bin/bash
# Description: Runs meetmax_url_download.py followed by run_import_job.py with config_id=2.
set -e  # Exit immediately if a command exits with a non-zero status

# Define paths
PROJECT_DIR="$HOME/client_etl_workflow"
RUN_SCRIPT="$PROJECT_DIR/jobscripts/run_python_etl_script.sh"
LOG_FILE="$PROJECT_DIR/logs/etl_cron.log"

# Run meetmax_url_download.py
echo "[$(date)] Starting meetmax_url_download.py" >> "$LOG_FILE"
/bin/bash "$RUN_SCRIPT" meetmax_url_download.py >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
    echo "[$(date)] meetmax_url_download.py completed successfully" >> "$LOG_FILE"
else
    echo "[$(date)] meetmax_url_download.py failed" >> "$LOG_FILE"
    exit 1
fi

# Run run_import_job.py with config_id=2
echo "[$(date)] Starting run_import_job.py 2" >> "$LOG_FILE"
/bin/bash "$RUN_SCRIPT" run_import_job.py 2 >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
    echo "[$(date)] run_import_job.py 2 completed successfully" >> "$LOG_FILE"
else
    echo "[$(date)] run_import_job.py 2 failed" >> "$LOG_FILE"
    exit 1
fi

echo "[$(date)] Download and import completed" >> "$LOG_FILE"