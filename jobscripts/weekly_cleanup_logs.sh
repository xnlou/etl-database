#!/bin/bash

# Script to delete log files in /home/yostfundsadmin/client_etl_workflow/logs older than 7 days
# Logs output to /home/yostfundsadmin/client_etl_workflow/logs/weekly_cleanup_logs_YYYYMMDDTHHMMSS.log

# Define paths
LOG_DIR="/home/yostfundsadmin/client_etl_workflow/logs"
LOG_FILE="$LOG_DIR/weekly_cleanup_logs_$(date '+%Y%m%dT%H%M%S').log"

# Ensure the log directory exists
mkdir -p "$LOG_DIR"

# Log start time
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting weekly log cleanup" >> "$LOG_FILE"

# Check if log directory exists
if [ ! -d "$LOG_DIR" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Error: Directory $LOG_DIR does not exist" >> "$LOG_FILE"
    exit 1
fi

# Find and delete log files older than 14 days
find "$LOG_DIR" -type f -mtime +14 -exec rm -v {} \; >> "$LOG_FILE" 2>&1

# Log completion
if [ $? -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Weekly log cleanup completed successfully" >> "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Error: Weekly log cleanup failed" >> "$LOG_FILE"
    exit 1
fi