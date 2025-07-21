#!/bin/bash

# Script to delete files in /home/yostfundsadmin/client_etl_workflow/archive/meetmaxevents older than 7 days
# Logs output to /home/yostfundsadmin/client_etl_workflow/logs/weekly_cleanup_YYYYMMDDTHHMMSS.log

# Define paths
ARCHIVE_DIR="$HOME/client_etl_workflow/archive/meetmaxevents"
LOG_DIR="$HOME/client_etl_workflow/logs"
LOG_FILE="$LOG_DIR/weekly_cleanup_meetmaxevents_$(date '+%Y%m%dT%H%M%S').log"

# Ensure the log directory exists
mkdir -p "$LOG_DIR"

# Log start time
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting weekly cleanup" >> "$LOG_FILE"

# Check if archive directory exists
if [ ! -d "$ARCHIVE_DIR" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Error: Directory $ARCHIVE_DIR does not exist" >> "$LOG_FILE"
    exit 1
fi

# Find and delete files older than 7 days
find "$ARCHIVE_DIR" -type f -mtime +7 -exec rm -v {} \; >> "$LOG_FILE" 2>&1

# Log completion
if [ $? -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Weekly cleanup completed successfully" >> "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Error: Weekly cleanup failed" >> "$LOG_FILE"
    exit 1
fi