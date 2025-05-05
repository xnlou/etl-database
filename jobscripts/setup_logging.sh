#!/bin/bash
# Description: Sets up logging for ETL workflows on Linux Mint Desktop.
set -e  # Exit immediately if a command exits with a non-zero status

# Global variables
SESSION_UUID=$(uuidgen)
LOG_COUNTER=0
START_TIME=$(date +%s)
LAST_LOG_TIME=$START_TIME
DB_CONN=""
PROCESS_TYPE="default"

# Function to setup logging
setup_logging() {
    local log_file_path="$1"
    local db_params="$2"  # Format: "host=localhost port=5432 user=postgres password=etlserver2025! dbname=etl_db"

    # Ensure the directory exists
    mkdir -p "$(dirname "$log_file_path")"

    # Reset counters and times
    LOG_COUNTER=0
    START_TIME=$(date +%s)
    LAST_LOG_TIME=$START_TIME

    # Set up database connection if provided
    if [ -n "$db_params" ]; then
        DB_CONN="$db_params"
        # Create table if it doesn't exist
        psql "$DB_CONN" -c "CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            counter INTEGER,
            uuid TEXT,
            process_type TEXT,
            step_runtime REAL,
            total_runtime REAL,
            message TEXT,
            \"user\" TEXT);" && echo "Log table created or already exists." || { echo "[ERROR] Failed to create log table"; exit 1; }
    fi

    echo "Logging setup complete. UUID: $SESSION_UUID"
}

# Function to log messages
log_etl_event() {
    local message="$1"
    local process_type="${2:-$PROCESS_TYPE}"
    local log_to_db="${3:-false}"

    ((LOG_COUNTER++))
    local CURRENT_TIME=$(date +%s)
    local TOTAL_RUNTIME=$(echo "scale=2; ($CURRENT_TIME - $START_TIME) / 60" | bc)
    local STEP_RUNTIME=$(echo "scale=2; ($CURRENT_TIME - $LAST_LOG_TIME) / 60" | bc)
    local CURRENT_USER=$(whoami)

    # Format the log message
    local FORMATTED_MESSAGE="Counter: $LOG_COUNTER - UUID: $SESSION_UUID - Process Type: $process_type - Step Runtime: $STEP_RUNTIME min - Total Runtime: $TOTAL_RUNTIME min - $message - User: $CURRENT_USER"

    # Log to file
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $FORMATTED_MESSAGE" >> "$log_file_path"

    # Log to database if specified
    if [ "$log_to_db" = true ] && [ -n "$DB_CONN" ]; then
        psql "$DB_CONN" -c "INSERT INTO logs (timestamp, counter, uuid, process_type, step_runtime, total_runtime, message, \"user\") VALUES (
            current_timestamp, 
            $LOG_COUNTER, 
            '$SESSION_UUID', 
            '$process_type', 
            $STEP_RUNTIME, 
            $TOTAL_RUNTIME, 
            '$message', 
            '$CURRENT_USER');" || echo "[ERROR] Failed to log to database"
    fi

    LAST_LOG_TIME=$CURRENT_TIME
}

# Function to close logging
close_log() {
    if [ -n "$DB_CONN" ]; then
        echo "Database logging session closed."
    fi
    echo "Logging session ended."
}

# Example usage
log_file_path="/home/$USER/etl_workflow/logs/etl.log"
db_params="host=localhost port=5432 user=postgres password=etlserver2025! dbname=etl_db"

# Create ETL database if it doesn't exist
sudo -u postgres psql -c "CREATE DATABASE etl_db;" || echo "Database etl_db already exists."

setup_logging "$log_file_path" "$db_params"
log_etl_event "Logging system initialized" "SETUP" true
log_etl_event "ETL environment setup completed"
close_log