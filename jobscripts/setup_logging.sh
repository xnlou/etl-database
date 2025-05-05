#!/bin/bash
# Description: Sets up logging for ETL workflows on Linux Mint Desktop.
set -e  # Exit immediately if a command exits with a non-zero status
set -x  # Enable debug mode

# Global variables
SESSION_UUID=$(uuidgen) || { echo "[ERROR] Failed to generate UUID"; exit 1; }
LOG_COUNTER=0
START_TIME=$(date +%s) || { echo "[ERROR] Failed to get START_TIME"; exit 1; }
LAST_LOG_TIME=$START_TIME
DB_CONN=""
PROCESS_TYPE="default"

# Function to setup logging
setup_logging() {
    local log_file_path="$1"
    local db_params="$2"  # Format: "host=localhost port=5432 user=postgres password=etlserver2025! dbname=etl_db"

    # Ensure the directory exists
    echo "Creating log directory: $(dirname "$log_file_path")"
    if ! mkdir -p "$(dirname "$log_file_path")"; then
        echo "[ERROR] Failed to create log directory: $(dirname "$log_file_path")"
        exit 1
    fi

    # Verify directory permissions
    if ! chown yostfundsadmintest:etl_group "$(dirname "$log_file_path")"; then
        echo "[ERROR] Failed to set ownership of log directory"
        exit 1
    fi
    if ! chmod 2770 "$(dirname "$log_file_path")"; then
        echo "[ERROR] Failed to set permissions on log directory"
        exit 1
    fi

    # Reset counters and times
    LOG_COUNTER=0
    START_TIME=$(date +%s) || { echo "[ERROR] Failed to reset START_TIME"; exit 1; }
    LAST_LOG_TIME=$START_TIME

    # Set up database connection if provided
    if [ -n "$db_params" ]; then
        DB_CONN="$db_params"
        # Drop and recreate the table to avoid sequence issues
        echo "Dropping logs table if it exists"
        psql "$DB_CONN" -c "DROP TABLE IF EXISTS logs;" 2>/dev/null || echo "[WARNING] Failed to drop logs table"

        # Create table
        echo "Creating logs table in database"
        if ! psql "$DB_CONN" -c "CREATE TABLE logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            counter INTEGER,
            uuid TEXT,
            process_type TEXT,
            step_runtime REAL,
            total_runtime REAL,
            message TEXT,
            \"user\" TEXT);" 2>/dev/null; then
            echo "[ERROR] Failed to create log table"
            exit 1
        fi
        echo "Log table created."
    fi

    echo "Logging setup complete. UUID: $SESSION_UUID"
}

# Function to log messages
log_etl_event() {
    local message="$1"
    local process_type="${2:-$PROCESS_TYPE}"
    local log_to_db="${3:-false}"

    echo "Logging event: $message"

    ((LOG_COUNTER++)) || { echo "[ERROR] Failed to increment LOG_COUNTER"; exit 1; }
    local CURRENT_TIME
    if ! CURRENT_TIME=$(date +%s); then
        echo "[ERROR] Failed to get CURRENT_TIME"
        exit 1
    fi
    local TOTAL_RUNTIME
    local STEP_RUNTIME

    # Calculate runtimes with error handling
    if ! TOTAL_RUNTIME=$(echo "scale=2; ($CURRENT_TIME - $START_TIME) / 60" | bc 2>/dev/null); then
        echo "[ERROR] Failed to calculate TOTAL_RUNTIME"
        exit 1
    fi
    if ! STEP_RUNTIME=$(echo "scale=2; ($CURRENT_TIME - $LAST_LOG_TIME) / 60" | bc 2>/dev/null); then
        echo "[ERROR] Failed to calculate STEP_RUNTIME"
        exit 1
    fi

    local CURRENT_USER
    if ! CURRENT_USER=$(whoami); then
        echo "[ERROR] Failed to get CURRENT_USER"
        exit 1
    fi

    # Format the log message
    local FORMATTED_MESSAGE="Counter: $LOG_COUNTER - UUID: $SESSION_UUID - Process Type: $process_type - Step Runtime: $STEP_RUNTIME min - Total Runtime: $TOTAL_RUNTIME min - $message - User: $CURRENT_USER"

    # Log to file with error handling
    local DATE_STAMP
    if ! DATE_STAMP=$(date '+%Y-%m-%d %H:%M:%S'); then
        echo "[ERROR] Failed to get date stamp"
        exit 1
    fi
    if ! echo "$DATE_STAMP - $FORMATTED_MESSAGE" >> "$log_file_path"; then
        echo "[ERROR] Failed to write to log file: $log_file_path"
        exit 1
    fi

    # Log to database if specified, with error handling
    if [ "$log_to_db" = true ] && [ -n "$DB_CONN" ]; then
        echo "Logging to database: $message"
        # Omit id column to let SERIAL auto-increment
        if ! psql "$DB_CONN" -c "INSERT INTO logs (timestamp, counter, uuid, process_type, step_runtime, total_runtime, message, \"user\") VALUES (
            current_timestamp,
            $LOG_COUNTER,
            '$SESSION_UUID',
            '$process_type',
            $STEP_RUNTIME,
            $TOTAL_RUNTIME,
            '$message',
            '$CURRENT_USER');" 2>/dev/null; then
            echo "[WARNING] Failed to log to database (duplicate key or other error)"
        fi
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
echo "Creating database etl_db"
sudo -u postgres psql -c "CREATE DATABASE etl_db;" || echo "Database etl_db already exists."

setup_logging "$log_file_path" "$db_params"
log_etl_event "Logging system initialized" "SETUP" true
log_etl_event "ETL environment setup completed"
close_log