#!/bin/bash
# Description: Executes specific SQL scripts in the onboarding directory against the Feeds database in a defined order.
set -e  # Exit immediately if a command exits with a non-zero status

# Define paths
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"
PROJECT_DIR="$HOME_DIR/client_etl_workflow"
ONBOARDING_DIR="$PROJECT_DIR/onboarding"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/sql_setupscripts_$(date +%Y%m%dT%H%M%S).log"

# Database connection details
DB_NAME="Feeds"
DB_USER="yostfundsadmin"
DB_HOST="localhost"
DB_PORT="5432"

# List of SQL scripts in execution order
SQL_SCRIPTS=(
    "dataset_setup.sql"
    "setup_dba_maintenance.sql"
    "maintenance_procedures.sql"
    "log_cleanup.sql"
    "table_index_monitoring.sql"
    "monitor_long_running_queries.sql"
)

# Ensure log directory exists
mkdir -p "$LOG_DIR"
chmod 2770 "$LOG_DIR"
chown "$CURRENT_USER":etl_group "$LOG_DIR"

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== SQL Setup Scripts Execution Started at $(date) by $CURRENT_USER ==="

# Check if onboarding directory exists
if [ ! -d "$ONBOARDING_DIR" ]; then
    echo "[ERROR] $(date): Onboarding directory $ONBOARDING_DIR does not exist."
    exit 1
fi

# Export PGPASSWORD for psql to avoid password prompt (securely handle in production)
export PGPASSWORD="etlserver2025!"

# Execute each SQL script in the specified order
for SCRIPT in "${SQL_SCRIPTS[@]}"; do
    SQL_FILE="$ONBOARDING_DIR/$SCRIPT"
    # Check if the SQL file exists
    if [ ! -f "$SQL_FILE" ]; then
        echo "[ERROR] $(date): SQL file $SQL_FILE does not exist."
        exit 1
    fi

    echo "[INFO] $(date): Executing $SQL_FILE..."
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SQL_FILE"; then
        echo "[SUCCESS] $(date): Successfully executed $SQL_FILE."
    else
        echo "[ERROR] $(date): Failed to execute $SQL_FILE. Check logs for details."
        exit 1
    fi
done

# Unset PGPASSWORD for security
unset PGPASSWORD

# Set permissions for log file
chown "$CURRENT_USER":etl_group "$LOG_FILE"
chmod 660 "$LOG_FILE"

echo "=== SQL Setup Scripts Execution Completed at $(date) ==="
echo "Check $LOG_FILE for details."