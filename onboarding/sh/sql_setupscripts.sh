#!/bin/bash
# Description: Sets up privileged roles and permissions, creates the Feeds database if it doesn't exist, and executes specific SQL scripts in the onboarding directory in a defined order.
# Must be run with sudo or as a user with sufficient privileges to execute commands as the postgres user.
set -e  # Exit immediately if a command exits with a non-zero status

# Define paths
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"
PROJECT_DIR="$HOME_DIR/client_etl_workflow"
ONBOARDING_DIR="$PROJECT_DIR/onboarding/sql"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/sql_setupscripts_$(date +%Y%m%dT%H%M%S).log"

# Database connection details
DB_NAME="Feeds"
DB_USER="yostfundsadmin"
ETL_USER="etl_user"
DB_HOST="localhost"
DB_PORT="5432"

# List of SQL scripts in execution order
SQL_SCRIPTS=(
    "setup_dba_maintenance.sql"
    "dataset_setup.sql"
    "maintenance_procedures.sql"
    "log_cleanup.sql"
    "table_index_monitoring.sql"
    "monitor_long_running_queries.sql"
    "create_importconfig_table.sql"
)

# Ensure log directory exists
mkdir -p "$LOG_DIR"
chmod 2770 "$LOG_DIR"
chown "$CURRENT_USER":etl_group "$LOG_DIR"

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== SQL Setup Scripts Execution Started at $(date) by $CURRENT_USER ==="

# --- Privileged Commands Section (Run as postgres user) ---

# Grant permissions for pg_default tablespace to yostfundsadmin
echo "[INFO] $(date): Granting permissions for pg_default tablespace to $DB_USER..."
if sudo -u postgres psql -c "GRANT ALL ON TABLESPACE pg_default TO $DB_USER;"; then
    echo "[SUCCESS] $(date): Successfully granted pg_default tablespace permissions to $DB_USER."
else
    echo "[ERROR] $(date): Failed to grant pg_default tablespace permissions to $DB_USER."
    exit 1
fi

# Check if etl_user role exists; create it if it doesn't
echo "[INFO] $(date): Checking if role $ETL_USER exists..."
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname = '$ETL_USER'" | grep -q 1; then
    echo "[INFO] $(date): Role $ETL_USER does not exist. Creating it..."
    if sudo -u postgres psql -c "CREATE ROLE $ETL_USER WITH LOGIN;"; then
        echo "[SUCCESS] $(date): Role $ETL_USER created successfully."
    else
        echo "[ERROR] $(date): Failed to create role $ETL_USER."
        exit 1
    fi
    # Set search_path for etl_user
    if sudo -u postgres psql -c "ALTER ROLE $ETL_USER SET search_path TO dba, public;"; then
        echo "[SUCCESS] $(date): Set search_path for $ETL_USER to dba, public."
    else
        echo "[ERROR] $(date): Failed to set search_path for $ETL_USER."
        exit 1
    fi
else
    echo "[INFO] $(date): Role $ETL_USER already exists."
fi

# Grant superuser privileges to yostfundsadmin (needed for event triggers)
echo "[INFO] $(date): Granting superuser privileges to $DB_USER..."
if sudo -u postgres psql -c "ALTER ROLE $DB_USER WITH SUPERUSER;"; then
    echo "[SUCCESS] $(date): Successfully granted superuser privileges to $DB_USER."
else
    echo "[ERROR] $(date): Failed to grant superuser privileges to $DB_USER."
    exit 1
fi

# --- End of Privileged Commands Section ---

# Check if onboarding directory exists
if [ ! -d "$ONBOARDING_DIR" ]; then
    echo "[ERROR] $(date): Onboarding directory $ONBOARDING_DIR does not exist."
    exit 1
fi

# Export PGPASSWORD for psql to avoid password prompt (securely handle in production)
export PGPASSWORD="etlserver2025!"

# Check if the Feeds database exists; if not, create it
echo "[INFO] $(date): Checking if database $DB_NAME exists..."
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1; then
    echo "[INFO] $(date): Database $DB_NAME does not exist. Creating it..."
    if createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$DB_NAME"; then
        echo "[SUCCESS] $(date): Database $DB_NAME created successfully."
    else
        echo "[ERROR] $(date): Failed to create database $DB_NAME."
        exit 1
    fi
else
    echo "[INFO] $(date): Database $DB_NAME already exists."
fi

# Execute each SQL script in the specified order
for SCRIPT in "${SQL_SCRIPTS[@]}"; do
    SQL_FILE="$ONBOARDING_DIR/$SCRIPT"
    # Check if the SQL file exists
    if [ ! -f "$SQL_FILE" ]; then
        echo "[ERROR] $(date): SQL file $SQL_FILE does not exist."
        exit 1
    fi

    echo "[INFO] $(date): Executing $SQL_FILE..."
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" --set ON_ERROR_STOP=on -f "$SQL_FILE"; then
        echo "[SUCCESS] $(date): Successfully executed $SQL_FILE."
    else
        echo "[ERROR] $(date): Failed to execute $SQL_FILE. Check logs for details."
        exit 1
    fi
done

# Unset PGPASSWORD for security
unset PGPASSWORD

# Set permissions for log file (remove chown if unnecessary)
chmod 660 "$LOG_FILE"

echo "=== SQL Setup Scripts Execution Completed at $(date) ==="
echo "Check $LOG_FILE for details."