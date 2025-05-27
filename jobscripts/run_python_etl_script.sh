#!/bin/bash
# Description: Wrapper script to run Python ETL scripts in the virtual environment.
set -e  # Exit immediately if a command exits with a non-zero status

# Project directory and virtual environment (hardcoded to ensure consistency)
PROJECT_DIR="/home/yostfundsadmin/client_etl_workflow"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON="$VENV_DIR/bin/python"
ACTIVATE_VENV="source $VENV_DIR/bin/activate"
ENV_FILE="$PROJECT_DIR/env.sh"

# Log file for script execution
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/run_python_etl_script.log"
TEMP_LOG="/tmp/run_python_etl_script_$(date +%Y%m%dT%H%M%S).log"

# Ensure log directory exists
if ! mkdir -p "$LOG_DIR" 2>>"$TEMP_LOG"; then
    echo "Error: Failed to create log directory $LOG_DIR" >>"$TEMP_LOG" 2>&1
    exit 1
fi

# Ensure log file exists
if ! touch "$LOG_FILE" 2>>"$TEMP_LOG"; then
    echo "Error: Cannot create or access log file $LOG_FILE, using $TEMP_LOG" >>"$TEMP_LOG" 2>&1
    LOG_FILE="$TEMP_LOG"
fi

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Running Python script at $(date) ==="

# Log shell and environment
echo "Shell: $SHELL"
echo "PATH: $PATH"

# Check if script name is provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <script_name> [args...]" >&2
    exit 1
fi

SCRIPT_NAME="$1"
shift  # Remove script name from arguments
SCRIPT_ARGS="$@"

# Log user and group information
echo "User: $(whoami)"
echo "Groups: $(groups)"

# Check access to key files and directories
for path in "$PROJECT_DIR" "$VENV_DIR" "$ENV_FILE" "$LOG_DIR"; do
    if [ ! -r "$path" ]; then
        echo "Error: No read access to $path for user $(whoami)" >&2
        ls -ld "$path" >>"$LOG_FILE" 2>&1
        exit 1
    fi
done

# Log script permissions
JOB_SCRIPTS_DIR="$PROJECT_DIR/jobscripts"
SYSTEM_SCRIPTS_DIR="$PROJECT_DIR/systemscripts"
if [ -f "$JOB_SCRIPTS_DIR/$SCRIPT_NAME" ]; then
    SCRIPT_PATH="$JOB_SCRIPTS_DIR/$SCRIPT_NAME"
elif [ -f "$SYSTEM_SCRIPTS_DIR/$SCRIPT_NAME" ]; then
    SCRIPT_PATH="$SYSTEM_SCRIPTS_DIR/$SCRIPT_NAME"
else
    echo "Error: Script '$SCRIPT_NAME' not found in $JOB_SCRIPTS_DIR or $SYSTEM_SCRIPTS_DIR" >&2
    exit 1
fi
if [ ! -r "$SCRIPT_PATH" ]; then
    echo "Error: No read access to $SCRIPT_PATH for user $(whoami)" >&2
    ls -l "$SCRIPT_PATH" >>"$LOG_FILE" 2>&1
    exit 1
fi
echo "Permissions of $SCRIPT_PATH: $(ls -l $SCRIPT_PATH)"

# Log Python binary permissions
echo "Permissions of $PYTHON: $(ls -l $PYTHON)"

# Check if Python binary is executable
if [ ! -x "$PYTHON" ]; then
    echo "Error: Python binary $PYTHON not found or not executable" >&2
    exit 1
fi

# Log environment variables
echo "Environment variables before sourcing:"
env | grep -E 'ETL_EMAIL|ETL_EMAIL_PASSWORD|PATH'

# Source the environment file
if [ -f "$ENV_FILE" ]; then
    if ! source "$ENV_FILE"; then
        echo "Error: Failed to source $ENV_FILE" >&2
        exit 1
    fi
    echo "Sourced $ENV_FILE successfully"
else
    echo "Error: Environment file '$ENV_FILE' not found" >&2
    exit 1
fi

# Log environment variables after sourcing
echo "Environment variables after sourcing $ENV_FILE:"
env | grep -E 'ETL_EMAIL|ETL_EMAIL_PASSWORD|PATH'

# Validate SMTP variables for email-related scripts
if [ "$SCRIPT_NAME" = "send_reports.py" ]; then
    if [ -z "$ETL_EMAIL" ] || [ -z "$ETL_EMAIL_PASSWORD" ]; then
        echo "Error: ETL_EMAIL or ETL_EMAIL_PASSWORD not set for $SCRIPT_NAME" >&2
        exit 1
    fi
fi

# Activate the virtual environment
if ! source "$VENV_DIR/bin/activate"; then
    echo "Error: Failed to activate virtual environment $VENV_DIR/bin/activate" >&2
    exit 1
fi

# Determine if the script needs sudo (e.g., update_cron_jobs.py writes to /etc/cron.d/)
if [ "$SCRIPT_NAME" = "update_cron_jobs.py" ]; then
    echo "Running $SCRIPT_NAME with sudo (requires elevated privileges)..."
    if ! sudo -E "$PYTHON" "$SCRIPT_PATH" $SCRIPT_ARGS; then
        echo "Error: Failed to run $SCRIPT_NAME with sudo" >&2
        exit 1
    fi
else
    if ! "$PYTHON" "$SCRIPT_PATH" $SCRIPT_ARGS; then
        echo "Error: Failed to run $SCRIPT_NAME" >&2
        exit 1
    fi
fi

# Deactivate the virtual environment
deactivate

echo "=== Python script $SCRIPT_NAME completed at $(date) ==="