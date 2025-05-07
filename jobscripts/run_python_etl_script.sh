#!/bin/bash
# Description: Wrapper script to activate virtual environment and run Python ETL scripts
set -e

# Define paths
ROOT_DIR="/home/yostfundsadmintest1/client_etl_workflow"
VENV_DIR="$ROOT_DIR/venv"
JOB_SCRIPTS_DIR="$ROOT_DIR/jobscripts"
SYSTEM_SCRIPTS_DIR="$ROOT_DIR/systemscripts"
LOG_FILE="$ROOT_DIR/logs/run_python_etl_script.log"

# Ensure script name is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <script_name> [args...]"
    echo "Example: $0 meetmax_url_check.py"
    exit 1
fi

SCRIPT_NAME="$1"
shift # Remove script name from arguments, leaving any additional args

# Check if script exists in jobscripts or systemscripts
if [ -f "$JOB_SCRIPTS_DIR/$SCRIPT_NAME" ]; then
    SCRIPT_PATH="$JOB_SCRIPTS_DIR/$SCRIPT_NAME"
elif [ -f "$SYSTEM_SCRIPTS_DIR/$SCRIPT_NAME" ]; then
    SCRIPT_PATH="$SYSTEM_SCRIPTS_DIR/$SCRIPT_NAME"
else
    echo "Error: Script '$SCRIPT_NAME' not found in $JOB_SCRIPTS_DIR or $SYSTEM_SCRIPTS_DIR"
    exit 1
fi

# Redirect output to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Running Python script $SCRIPT_NAME at $(date) ==="

# Activate virtual environment
source "$VENV_DIR/bin/activate"
echo "PATH after activation: $PATH"  # Debug line

# Run the script with any additional arguments
python "$SCRIPT_PATH" "$@"

# Deactivate virtual environment
deactivate

echo "=== Python script $SCRIPT_NAME completed at $(date) ==="