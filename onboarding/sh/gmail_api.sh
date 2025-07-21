#!/bin/bash
# Script: install_gmail_api_deps.sh (save in SYSTEM_SCRIPTS_DIR)
set -e

ROOT_DIR="$HOME/client_etl_workflow"
VENV_DIR="$ROOT_DIR/venv"
LOG_FILE="$ROOT_DIR/logs/install_gmail_api_deps.log"

exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Installing Gmail API dependencies at $(date) ==="

# Activate venv
source "$VENV_DIR/bin/activate"

# Install libraries
pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2

# Deactivate
deactivate

echo "=== Installation complete. Check $LOG_FILE for issues. ==="