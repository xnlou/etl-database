#!/bin/bash
# Description: Configures etl_user, etl_group, and sets up the directory structure for ETL workflows.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
PROJECT_DIR="$HOME/client_etl_workflow"
LOG_FILE="$PROJECT_DIR/logs/configure_etl_user.log"

# Function to log errors with timestamp
log_error() {
    echo "[ERROR] $(date): $1" >&2
    exit 1
}

# Function to log system checks
log_system_check() {
    local check_name="$1"
    local check_result="$2"
    echo "[CHECK] $(date): $check_name: $check_result"
}

# Create etl_group if it doesn't exist
echo "Creating etl_group if it doesn't exist..."
if ! getent group etl_group > /dev/null 2>&1; then
    sudo groupadd etl_group && echo "etl_group created." || log_error "Failed to create etl_group"
fi

# Create etl_user with dedicated home directory if it doesn't exist
echo "Creating etl_user if it doesn't exist..."
if ! id -u etl_user > /dev/null 2>&1; then
    sudo useradd -m -d "/home/etl_user" -s /bin/bash -G etl_group etl_user && echo "etl_user created." || log_error "Failed to create etl_user"
fi

# Add current user to etl_group
echo "Adding $CURRENT_USER to etl_group..."
sudo usermod -aG etl_group "$CURRENT_USER" && echo "$CURRENT_USER added to etl_group." || log_error "Failed to add $CURRENT_USER to etl_group"

# Set up project directory
echo "Creating project directory: $PROJECT_DIR..."
mkdir -p "$PROJECT_DIR"
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR"
sudo chmod -R 2770 "$PROJECT_DIR" && echo "Project directory created and permissions set." || log_error "Failed to set up project directory" "$(echo 'Check if etl_group exists and user has sudo privileges.')"

# Create subdirectories
echo "Creating necessary subdirectories..."
mkdir -p "$PROJECT_DIR/file_watcher" "$PROJECT_DIR/file_watcher/file_watcher_temp" "$PROJECT_DIR/logs" "$PROJECT_DIR/archive" "$PROJECT_DIR/jobscripts" "$PROJECT_DIR/systemscripts" "$PROJECT_DIR/onboarding" && echo "Subdirectories created." || log_error "Failed to create subdirectories" "$(echo 'Check disk space and permissions on parent directory.')"
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR/file_watcher" "$PROJECT_DIR/file_watcher/file_watcher_temp" "$PROJECT_DIR/logs" "$PROJECT_DIR/archive" "$PROJECT_DIR/jobscripts" "$PROJECT_DIR/systemscripts" "$PROJECT_DIR/onboarding"
sudo chmod -R 2770 "$PROJECT_DIR/file_watcher" "$PROJECT_DIR/file_watcher/file_watcher_temp" "$PROJECT_DIR/logs" "$PROJECT_DIR/archive" "$PROJECT_DIR/jobscripts" "$PROJECT_DIR/systemscripts" "$PROJECT_DIR/onboarding" && echo "Subdirectory permissions set." || log_error "Failed to set subdirectory permissions" "$(echo 'Check if etl_group exists and user has sudo privileges.')"

# Define log file path and redirect stdout and stderr
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== User configuration started at $(date) by $CURRENT_USER ==="

# Set log file permissions
sudo chown "$CURRENT_USER":etl_group "$LOG_FILE"
sudo chmod 660 "$LOG_FILE" && echo "Log file permissions set." || log_error "Failed to set log file permissions" "$(echo 'Check if etl_group exists and user has sudo privileges.')"

# Pre-execution system checks
log_system_check "Checking if etl_group exists" "$(getent group etl_group || echo 'Not found')"
log_system_check "Checking network connectivity" "$(ping -c 1 8.8.8.8 > /dev/null 2>&1 && echo 'Success' || echo 'Failed')"
log_system_check "Checking disk space on /home" "$(df -h /home | tail -1)"

# Configure sudoers for etl_user (limit to necessary commands)
echo "Allowing etl_user to run specific commands without password..."
echo "etl_user ALL=(ALL) NOPASSWD: $PROJECT_DIR/venv/bin/python, /bin/bash, /bin/chown, /bin/chmod" | sudo tee /etc/sudoers.d/etl_user >/dev/null
sudo chmod 440 /etc/sudoers.d/etl_user && sudo visudo -c && echo "Sudoers configuration set." || log_error "Failed to configure sudoers"

# Set up cron for etl_user
echo "Configuring cron for etl_user..."
CRON_FILE="/etc/cron.d/etl_jobs"
echo "0 8 * * * etl_user PATH=/usr/local/bin:/usr/bin:/bin /bin/bash $PROJECT_DIR/jobscripts/run_python_etl_script.sh send_reports.py 2 >> $PROJECT_DIR/logs/etl_cron.log 2>&1" | sudo tee "$CRON_FILE" >/dev/null
sudo chown root:cron_etl "$CRON_FILE"
sudo chmod 644 "$CRON_FILE" && echo "Cron job configured for etl_user." || log_error "Failed to configure cron"

echo "=== User configuration complete at $(date) ==="
echo "Check $LOG_FILE for any issues."