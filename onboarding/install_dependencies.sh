#!/bin/bash
# Description: Installs dependencies and configures a Linux Mint Desktop for ETL workflows.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"
PROJECT_DIR="$HOME_DIR/client_etl_workflow"

# Function to log errors with timestamp and command output
log_error() {
    local error_msg="$1"
    local command_output="$2"
    echo "[ERROR] $(date): $error_msg" >&2
    if [ -n "$command_output" ]; then
        echo "[ERROR DETAIL] Command output: $command_output" >&2
    fi
    echo "[INFO] Check system logs (e.g., /var/log/syslog) for additional details." >&2
    exit 1
}

# Function to log system checks
log_system_check() {
    local check_name="$1"
    local check_result="$2"
    echo "[CHECK] $(date): $check_name: $check_result"
}

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
LOG_FILE="$PROJECT_DIR/logs/install_dependencies.log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Installation started at $(date) by $CURRENT_USER ==="

# Set log file permissions
sudo chown "$CURRENT_USER":etl_group "$LOG_FILE"
sudo chmod 660 "$LOG_FILE" && echo "Log file permissions set." || log_error "Failed to set log file permissions" "$(echo 'Check if etl_group exists and user has sudo privileges.')"

# Pre-execution system checks
log_system_check "Checking if etl_group exists" "$(getent group etl_group || echo 'Not found')"
log_system_check "Checking network connectivity" "$(ping -c 1 8.8.8.8 > /dev/null 2>&1 && echo 'Success' || echo 'Failed')"
log_system_check "Checking disk space on /home" "$(df -h /home | tail -1)"

# Update package lists
echo "Updating package lists..."
sudo apt update -y 2>apt_update_error.log && echo "Package lists updated successfully." || log_error "Apt update failed" "$(cat apt_update_error.log)"
rm -f apt_update_error.log

# Install dependencies, including PostgreSQL and client tools
echo "Installing dependencies..."
sudo apt install -y git acl postgresql postgresql-contrib cron python3.12 python3.12-venv python3.12-dev \
    libpq-dev build-essential 2>apt_install_error.log && echo "Dependencies installed." || log_error "Failed to install dependencies" "$(cat apt_install_error.log)"
rm -f apt_install_error.log

# Install DBeaver Community Edition
echo "Installing DBeaver Community Edition..."
wget -O - https://dbeaver.io/debs/dbeaver.gpg.key 2>wget_error.log | sudo apt-key add - 2>apt_key_error.log || log_error "Failed to add DBeaver GPG key" "$(cat wget_error.log; cat apt_key_error.log)"
rm -f wget_error.log apt_key_error.log
echo "deb https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list
sudo apt update -y 2>dbeaver_update_error.log && echo "DBeaver repository updated." || log_error "Failed to update DBeaver repository" "$(cat dbeaver_update_error.log)"
rm -f dbeaver_update_error.log
sudo apt install -y dbeaver-ce 2>dbeaver_install_error.log && echo "DBeaver installed." || log_error "Failed to install DBeaver" "$(cat dbeaver_install_error.log)"
rm -f dbeaver_install_error.log

# Set Python 3.12 as default
echo "Setting Python 3.12 as default..."
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2 2>update_alternatives_error.log || log_error "Failed to install Python 3.12 alternative" "$(cat update_alternatives_error.log)"
sudo update-alternatives --set python3 /usr/bin/python3.12 2>set_python_error.log && echo "Python 3.12 set as default." || log_error "Failed to set Python 3.12 as default" "$(cat set_python_error.log)"
rm -f update_alternatives_error.log set_python_error.log

# Start and enable cron service
echo "Starting and enabling cron service..."
if ! systemctl is-active cron >/dev/null 2>&1; then
    sudo systemctl start cron 2>cron_start_error.log && echo "Cron service started." || log_error "Failed to start cron" "$(cat cron_start_error.log)"
    rm -f cron_start_error.log
fi
sudo systemctl enable cron 2>cron_enable_error.log && echo "Cron service enabled." || log_error "Failed to enable cron" "$(cat cron_enable_error.log)"
rm -f cron_enable_error.log

# Ensure PostgreSQL is running and enabled
echo "Ensuring PostgreSQL is running and enabled at boot..."
PG_VERSION=$(ls /etc/postgresql | grep -E '^[0-9]+$' | sort -nr | head -n1)
sudo systemctl restart postgresql 2>pg_restart_error.log && sudo systemctl enable postgresql 2>pg_enable_error.log && echo "PostgreSQL restarted and enabled." || log_error "Failed to configure PostgreSQL service" "$(cat pg_restart_error.log; cat pg_enable_error.log)"
rm -f pg_restart_error.log pg_enable_error.log

# Configure PostgreSQL authentication
echo "Configuring PostgreSQL authentication..."
sudo cp "/etc/postgresql/$PG_VERSION/main/pg_hba.conf" "/etc/postgresql/$PG_VERSION/main/pg_hba.conf.bak" 2>pg_hba_backup_error.log || log_error "Failed to backup pg_hba.conf" "$(cat pg_hba_backup_error.log)"
sudo sed -i 's/local   all             all                                     peer/local   all             all                                     md5/' "/etc/postgresql/$PG_VERSION/main/pg_hba.conf" 2>sed_error.log || log_error "Failed to modify pg_hba.conf" "$(cat sed_error.log)"
sudo systemctl restart postgresql 2>pg_restart_auth_error.log && echo "PostgreSQL authentication configured." || log_error "Failed to configure PostgreSQL authentication" "$(cat pg_restart_auth_error.log)"
rm -f pg_hba_backup_error.log sed_error.log pg_restart_auth_error.log

# Create PostgreSQL user 'yostfunds' and set password
echo "Creating PostgreSQL user 'yostfunds'..."
sudo -u postgres psql -c "CREATE ROLE yostfunds WITH LOGIN PASSWORD 'etlserver2025!';" 2>psql_create_error.log || log_error "Failed to create user yostfunds" "$(cat psql_create_error.log)"
sudo -u postgres psql -c "ALTER ROLE yostfunds CREATEDB;" 2>psql_alter_error.log || log_error "Failed to grant CREATEDB to yostfunds" "$(cat psql_alter_error.log)"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE postgres TO yostfunds;" 2>psql_grant_error.log || log_error "Failed to grant privileges to yostfunds" "$(cat psql_grant_error.log)"
echo "PostgreSQL user 'yostfunds' created and configured."
rm -f psql_create_error.log psql_alter_error.log psql_grant_error.log

# Set up Python virtual environment
echo "Setting up Python virtual environment..."
cd "$PROJECT_DIR"
if [ ! -d "venv" ]; then
    /usr/bin/python3.12 -m venv venv 2>venv_create_error.log && echo "Virtual environment created." || log_error "Failed to create virtual environment" "$(cat venv_create_error.log)"
    rm -f venv_create_error.log
fi

# Upgrade pip and install Python dependencies
echo "Upgrading pip..."
"$PROJECT_DIR/venv/bin/pip" install --upgrade pip -v 2>pip_upgrade_error.log && echo "Pip upgraded." || log_error "Failed to upgrade pip" "$(cat pip_upgrade_error.log)"
rm -f pip_upgrade_error.log
echo "Installing build dependencies..."
"$PROJECT_DIR/venv/bin/pip" install setuptools wheel -v 2>build_deps_error.log && echo "Build dependencies installed." || log_error "Failed to install build dependencies" "$(cat build_deps_error.log)"
rm -f build_deps_error.log
echo "Installing Python dependencies..."
"$PROJECT_DIR/venv/bin/pip" install numpy==1.26.4 pandas==1.5.3 requests==2.28.1 psycopg2-binary==2.9.5 matplotlib==3.7.1 \
    scrapy==2.11.2 beautifulsoup4==4.12.3 openpyxl==3.1.2 aiohttp==3.9.5 tqdm==4.66.5 -v 2>python_deps_error.log && echo "Python dependencies installed." || log_error "Failed to install Python dependencies" "$(cat python_deps_error.log)"
rm -f python_deps_error.log

# Set permissions for virtual environment
echo "Setting permissions for virtual environment..."
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR/venv"
sudo chmod -R 2770 "$PROJECT_DIR/venv" && echo "Virtual environment permissions set." || log_error "Failed to set virtual environment permissions" "$(echo 'Check if etl_group exists and user has sudo privileges.')"

echo "=== Setup complete at $(date) ==="
echo "Check $LOG_FILE for any issues."