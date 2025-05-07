#!/bin/bash
# Description: Installs dependencies and configures a Linux Mint Desktop for ETL workflows.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"
PROJECT_DIR="$HOME_DIR/client_etl_workflow"
LOG_FILE="$PROJECT_DIR/logs/install_dependencies.log"

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Installation started at $(date) by $CURRENT_USER ==="

# Function to log errors with timestamp
log_error() {
    echo "[ERROR] $(date): $1" >&2
    exit 1
}

# Update package lists
echo "Updating package lists..."
sudo apt update && echo "Package lists updated successfully." || log_error "Apt update failed"

# Install dependencies, including PostgreSQL and client tools
echo "Installing dependencies..."
sudo apt install -y git acl postgresql postgresql-contrib cron python3.12 python3.12-venv python3.12-dev \
    libpq-dev build-essential && echo "Dependencies installed." || log_error "Failed to install dependencies"

# Install DBeaver Community Edition
echo "Installing DBeaver Community Edition..."
wget -O - https://dbeaver.io/debs/dbeaver.gpg.key | sudo apt-key add - || log_error "Failed to add DBeaver GPG key"
echo "deb https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list
sudo apt update && echo "DBeaver repository updated." || log_error "Failed to update DBeaver repository"
sudo apt install -y dbeaver-ce && echo "DBeaver installed." || log_error "Failed to install DBeaver"

# Set Python 3.12 as default
echo "Setting Python 3.12 as default..."
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2
sudo update-alternatives --set python3 /usr/bin/python3.12 && echo "Python 3.12 set as default." || log_error "Failed to set Python 3.12 as default"

# Start and enable cron service
echo "Starting and enabling cron service..."
if ! systemctl is-active cron >/dev/null 2>&1; then
    sudo systemctl start cron && echo "Cron service started." || log_error "Failed to start cron"
fi
sudo systemctl enable cron && echo "Cron service enabled." || log_error "Failed to enable cron"

# Ensure PostgreSQL is running and enabled
echo "Ensuring PostgreSQL is running and enabled at boot..."
PG_VERSION=$(ls /etc/postgresql | grep -E '^[0-9]+$' | sort -nr | head -n1)
sudo systemctl restart postgresql && sudo systemctl enable postgresql && echo "PostgreSQL restarted and enabled." || log_error "Failed to configure PostgreSQL service"

# Configure PostgreSQL authentication
echo "Configuring PostgreSQL authentication..."
sudo cp "/etc/postgresql/$PG_VERSION/main/pg_hba.conf" "/etc/postgresql/$PG_VERSION/main/pg_hba.conf.bak"
sudo sed -i 's/local   all             all                                     peer/local   all             all                                     md5/' "/etc/postgresql/$PG_VERSION/main/pg_hba.conf"
sudo systemctl restart postgresql && echo "PostgreSQL authentication configured." || log_error "Failed to configure PostgreSQL authentication"

# Create PostgreSQL user 'yostfunds' and set password
echo "Creating PostgreSQL user 'yostfunds'..."
sudo -u postgres psql -c "CREATE ROLE yostfunds WITH LOGIN PASSWORD 'etlserver2025!';" || log_error "Failed to create user yostfunds"
sudo -u postgres psql -c "ALTER ROLE yostfunds CREATEDB;" || log_error "Failed to grant CREATEDB to yostfunds"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE postgres TO yostfunds;" || log_error "Failed to grant privileges to yostfunds"
echo "PostgreSQL user 'yostfunds' created and configured."

# Set up project directory
echo "Creating project directory: $PROJECT_DIR..."
mkdir -p "$PROJECT_DIR"
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR"
sudo chmod -R 2770 "$PROJECT_DIR" && echo "Project directory created and permissions set." || log_error "Failed to set up project directory"

# Create subdirectories
echo "Creating necessary subdirectories..."
mkdir -p "$PROJECT_DIR/file_watcher" "$PROJECT_DIR/logs" "$PROJECT_DIR/archive" "$PROJECT_DIR/jobscripts" "$PROJECT_DIR/systemscripts" && echo "Subdirectories created." || log_error "Failed to create subdirectories"
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR/file_watcher" "$PROJECT_DIR/logs" "$PROJECT_DIR/archive" "$PROJECT_DIR/jobscripts" "$PROJECT_DIR/systemscripts"
sudo chmod -R 2770 "$PROJECT_DIR/file_watcher" "$PROJECT_DIR/logs" "$PROJECT_DIR/archive" "$PROJECT_DIR/jobscripts" "$PROJECT_DIR/systemscripts" && echo "Subdirectory permissions set." || log_error "Failed to set subdirectory permissions"

# Set up Python virtual environment
echo "Setting up Python virtual environment..."
cd "$PROJECT_DIR"
if [ ! -d "venv" ]; then
    /usr/bin/python3.12 -m venv venv && echo "Virtual environment created." || log_error "Failed to create virtual environment"
fi

# Upgrade pip and install Python dependencies
echo "Upgrading pip..."
"$PROJECT_DIR/venv/bin/pip" install --upgrade pip && echo "Pip upgraded." || log_error "Failed to upgrade pip"
echo "Installing build dependencies..."
"$PROJECT_DIR/venv/bin/pip" install setuptools wheel && echo "Build dependencies installed." || log_error "Failed to install build dependencies"
echo "Installing Python dependencies..."
"$PROJECT_DIR/venv/bin/pip" install numpy==1.26.4 pandas==1.5.3 requests==2.28.1 psycopg2-binary==2.9.5 matplotlib==3.7.1 \
    scrapy==2.11.2 beautifulsoup4==4.12.3 openpyxl==3.1.2 && echo "Python dependencies installed." || log_error "Failed to install Python dependencies"

# Set permissions for virtual environment
echo "Setting permissions for virtual environment..."
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR/venv"
sudo chmod -R 2770 "$PROJECT_DIR/venv" && echo "Virtual environment permissions set." || log_error "Failed to set virtual environment permissions"

# Set log file permissions
sudo chown "$CURRENT_USER":etl_group "$LOG_FILE"
sudo chmod 660 "$LOG_FILE" && echo "Log file permissions set." || log_error "Failed to set log file permissions"

echo "=== Setup complete at $(date) ==="
echo "Check $LOG_FILE for any issues."