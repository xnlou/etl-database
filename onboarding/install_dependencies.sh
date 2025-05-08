#!/bin/bash
# Description: Installs dependencies and configures a Linux Mint Desktop for ETL workflows.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"
PROJECT_DIR="$HOME_DIR/client_etl_workflow"
LOG_FILE="$PROJECT_DIR/logs/install_dependencies.log"

# Function to log errors with timestamp and exit code
log_error() {
    local exit_code=$?
    echo "[ERROR] $(date): $1 (Exit code: $exit_code)" >&2
    exit 1
}

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Installation started at $(date) by $CURRENT_USER ==="

# Update package lists
echo "Updating package lists..."
sudo apt update && echo "Package lists updated successfully." || log_error "Apt update failed"

# Install dependencies, including PostgreSQL, client tools, and additional packages
echo "Installing dependencies..."
sudo apt install -y git acl postgresql postgresql-contrib cron python3.12 python3.12-venv python3.12-dev \
    libpq-dev build-essential openssh-server && echo "Dependencies installed." || log_error "Failed to install dependencies: git, acl, postgresql, etc."

# Install DBeaver Community Edition
echo "Installing DBeaver Community Edition..."
wget -O - https://dbeaver.io/debs/dbeaver.gpg.key | sudo apt-key add - || log_error "Failed to add DBeaver GPG key"
echo "deb https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list || log_error "Failed to add DBeaver repository"
sudo apt update && echo "DBeaver repository updated." || log_error "Failed to update DBeaver repository"
sudo apt install -y dbeaver-ce && echo "DBeaver installed." || log_error "Failed to install DBeaver"

# Install Visual Studio Code
echo "Installing Visual Studio Code..."
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > packages.microsoft.gpg || log_error "Failed to download Microsoft GPG key"
sudo install -o root -g root -m 644 packages.microsoft.gpg /etc/apt/trusted.gpg.d/ || log_error "Failed to install Microsoft GPG key"
sudo sh -c 'echo "deb [arch=amd64,arm64,armhf signed-by=/etc/apt/trusted.gpg.d/packages.microsoft.gpg] https://packages.microsoft.com/repos/code stable main" > /etc/apt/sources.list.d/vscode.list' || log_error "Failed to add VS Code repository"
rm -f packages.microsoft.gpg
sudo apt update && echo "VS Code repository updated." || log_error "Failed to update VS Code repository"
sudo apt install -y code && echo "VS Code installed." || log_error "Failed to install Visual Studio Code"

# Enable and start SSH service
echo "Enabling and starting SSH service..."
sudo systemctl enable ssh || log_error "Failed to enable SSH service"
sudo systemctl start ssh || log_error "Failed to start SSH service"

# Set Python 3.12 as default
echo "Setting Python 3.12 as default..."
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2 || log_error "Failed to install Python 3.12 alternative"
sudo update-alternatives --set python3 /usr/bin/python3.12 && echo "Python 3.12 set as default." || log_error "Failed to set Python 3.12 as default"

# Start and enable cron service
echo "Starting and enabling cron service..."
if ! systemctl is-active cron >/dev/null 2>&1; then
    sudo systemctl start cron && echo "Cron service started." || log_error "Failed to start cron"
fi
sudo systemctl enable cron && echo "Cron service enabled." || log_error "Failed to enable cron"

# Ensure PostgreSQL is running and enabled
echo "Ensuring PostgreSQL is running and enabled at boot..."
PG_VERSION=$(ls /etc/postgresql | grep -E '^[0-9]+$' | sort -nr | head -1)
sudo systemctl restart postgresql && sudo systemctl enable postgresql && echo "PostgreSQL restarted and enabled." || log_error "Failed to configure PostgreSQL service"

# Configure PostgreSQL authentication
echo "Configuring PostgreSQL authentication..."
sudo cp "/etc/postgresql/$PG_VERSION/main/pg_hba.conf" "/etc/postgresql/$PG_VERSION/main/pg_hba.conf.bak" || log_error "Failed to back up PostgreSQL config"
sudo sed -i 's/local   all             all                                     peer/local   all             all                                     md5/' "/etc/postgresql/$PG_VERSION/main/pg_hba.conf" || log_error "Failed to modify PostgreSQL config"
sudo systemctl restart postgresql && echo "PostgreSQL authentication configured." || log_error "Failed to configure PostgreSQL authentication"

# Create PostgreSQL user based on the current user
echo "Creating PostgreSQL user '$CURRENT_USER'..."
sudo -u postgres psql -c "CREATE ROLE StandardUser WITH LOGIN PASSWORD 'etlserver2025!';" || log_error "Failed to create PostgreSQL user"
sudo -u postgres psql -c "ALTER ROLE StandardUser CREATEDB;" || log_error "Failed to grant CREATEDB to PostgreSQL user"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE postgres TO StandardUser;" || log_error "Failed to grant privileges to PostgreSQL user"
echo "PostgreSQL user '$CURRENT_USER' created and configured."

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
    scrapy==2.11.2 beautifulsoup4==4.12.3 openpyxl==3.1.2 aiohttp==3.9.5 tqdm==4.66.5 && echo "Python dependencies installed." || log_error "Failed to install Python dependencies"

# Set permissions for virtual environment
echo "Setting permissions for virtual environment..."
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR/venv" || log_error "Failed to change ownership of virtual environment"
sudo chmod -R 2770 "$PROJECT_DIR/venv" && echo "Virtual environment permissions set." || log_error "Failed to set virtual environment permissions"

echo "=== Setup complete at $(date) ==="
echo "Check $LOG_FILE for any issues."