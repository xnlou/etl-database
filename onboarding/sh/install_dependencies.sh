#!/bin/bash
# Description: Installs dependencies and configures a Linux Mint Desktop for ETL workflows.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
PROJECT_DIR="/home/yostfundsadmin/client_etl_workflow"
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
    libpq-dev build-essential openssh-server && echo "Dependencies installed." || log_error "Failed to install dependencies"

# Configure Git global username to the current user
echo "Configuring Git global username..."
if ! git config --global user.name | grep -q "$CURRENT_USER"; then  # Added for idempotency
    git config --global user.name "$CURRENT_USER" && echo "Git username set to '$CURRENT_USER'." || log_error "Failed to set Git username"
else
    echo "Git username already set to '$CURRENT_USER'."
fi

# Install DBeaver Community Edition
echo "Installing DBeaver Community Edition..."
if ! dpkg -l dbeaver-ce >/dev/null 2>&1; then  # Added for idempotency
    wget -O - https://dbeaver.io/debs/dbeaver.gpg.key | sudo apt-key add - || log_error "Failed to add DBeaver GPG key"
    echo "deb https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list || log_error "Failed to add DBeaver repository"
    sudo apt update && echo "DBeaver repository updated." || log_error "Failed to update DBeaver repository"
    sudo apt install -y dbeaver-ce && echo "DBeaver installed." || log_error "Failed to install DBeaver"
else
    echo "DBeaver already installed."
fi

# Enable and start SSH service
echo "Enabling and starting SSH service..."
if ! systemctl is-enabled ssh >/dev/null 2>&1; then  # Added for idempotency
    sudo systemctl enable ssh || log_error "Failed to enable SSH service"
fi
if ! systemctl is-active ssh >/dev/null 2>&1; then  # Added for idempotency
    sudo systemctl start ssh || log_error "Failed to start SSH service"
fi
echo "SSH service enabled and started."

# Set Python 3.12 as default
echo "Setting Python 3.12 as default..."
if ! update-alternatives --get-selections | grep -q "python3.*python3.12"; then  # Added for idempotency
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2 || log_error "Failed to install Python 3.12 alternative"
    sudo update-alternatives --set python3 /usr/bin/python3.12 && echo "Python 3.12 set as default." || log_error "Failed to set Python 3.12 as default"
else
    echo "Python 3.12 already set as default."
fi

# Start and enable cron service
echo "Starting and enabling cron service..."
if ! systemctl is-active cron >/dev/null 2>&1; then
    sudo systemctl start cron && echo "Cron service started." || log_error "Failed to start cron"
fi
if ! systemctl is-enabled cron >/dev/null 2>&1; then  # Added for idempotency
    sudo systemctl enable cron && echo "Cron service enabled." || log_error "Failed to enable cron"
else
    echo "Cron service already enabled."
fi

# Ensure PostgreSQL is running and enabled at boot
echo "Ensuring PostgreSQL is running and enabled at boot..."
PG_VERSION=$(ls /etc/postgresql | grep -E '^[0-9]+$' | sort -nr | head -1)
if ! systemctl is-active postgresql >/dev/null 2>&1 || ! systemctl is-enabled postgresql >/dev/null 2>&1; then  # Added for idempotency
    sudo systemctl restart postgresql && sudo systemctl enable postgresql && echo "PostgreSQL restarted and enabled." || log_error "Failed to configure PostgreSQL service"
else
    echo "PostgreSQL already running and enabled."
fi

# Configure PostgreSQL authentication
echo "Configuring PostgreSQL authentication..."
if ! grep -q "local   all             all                                     md5" "/etc/postgresql/$PG_VERSION/main/pg_hba.conf"; then  # Added for idempotency
    sudo cp "/etc/postgresql/$PG_VERSION/main/pg_hba.conf" "/etc/postgresql/$PG_VERSION/main/pg_hba.conf.bak" || log_error "Failed to back up PostgreSQL config"
    sudo sed -i 's/local   all             all                                     peer/local   all             all                                     md5/' "/etc/postgresql/$PG_VERSION/main/pg_hba.conf" || log_error "Failed to modify PostgreSQL config"
    sudo systemctl restart postgresql && echo "PostgreSQL authentication configured." || log_error "Failed to configure PostgreSQL authentication"
else
    echo "PostgreSQL authentication already configured."
fi

# Create PostgreSQL user based on the current user
echo "Creating PostgreSQL user '$CURRENT_USER'..."
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$CURRENT_USER'" | grep -q 1; then  # Added for idempotency
    sudo -u postgres psql -c "CREATE ROLE $CURRENT_USER WITH LOGIN PASSWORD 'etlserver2025!';" || log_error "Failed to create PostgreSQL user"
    sudo -u postgres psql -c "ALTER ROLE $CURRENT_USER CREATEDB;" || log_error "Failed to grant CREATEDB to PostgreSQL user"
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE postgres TO $CURRENT_USER;" || log_error "Failed to grant privileges to PostgreSQL user"
    echo "PostgreSQL user '$CURRENT_USER' created and configured."
else
    echo "PostgreSQL user '$CURRENT_USER' already exists."
fi

# Set up Python virtual environment
echo "Setting up Python virtual environment..."
cd "$PROJECT_DIR"
if [ ! -d "venv" ]; then
    /usr/bin/python3.12 -m venv venv && echo "Virtual environment created." || log_error "Failed to create virtual environment"
else
    echo "Virtual environment already exists."  # Added for idempotency
fi

# Upgrade pip and install Python dependencies
echo "Upgrading pip..."
"$PROJECT_DIR/venv/bin/pip" install --upgrade pip && echo "Pip upgraded." || log_error "Failed to upgrade pip"
echo "Installing build dependencies..."
"$PROJECT_DIR/venv/bin/pip" install setuptools wheel && echo "Build dependencies installed." || log_error "Failed to install build dependencies"
echo "Installing Python dependencies..."
"$PROJECT_DIR/venv/bin/pip" install numpy==1.26.4 pandas==1.5.3 requests==2.28.1 psycopg2-binary==2.9.5 matplotlib==3.7.1 \
    scrapy==2.11.2 beautifulsoup4==4.12.3 openpyxl==3.1.2 aiohttp==3.9.5 tqdm==4.66.5 xlrd==2.0.1 croniter==2.0.5 sqlalchemy==2.0.30 \
    && echo "Python dependencies installed." || log_error "Failed to install Python dependencies"

# Set permissions for virtual environment
echo "Setting permissions for virtual environment..."
# Added for idempotency: Check if etl_group exists
if ! getent group etl_group >/dev/null; then
    sudo groupadd etl_group || log_error "Failed to create etl_group"
    echo "Created etl_group."
else
    echo "etl_group already exists."
fi
# Added for idempotency: Check if CURRENT_USER exists
if ! id "$CURRENT_USER" >/dev/null 2>&1; then
    sudo adduser --system --group "$CURRENT_USER" || log_error "Failed to create $CURRENT_USER"
    echo "Created user $CURRENT_USER."
else
    echo "User $CURRENT_USER already exists."
fi
# Added for idempotency: Check if CURRENT_USER is in etl_group
if ! groups "$CURRENT_USER" | grep -qw etl_group; then
    sudo usermod -aG etl_group "$CURRENT_USER" || log_error "Failed to add $CURRENT_USER to etl_group"
    echo "Added $CURRENT_USER to etl_group."
else
    echo "$CURRENT_USER is already in etl_group."
fi
# Added for idempotency: Check if venv exists
if [ -d "$PROJECT_DIR/venv" ]; then
    # Added for idempotency: Check current ownership
    current_owner=$(stat -c %U:%G "$PROJECT_DIR/venv" 2>/dev/null)
    if [ "$current_owner" != "$CURRENT_USER:etl_group" ]; then
        sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR/venv" || log_error "Failed to change ownership of virtual environment"
        echo "Changed ownership of virtual environment."
    else
        echo "Virtual environment ownership already set."
    fi
    # Added for idempotency: Check current permissions
    current_perms=$(stat -c %a "$PROJECT_DIR/venv" 2>/dev/null)
    if [ "$current_perms" != "2770" ]; then
        sudo chmod -R 2770 "$PROJECT_DIR/venv" && echo "Virtual environment permissions set." || log_error "Failed to set virtual environment permissions"
    else
        echo "Virtual environment permissions already set."
    fi
else
    echo "Virtual environment directory $PROJECT_DIR/venv does not exist. Skipping permission setup."
fi

# Configure cron permissions for yostfundsadmin and etl_user
echo "Configuring cron permissions for $CURRENT_USER and etl_user..."
# Create cron_etl group if it doesn't exist
if ! getent group cron_etl >/dev/null; then
    sudo groupadd cron_etl || log_error "Failed to create cron_etl group"
    echo "Created cron_etl group."
else
    echo "cron_etl group already exists."
fi

# Added for idempotency: Check if etl_user exists
if ! id etl_user >/dev/null 2>&1; then
    sudo adduser --system --group etl_user || log_error "Failed to create etl_user"
    echo "Created etl_user."
else
    echo "etl_user already exists."
fi

# Add yostfundsadmin and etl_user to cron_etl group
if ! groups "$CURRENT_USER" | grep -qw cron_etl; then  # Added for idempotency
    sudo usermod -aG cron_etl "$CURRENT_USER" || log_error "Failed to add $CURRENT_USER to cron_etl group"
    echo "Added $CURRENT_USER to cron_etl group."
else
    echo "$CURRENT_USER is already in cron_etl group."
fi
if ! groups etl_user | grep -qw cron_etl; then  # Added for idempotency
    sudo usermod -aG cron_etl etl_user || log_error "Failed to add etl_user to cron_etl group"
    echo "Added etl_user to cron_etl group."
else
    echo "etl_user is already in cron_etl group."
fi

# Change group ownership and permissions of /etc/cron.d/
# Added for idempotency: Check current group ownership
current_group=$(stat -c %G /etc/cron.d 2>/dev/null)
if [ "$current_group" != "cron_etl" ]; then
    sudo chgrp cron_etl /etc/cron.d || log_error "Failed to change group ownership of /etc/cron.d to cron_etl"
    echo "Changed /etc/cron.d group to cron_etl."
else
    echo "/etc/cron.d group is already cron_etl."
fi
# Added for idempotency: Check current permissions
current_perms=$(stat -c %a /etc/cron.d 2>/dev/null)
if [ "$current_perms" != "775" ]; then
    sudo chmod 775 /etc/cron.d || log_error "Failed to set permissions on /etc/cron.d"
    echo "Set /etc/cron.d permissions to 775."
else
    echo "/etc/cron.d permissions are already 775."
fi

# Create or update /etc/cron.d/etl_jobs
CRON_FILE="/etc/cron.d/etl_jobs"
if [ ! -f "$CRON_FILE" ]; then
    sudo touch "$CRON_FILE" || log_error "Failed to create $CRON_FILE"
    echo "Created $CRON_FILE."
else
    echo "$CRON_FILE already exists."  # Added for idempotency
fi
# Added for idempotency: Check current ownership
current_owner=$(stat -c %U:%G "$CRON_FILE" 2>/dev/null)
if [ "$current_owner" != "root:cron_etl" ]; then
    sudo chown root:cron_etl "$CRON_FILE" || log_error "Failed to set ownership of $CRON_FILE to root:cron_etl"
    echo "Set $CRON_FILE ownership to root:cron_etl."
else
    echo "$CRON_FILE ownership is already root:cron_etl."
fi
# Added for idempotency: Check current permissions
current_perms=$(stat -c %a "$CRON_FILE" 2>/dev/null)
if [ "$current_perms" != "644" ]; then
    sudo chmod 644 "$CRON_FILE" || log_error "Failed to set permissions on $CRON_FILE"
    echo "Set $CRON_FILE permissions to 644."
else
    echo "$CRON_FILE permissions are already 644."
fi
echo "Cron permissions configured for $CURRENT_USER and etl_user."

echo "=== Setup complete at $(date) ==="
echo "Check $LOG_FILE for any issues."