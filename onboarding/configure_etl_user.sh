#!/bin/bash
# Description: Configures etl_user and group for ETL workflows on Linux Mint Desktop.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"
PROJECT_DIR="$HOME_DIR/client_etl_workflow"
LOG_FILE="$HOME_DIR/configure_etl_user.log"

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== User configuration started at $(date) by $CURRENT_USER ==="

# Function to log errors with timestamp
log_error() {
    echo "[ERROR] $(date): $1" >&2
    exit 1
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

# Create project directory
echo "Creating project directory: $PROJECT_DIR..."
mkdir -p "$PROJECT_DIR" || log_error "Failed to create project directory"
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR"
sudo chmod -R 2770 "$PROJECT_DIR"  # Group rwx, setgid
sudo setfacl -R -m g:etl_group:rwx "$PROJECT_DIR"
sudo setfacl -R -d -m g:etl_group:rwx "$PROJECT_DIR" && echo "Directory permissions set." || log_error "Failed to set directory permissions"

# Configure sudoers for etl_user (limit to necessary commands)
echo "Allowing etl_user to run specific commands without password..."
echo "etl_user ALL=(ALL) NOPASSWD: /usr/bin/python3, /bin/bash" | sudo tee /etc/sudoers.d/etl_user >/dev/null
sudo chmod 440 /etc/sudoers.d/etl_user && sudo visudo -c && echo "Sudoers configuration set." || log_error "Failed to configure sudoers"

# Set up cron for etl_user (update path if needed)
echo "Configuring cron for etl_user..."
CRON_FILE="/etc/cron.d/etl_jobs"
echo "0 2 * * 1 etl_user /bin/bash /path/to/your/etl_script.sh >> $PROJECT_DIR/logs/etl_cron.log 2>&1" | sudo tee "$CRON_FILE" >/dev/null
sudo chown root:root "$CRON_FILE"
sudo chmod 644 "$CRON_FILE" && echo "Cron job configured for etl_user." || log_error "Failed to configure cron"

echo "=== User configuration complete at $(date) ==="
echo "Check $LOG_FILE for any issues."