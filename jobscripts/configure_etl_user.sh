#!/bin/bash
# Description: Configures etl_user and group for ETL workflows on Linux Mint Desktop.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"
LOG_FILE="$HOME_DIR/configure_etl_user.log"

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== User configuration started at $(date) by $CURRENT_USER ==="

# Function to log errors with timestamp
log_error() {
    echo "[ERROR] $(date): $1" >&2
}

# Create etl_group if it doesn't exist
echo "Creating etl_group if it doesn't exist..."
if ! getent group etl_group > /dev/null 2>&1; then
    sudo groupadd etl_group && echo "etl_group created." || log_error "Failed to create etl_group"
fi

# Create etl_user if it doesn't exist
echo "Creating etl_user if it doesn't exist..."
if ! id -u etl_user > /dev/null 2>&1; then
    sudo useradd -m -s /bin/bash -G etl_group etl_user && echo "etl_user created." || log_error "Failed to create etl_user"
fi

# Add users to etl_group
echo "Adding $CURRENT_USER and etl_user to etl_group..."
sudo usermod -aG etl_group "$CURRENT_USER" && sudo usermod -aG etl_group etl_user && echo "Users added to etl_group." || log_error "Failed to add users to etl_group"

# Set etl_user home directory
echo "Setting etl_user home directory to $HOME_DIR..."
sudo usermod -d "$HOME_DIR" etl_user && echo "etl_user home directory set." || log_error "Failed to set etl_user home directory"

# Adjust home directory permissions
echo "Adjusting home directory permissions..."
sudo chown -R "$CURRENT_USER":etl_group "$HOME_DIR"
sudo chmod -R 2770 "$HOME_DIR"  # Stricter permissions (group rwx, others none)
sudo setfacl -R -m g:etl_group:rwx "$HOME_DIR"
sudo setfacl -R -d -m g:etl_group:rwx "$HOME_DIR" && echo "Home directory permissions set." || log_error "Failed to set home directory permissions"

# Set up project directory
PROJECT_DIR="$HOME_DIR/etl_workflow"
echo "Ensuring project directory permissions: $PROJECT_DIR..."
sudo chown -R "$CURRENT_USER":etl_group "$PROJECT_DIR"
sudo chmod -R 2770 "$PROJECT_DIR" && echo "Project directory permissions set." || log_error "Failed to set project directory permissions"

# Configure sudoers for etl_user with limited commands
echo "Allowing etl_user to run specific commands without password..."
echo "etl_user ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart postgresql, /usr/bin/git" | sudo tee /etc/sudoers.d/etl_user >/dev/null
sudo chmod 440 /etc/sudoers.d/etl_user && echo "Sudoers configuration set." || log_error "Failed to configure sudoers"

echo "=== User configuration complete at $(date) ==="
echo "Check $LOG_FILE for any issues."