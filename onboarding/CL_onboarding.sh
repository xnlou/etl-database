#!/bin/bash
# Description: Personal onboarding script to install software.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"

# Create log file with datetime stamp in the current user's directory
LOG_FILE="$HOME_DIR/CL_onboarding_$(date +%Y%m%d_%H%M%S).log"

# Function to log errors with timestamp
log_error() {
    echo "[ERROR] $(date): $1" >&2
    exit 1
}

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Onboarding started at $(date) by $CURRENT_USER ==="

# Update package lists
echo "Updating package lists..."
sudo apt update || log_error "Failed to update package lists"

# Install Caffeine
echo "Installing Caffeine..."
sudo apt install -y caffeine || log_error "Failed to install Caffeine"

# Install AnyDesk
echo "Installing AnyDesk..."
wget -qO - https://keys.anydesk.com/repos/DEB-GPG-KEY | sudo apt-key add - || log_error "Failed to add AnyDesk GPG key"
echo "deb http://deb.anydesk.com/ all main" | sudo tee /etc/apt/sources.list.d/anydesk-stable.list
sudo apt update || log_error "Failed to update AnyDesk repository"
sudo apt install -y anydesk || log_error "Failed to install AnyDesk"

# Install Brave Browser
echo "Installing Brave Browser..."
sudo curl -fsSLo /usr/share/keyrings/brave-browser-archive-keyring.gpg https://brave-browser-apt-release.s3.brave.com/brave-browser-archive-keyring.gpg || log_error "Failed to add Brave Browser GPG key"
echo "deb [signed-by=/usr/share/keyrings/brave-browser-archive-keyring.gpg] https://brave-browser-apt-release.s3.brave.com/ stable main" | sudo tee /etc/apt/sources.list.d/brave-browser-release.list
sudo apt update || log_error "Failed to update Brave Browser repository"
sudo apt install -y brave-browser || log_error "Failed to install Brave Browser"

# Install Discord
echo "Installing Discord..."
wget -O discord.deb "https://discordapp.com/api/download?platform=linux&format=deb" || log_error "Failed to download Discord package"
sudo dpkg -i discord.deb || log_error "Failed to install Discord package"
sudo apt install -f -y || log_error "Failed to fix dependencies for Discord"
rm -f discord.deb

echo "=== Onboarding completed at $(date) ==="
echo "Check $LOG_FILE for any issues."