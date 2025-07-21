#!/bin/bash
# Description: Disables power-saving features to keep the server running 24/7.
set -e  # Exit immediately if a command exits with a non-zero status

# Detect the user running the script
CURRENT_USER=$(whoami)
HOME_DIR="/home/$CURRENT_USER"

# Create log file with datetime stamp in the current user's directory
LOG_FILE="$HOME_DIR/disable_power_saving_$(date +%Y%m%d_%H%M%S).log"

# Function to log errors with timestamp
log_error() {
    echo "[ERROR] $(date): $1" >&2
    exit 1
}

# Redirect stdout and stderr to log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== Power saving disable started at $(date) by $CURRENT_USER ==="

# Disable GUI sleep and suspend (Cinnamon settings)
echo "Disabling GUI sleep and suspend settings..."
gsettings set org.cinnamon.settings-daemon.plugins.power sleep-inactive-ac-timeout 0 || log_error "Failed to disable AC sleep timeout"
gsettings set org.cinnamon.settings-daemon.plugins.power sleep-inactive-battery-timeout 0 || log_error "Failed to disable battery sleep timeout"
gsettings set org.cinnamon.settings-daemon.plugins.power idle-dim-time 0 || log_error "Failed to disable idle dim time"
gsettings set org.cinnamon.settings-daemon.plugins.power sleep-display-ac 0 || log_error "Failed to disable AC display sleep"
gsettings set org.cinnamon.settings-daemon.plugins.power sleep-display-battery 0 || log_error "Failed to disable battery display sleep"
echo "GUI sleep and suspend settings disabled."

# Disable console blanking and powerdown
echo "Disabling console blanking and powerdown..."
setterm -blank 0 -powerdown 0 -powersave off || log_error "Failed to disable console blanking and powerdown"
echo "Console blanking and powerdown disabled."

# Update GRUB to disable sleep/suspend
echo "Updating GRUB to disable sleep and suspend..."
GRUB_FILE="/etc/default/grub"
if grep -q "GRUB_CMDLINE_LINUX_DEFAULT" "$GRUB_FILE"; then
    sudo sed -i 's/GRUB_CMDLINE_LINUX_DEFAULT="\(.*\)"/GRUB_CMDLINE_LINUX_DEFAULT="\1 mem_sleep_default=deep acpi=off"/' "$GRUB_FILE" || log_error "Failed to update GRUB configuration"
else
    echo 'GRUB_CMDLINE_LINUX_DEFAULT="mem_sleep_default=deep acpi=off"' | sudo tee -a "$GRUB_FILE" || log_error "Failed to append GRUB configuration"
fi
sudo update-grub || log_error "Failed to update GRUB"
echo "GRUB updated to disable sleep and suspend."

# Disable systemd sleep and suspend
echo "Disabling systemd sleep and suspend..."
sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target || log_error "Failed to mask systemd sleep targets"
echo "HandleSuspendKey=ignore" | sudo tee -a /etc/systemd/logind.conf || log_error "Failed to configure logind.conf for suspend"
echo "HandleHibernateKey=ignore" | sudo tee -a /etc/systemd/logind.conf || log_error "Failed to configure logind.conf for hibernate"
echo "HandleLidSwitch=ignore" | sudo tee -a /etc/systemd/logind.conf || log_error "Failed to configure logind.conf for lid switch"
sudo systemctl restart systemd-logind || log_error "Failed to restart systemd-logind"
echo "Systemd sleep and suspend disabled."

echo "=== Power saving disable completed at $(date) ==="
echo "Check $LOG_FILE for any issues."