# Client ETL Workflow Project

## Project Overview
This project sets up an automated ETL (Extract, Transform, Load) pipeline on a Linux Mint Desktop for managing client data workflows in a homelab environment. The pipeline is designed to:
- Scrape data from websites.
- Convert client XLSX/XLS files to CSV.
- Process and load data into a PostgreSQL database.
- Send email reports to clients with job status or data summaries.
- Support secure client file exchanges via SFTP or cloud-based sharing.

### Project Goals
- **Automation**: Automate weekly ETL runs with minimal maintenance using Bash and Python scripts.
- **Security**: Implement SSL/TLS for database and email interactions, and use strong authentication for file exchanges.
- **Scalability**: Handle light data volumes (~1–10 MB/run) with potential for moderate growth.
- **Accessibility**: Provide GUI tools (DBeaver, pgAdmin) for easy database management.
- **Traceability**: Maintain detailed logs for debugging and monitoring.

## Prerequisites
- **Operating System**: Linux Mint Desktop.
- **Hardware**: 16GB RAM, 500GB storage, stable internet connection.
- **Access**: User with sudo privileges to run installation scripts.

## Directory Structure
The project uses the following directory structure under `/home/$USER/client_etl_workflow`:
- `archive/`: Stores archived files.
- `file_watcher/`: Monitors incoming client files.
  - `file_watcher_temp/`: Temporary storage for file processing.
- `jobscripts/`: Contains ETL scripts (e.g., `meetmax_url_check.py`).
- `logs/`: Stores log files for all scripts.
- `onboarding/`: For onboarding-related files.
- `systemscripts/`: Utility scripts (e.g., `log_utils.py`).
- `venv/`: Python virtual environment for dependencies.

## Installation Instructions

### Script Execution Order
To set up the ETL server, run the scripts in the following order. Ensure each script completes successfully before proceeding to the next one.

1. **[Optional] `CL_onboarding.sh`**  
   This script is for personal setup and not strictly part of the ETL pipeline. It downloads a repository ZIP and installs additional software for convenience.
   - **Purpose**: Installs personal tools (Caffeine, AnyDesk, Brave Browser, Discord) and downloads the project repository as a ZIP.
   - **Run Command**:
     ```bash
     chmod +x CL_onboarding.sh
     ./CL_onboarding.sh
     ```
   - **Log File**: Check `/home/$USER/CL_onboarding_YYYYMMDD_HHMMSS.log` for details.

2. **`configure_etl_user.sh`**  
   This script sets up the user, group, and directory structure for the ETL pipeline.
   - **Purpose**: Creates `etl_user` and `etl_group`, sets up the project directory structure, and configures a cron job for automated ETL runs.
   - **Run Command**:
     ```bash
     chmod +x configure_etl_user.sh
     ./configure_etl_user.sh
     ```
   - **Log File**: Check `/home/$USER/client_etl_workflow/logs/configure_etl_user.log` for details.

3. **`install_dependencies.sh`**  
   This script installs all necessary dependencies and configures the environment.
   - **Purpose**: Installs PostgreSQL, Python 3.12, Git, DBeaver, pgAdmin, Visual Studio Code, and other dependencies, sets up a PostgreSQL user, and configures a Python virtual environment.
   - **Run Command**:
     ```bash
     chmod +x install_dependencies.sh
     ./install_dependencies.sh
     ```
   - **Log File**: Check `/home/$USER/client_etl_workflow/logs/install_dependencies.log` for details.

### Post-Installation Steps
- **Database Setup**: After installation, use DBeaver or pgAdmin to connect to your PostgreSQL database:
  - Host: `localhost`
  - Database: `postgres`
  - User: Your username (set by `install_dependencies.sh`)
  - Password: `etlserver2025!`
  Create a new database for your ETL data (e.g., `etl_db`) and set up tables as needed.
- **Cron Job Configuration**: The `configure_etl_user.sh` script sets up a cron job for `etl_user`. Update the script path in `/etc/cron.d/etl_jobs` to point to your ETL script (e.g., `/home/$USER/client_etl_workflow/jobscripts/meetmax_url_check.sh`).
- **Secure File Exchange**: Set up SFTP or a cloud-based solution (e.g., Nextcloud) for client file sharing, ensuring strong authentication and encryption.

## Additional Notes
- **Logging**: Each script generates a log file in `/home/$USER/client_etl_workflow/logs/` (except `CL_onboarding.sh`, which logs to `/home/$USER/`). Check these logs for debugging issues.
- **Maintenance**:
  - Regularly check disk space (`df -h`) to ensure storage isn’t running low.
  - Monitor PostgreSQL performance and optimize queries if data volume grows.
  - Update dependencies periodically with `sudo apt update && sudo apt upgrade`.
- **Security**:
  - Use SSL/TLS for PostgreSQL connections (configured in `install_dependencies.sh`).
  - Ensure strong passwords for all accounts, including `etl_user` and PostgreSQL roles.
  - Configure a firewall (`ufw`) to restrict access:
    ```bash
    sudo ufw allow ssh
    sudo ufw allow 5432/tcp  # PostgreSQL
    sudo ufw enable
    ```
- **Backups**:
  - Schedule regular database backups with `pg_dump`:
    ```bash
    pg_dump -U your_username etl_db > /path/to/backups/etl_db_$(date +%Y%m%d).sql
    ```
  - Back up key directories (e.g., `logs`, `archive`) using `rsync`:
    ```bash
    rsync -av /home/$USER/client_etl_workflow/ /backup/location/
    ```

## Troubleshooting
- **Script Fails**: Check the relevant log file for error messages. Common issues include network connectivity, missing dependencies, or permission errors.
- **PostgreSQL Connection Issues**: Verify the user credentials and ensure the service is running (`sudo systemctl status postgresql`).
- **Cron Job Not Running**: Check `/etc/cron.d/etl_jobs` for the correct script path and ensure the cron service is active (`sudo systemctl status cron`).

## Future Enhancements
- Add monitoring with tools like Prometheus or Nagios to track system and ETL job performance.
- Implement email notifications for script failures using `smtplib` in Python.
- Expand the ETL pipeline to support larger data volumes or additional data sources.