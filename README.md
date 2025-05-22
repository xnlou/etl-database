# Client ETL Workflow Project

## Project Overview
The **Client ETL Workflow Project** establishes an automated Extract, Transform, Load (ETL) pipeline on a Linux Mint Desktop, designed for managing client data workflows in a homelab environment. The pipeline automates data ingestion, processing, and reporting, ensuring secure and efficient handling of client data. Key functionalities include:

- **Web Scraping**: Extracts data from static websites (e.g., MeetMax event pages) using Python scripts like `meetmax_url_check.py`.
- **File Conversion**: Converts client XLS/XLSX files to CSV using `xls_to_csv.py` with `openpyxl` and `xlrd`.
- **Data Processing and Loading**: Transforms data using `pandas` and loads it into a PostgreSQL database via `generic_import.py` with `psycopg2`.
- **Email Reporting**: Supports automated email notifications for job status or data summaries using `smtplib` (to be implemented).
- **Secure File Exchange**: Facilitates client file sharing via SFTP or cloud-based solutions, with strong authentication and encryption.

### Project Goals
- **Automation**: Executes weekly ETL runs with minimal maintenance using Bash and Python scripts, scheduled via cron.
- **Security**: Implements SSL/TLS for database and email interactions, uses strong authentication, and secures file permissions.
- **Scalability**: Handles light data volumes (~1–10 MB/run) with flexibility for moderate growth.
- **Traceability**: Logs all operations to CSV, TXT, and PostgreSQL for debugging and monitoring.
- **Maintainability**: Uses open-source tools and a modular structure for easy updates and extensions.

## How It Works
The ETL pipeline operates as a series of modular scripts coordinated within a structured directory environment:

1. **Setup and Configuration**:
   - `configure_etl_user.sh`: Creates an `etl_user` and `etl_group`, sets up directories (`/home/$USER/client_etl_workflow`), and configures cron for scheduling.
   - `install_dependencies.sh`: Installs PostgreSQL, Python 3.12, Git, DBeaver, and Python dependencies (`pandas`, `scrapy`, `openpyxl`, etc.) in a virtual environment.
   - `directory_management.py`: Ensures consistent directory structure for logs, file processing, and scripts.

2. **Data Extraction**:
   - `meetmax_url_check.py`: Scrapes MeetMax event pages to identify valid URLs and downloadable XLS files, saving results to CSV and PostgreSQL (`public.tmeetmaxurlcheck`).
   - `meetmax_url_download.py`: Downloads XLS files from identified URLs, storing them in `file_watcher/` for further processing.

3. **Data Transformation**:
   - `xls_to_csv.py`: Converts downloaded XLS/XLSX files to CSV, handling both modern (`openpyxl`) and legacy (`xlrd`) formats.
   - `generic_import.py`: Reads CSV files, applies transformations based on configurations in `dba.timportconfig`, and prepares data for database loading.

4. **Data Loading**:
   - `generic_import.py`: Loads transformed data into PostgreSQL tables (e.g., `dba.tmeetmax`), dynamically creating tables or adding columns based on the import strategy. It manages metadata, dataset IDs, and ensures data integrity.

5. **Logging and Monitoring**:
   - `log_utils.py`: Records detailed logs to CSV, TXT, and PostgreSQL (`dba.tLogEntry`), capturing timestamps, process types, and run UUIDs for traceability.
   - Logs are stored in `/home/$USER/client_etl_workflow/logs/` with permissions set to `660` for security.

6. **Automation**:
   - `run_python_etl_script.sh`: A wrapper script that activates the Python virtual environment and runs ETL scripts, ensuring consistent execution.
   - Cron jobs (configured in `/etc/cron.d/etl_jobs`) schedule weekly runs, logging output to `logs/etl_cron.log`.

7. **Secure File Exchange**:
   - Supports SFTP for client file uploads/downloads, with SSH enabled by `install_dependencies.sh`.
   - File permissions are set to `660` or `770` for `etl_group` access, ensuring secure handling.

## Prerequisites
- **Operating System**: Linux Mint Desktop.
- **Hardware**: 16GB RAM, 500GB storage, stable internet connection.
- **Access**: User with sudo privileges for installation scripts.
- **Dependencies**: Installed via `install_dependencies.sh` (see Installation Instructions).

## Directory Structure
The project is organized under `/home/$USER/client_etl_workflow`:
- `archive/`: Stores processed files after import.
- `file_watcher/`: Monitors incoming client files.
  - `file_watcher_temp/`: Temporary storage for intermediate files.
- `jobscripts/`: ETL scripts (e.g., `meetmax_url_check.py`, `meetmax_url_download.py`).
- `logs/`: Stores CSV and TXT log files for all operations.
- `systemscripts/`: Utility scripts (e.g., `log_utils.py`, `xls_to_csv.py`).
- `venv/`: Python virtual environment with dependencies.

## Installation Instructions

### Script Execution Order
Run the following scripts in order to set up the ETL pipeline. Each script logs its progress for troubleshooting.

1. **`configure_etl_user.sh`**  
   Sets up the user, group, and directory structure, and configures a cron job.
   - **Run Command**:
     ```bash
     chmod +x configure_etl_user.sh
     ./configure_etl_user.sh