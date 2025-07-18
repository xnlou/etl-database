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
## File Descriptions

### Root Directory
*   `.gitignore`: Specifies intentionally untracked files to ignore.
*   `env.sh`: Sets environment variables for the ETL pipeline.
*   `ETL Pipeline Project Tasks.md`: Outlines the tasks for the ETL pipeline project.
*   `LICENSE`: Contains the GNU General Public License.
*   `README.md`: Provides an overview of the project.

### jobscripts/
*   `daily_backup.sh`: A bash script to perform daily backups of the PostgreSQL database.
*   `meetmax_url_check.py`: Scrapes MeetMax event URLs to check for valid events and downloadable files.
*   `meetmax_url_download.py`: Downloads XLS files from URLs identified by the URL checker.
*   `process_inbox.py`: Processes emails from a Gmail inbox based on specified configurations.
*   `run_download_and_import.sh`: A shell script that runs the download and import jobs in sequence.
*   `run_import_job.py`: A wrapper script to run the generic import process for a specific configuration.
*   `run_python_etl_script.sh`: A generic wrapper script to execute Python ETL scripts within the virtual environment.
*   `send_reports.py`: Sends email reports based on configurations in the `dba.treportmanager` table.
*   `testemail.py`: A script to send a test email with SQL query results.
*   `update_cron_jobs.py`: Updates cron jobs based on schedules defined in the database.
*   `weekly_cleanup_logs.sh`: Deletes log files older than 7 days.
*   `weekly_cleanup_meetmaxevents.sh`: Deletes archived MeetMax event files older than 7 days.

### onboarding/sh/
*   `CL_onboarding.sh`: A personal onboarding script to install various software.
*   `configure_etl_user.sh`: Configures the `etl_user`, `etl_group`, and directory structure.
*   `disable_power_saving.sh`: Disables power-saving features to keep the server running.
*   `gmail_api.sh`: Installs Gmail API dependencies.
*   `install_dependencies.sh`: Installs system and Python dependencies for the ETL workflow.
*   `sql_setupscripts.sh`: Executes a series of SQL scripts to set up the database.

### onboarding/sql/
*   `create_f_get_event_changes.sql`: Creates a SQL function to get event changes.
*   `create_importconfig_table.sql`: Creates the `timportconfig` table and related procedures.
*   `create_inboxconfig_table.sql`: Creates the `tinboxconfig` table for Gmail inbox processing.
*   `create_treportmanager.sql`: Creates the `treportmanager` table for managing email reports.
*   `create_tscheduler_procedures.sql`: Creates stored procedures for the task scheduler.
*   `create_tscheduler.sql`: Creates the `tscheduler` table for scheduling tasks.
*   `dataset_setup.sql`: Sets up tables and functions for tracking dataset metadata.
*   `log_cleanup.sql`: Creates a procedure to purge old log entries.
*   `maintenance_procedures.sql`: Defines maintenance procedures and tables.
*   `monitor_long_running_queries.sql`: Creates a procedure to monitor long-running queries.
*   `setup_dba_maintenance.sql`: Sets up the `dba` schema and logging tables.
*   `table_index_monitoring.sql`: Defines a table and procedure to monitor table and index usage.
*   `usefulqueries.sql`: Contains a collection of useful SQL queries for analysis.

### systemscripts/
*   `__init__.py`: Initializes the `systemscripts` directory as a Python package.
*   `credentials.json`: A template for Google API credentials.
*   `db_config.py`: Contains database connection parameters.
*   `directory_management.py`: Manages the creation and initialization of directories.
*   `generic_import.py`: A generic script to import data from files into the database.
*   `log_utils.py`: Provides utility functions for logging.
*   `periodic_utils.py`: A utility for running tasks periodically.
*   `user_utils.py`: A utility to get the current username.
*   `web_utils.py`: Provides utility functions for fetching URLs.
*   `xls_to_csv.py`: Converts XLS/XLSX files to CSV format.

## Cron Job Automation

The ETL pipeline relies on cron, a time-based job scheduler in Unix-like operating systems, to automate its various tasks.

### How to Interpret a Cron Job

A cron job is defined by a line in a crontab file. Each line consists of a schedule followed by the command to execute.

```
* * * * *  user  command_to_execute
- - - - -
| | | | |
| | | | +----- day of week (0 - 7) (Sunday is both 0 and 7)
| | | +------- month (1 - 12)
| | +--------- day of month (1 - 31)
| +----------- hour (0 - 23)
+------------- minute (0 - 59)
```

-   `*`: Represents "every". For example, `*` in the hour field means "every hour".
-   `,`: Specifies a list of values. For example, `5,17` in the hour field means "at 5 AM and 5 PM".
-   `-`: Specifies a range of values. For example, `1-5` in the day of week field means "from Monday to Friday".

### Summary of Scheduled Jobs

The following jobs are configured in `/etc/cron.d/etl_jobs`:

*   **MeetMax Download and Import**:
    *   **Schedule**: `0 5,17 * * 1-5` (At 5:00 AM and 5:00 PM, Monday to Friday).
    *   **Action**: Runs `run_download_and_import.sh` to download the latest MeetMax files and import them into the database.

*   **Send Report #1**:
    *   **Schedule**: `20 11 * * 1-5` (At 11:20 AM, Monday to Friday).
    *   **Action**: Executes `send_reports.py` for the report with `reportID = 1`.

*   **Send Report #2**:
    *   **Schedule**: `25 11 * * 1-5` (At 11:25 AM, Monday to Friday).
    *   **Action**: Executes `send_reports.py` for the report with `reportID = 2`.

*   **MeetMax URL Check**:
    *   **Schedule**: `0 19 * * 5` (At 7:00 PM on Friday).
    *   **Action**: Runs `meetmax_url_check.py` to scan for new and updated MeetMax event URLs.

*   **Weekly Cleanup**:
    *   **Schedule**: `0 2 * * 0` (At 2:00 AM on Sunday).
    *   **Action**: Runs `weekly_cleanup_meetmaxevents.sh` and `weekly_cleanup_logs.sh` to remove old archived files and logs.

*   **Daily Database Backup**:
    *   **Schedule**: `0 3 * * *` (At 3:00 AM every day).
    *   **Action**: Runs `pg_backup_all.sh` to perform a full backup of all PostgreSQL databases.

### How to Edit Cron Jobs

There are two primary ways to manage the cron jobs for this project:

1.  **Manual Editing (for system administrators)**:
    The cron jobs are defined in a system-wide crontab file located at `/etc/cron.d/etl_jobs`. To edit this file directly, use a text editor with root privileges:
    ```bash
    sudo nano /etc/cron.d/etl_jobs
    ```
    After saving the file, the cron daemon will automatically apply the changes. No service restart is needed.

2.  **Automated Updates via Database (Recommended)**:
    The project is designed to manage schedules dynamically. The `update_cron_jobs.py` script reads schedule configurations from the `dba.tscheduler` and `dba.treportmanager` tables in the database and automatically generates the `/etc/cron.d/etl_jobs` file.

    To add or modify a job, you should update the corresponding entry in the database tables. Then, you can run the update script manually to apply the changes:
    ```bash
    # This command must be run with sufficient privileges to write to /etc/cron.d/
    sudo /bin/bash /home/yostfundsadmin/client_etl_workflow/jobscripts/run_python_etl_script.sh update_cron_jobs.py
    ```
    This approach is safer and ensures that the cron configuration stays in sync with the application's database.
