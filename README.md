# Client ETL Workflow Project

## Project Overview
The **Client ETL Workflow Project** establishes an automated Extract, Transform, Load (ETL) pipeline on a Linux Mint Desktop. Designed for a homelab environment, the pipeline automates data ingestion from web scraping and email attachments, processes the data, loads it into a PostgreSQL database, and sends out reports.

The entire workflow is designed to be highly modular and configurable directly from the database, minimizing the need for code changes for routine operational adjustments.

### Key Functionalities
- **Multiple Data Sources**: Ingests data from web scraping (e.g., MeetMax) and by processing email attachments from a Gmail inbox using the Gmail API with OAuth2.
- **Database-Driven Configuration**: All major processes



email processing, file import, and report generation

are controlled by configuration tables in a PostgreSQL database.
- **Automated Processing**: Converts various file formats (XLS/XLSX to CSV), transforms data using pandas, and loads it into the database.
- **Scheduled Operations**: Uses cron for scheduling all automated tasks, with the cron configuration itself being dynamically generated from the database.
- **Secure and Robust**: Implements secure authentication for Gmail (OAuth2), enforces strict file permissions, and provides detailed logging to both files and the database for traceability.

---

## Core Concepts: A Database-Driven Pipeline
This ETL pipeline is not configured through static files (`.json`, `.ini`, etc.). Instead, its behavior is defined by records in the PostgreSQL database. Understanding this concept is key to managing and extending the workflow.

The primary configuration tables are located in the `dba` schema:
- **`dba.tinboxconfig`**: Defines rules for processing emails from a Gmail inbox. You can specify subject lines, attachment name patterns, and where to save the files.
- **`dba.timportconfig`**: Defines how to process files that land in the `file_watcher/` directory. It specifies the file pattern to look for, the target database table, and the import strategy.
- **`dba.treportmanager`**: Configures email reports. Each record defines the recipients, subject, body (including SQL queries to generate data grids), and schedule.
- **`dba.tscheduler`**: Manages the schedules for miscellaneous cron jobs that are not reports.

**To modify the pipeline's behavior (e.g., process a new type of email attachment or run a new script), you will typically add or update a row in one of these tables.**

---

## How It Works: Data Flow
The pipeline operates as a sequence of automated steps, orchestrated by cron and configured by the database.

**Step 1: Data Ingestion (Getting files into `file_watcher/`)**
This happens in one of two ways, both running on independent schedules:
- **A) Gmail Processing**:
    1. The `run_gmail_inbox_processor.py` script is executed by cron.
    2. It queries `dba.tinboxconfig` for all active rules.
    3. It scans the configured Gmail inbox.
    4. For emails matching a rule's subject and attachment patterns, it downloads the `.eml` file and the attachment(s) directly into the `file_watcher/` directory. Filenames are prefixed with the email's sent date (`yyyyMMdd`).
    5. The email is moved to the 'Processed' label in Gmail. Any email that does not match a rule is moved to the 'ErrorFolder' label to keep the inbox clean.
- **B) Web Scraping**:
    1. The `meetmax_url_download.py` script is executed by cron.
    2. It downloads XLS files from MeetMax and saves them in the `file_watcher/` directory.

**Step 2: Generic File Import**
1. The `run_import_job.py` script is executed by cron, with a `config_id` as an argument.
2. This script calls `generic_import.py`, which reads the corresponding configuration from `dba.timportconfig`.
3. It finds the matching file(s) in the `file_watcher/` directory based on the `file_pattern` in the configuration.
4. If the file is an XLS/XLSX, it first calls `xls_to_csv.py` to convert it.
5. It then loads the CSV data into the `target_table` specified in the configuration, creating or altering table columns if the import strategy allows.
6. After a successful import, the source file is moved to the `archive/` directory.

**Step 3: Reporting**
1. The `send_reports.py` script is executed by cron with a `reportID`.
2. It reads the configuration from `dba.treportmanager`.
3. It executes the SQL queries defined in the configuration to generate data for the email body.
4. It sends the final report via email.

---

## Installation and Setup Guide
Follow these steps to set up the ETL pipeline on a new Linux Mint machine.

1.  **Configure User and Directories**:
    - Run the user setup script. This will create the `etl_user`, `etl_group`, and the project directory structure.
      ```bash
      chmod +x onboarding/sh/configure_etl_user.sh
      ./onboarding/sh/configure_etl_user.sh
      ```

2.  **Install Dependencies**:
    - Run the dependency installer. This will install PostgreSQL, Python, and all required system and Python packages.
      ```bash
      chmod +x onboarding/sh/install_dependencies.sh
      ./onboarding/sh/install_dependencies.sh
      ```

3.  **Set Environment Variables**:
    - Edit `env.sh` to set the passwords and email credentials for your environment.
      ```bash
      nano env.sh
      ```

4.  **Set Up the Database**:
    - Run the SQL setup script. This will create the database, schemas, and all necessary tables and functions.
      ```bash
      chmod +x onboarding/sh/sql_setupscripts.sh
      ./onboarding/sh/sql_setupscripts.sh
      ```

5.  **Configure Google API Credentials**:
    - Go to the Google Cloud Console and create a new project.
    - Enable the **Gmail API**.
    - Create an **OAuth 2.0 Client ID** for a **Desktop app**.
    - Download the credentials JSON file and save it as `systemscripts/credentials.json`.

6.  **Generate Initial Gmail API Token**:
    - The first time you run a script that accesses the Gmail API, you will need to authorize it. The `test_oauth_gmail_headless.py` script is designed for this.
    - Run it from the command line. It will print a URL.
      ```bash
      /bin/bash jobscripts/run_python_etl_script.sh test_oauth_gmail_headless.py
      ```
    - Copy the URL into a browser, sign in to your Google account, and grant permission.
    - You will be redirected to a non-working `localhost` page. **Copy the entire URL from your browser's address bar** and paste it back into the terminal when prompted.
    - This will generate a `systemscripts/token.json` file, which will be used for all future authentications. The script will handle refreshing the token automatically.

---

## Configuration Guide
To change how the pipeline works, you will typically modify the database tables.

- **To process a new email attachment**:
  - Add a new row to `dba.tinboxconfig`.
  - **`config_name`**: A unique name (e.g., 'NewClientReport').
  - **`subject_pattern`**: A regex pattern to match the email subject.
  - **`has_attachment`**: `TRUE`.
  - **`attachment_name_pattern`**: A regex pattern to match the attachment's filename (e.g., `'.*\\.csv$'` for any CSV file).
  - **`local_repository_path`**: The directory to save the files in (usually `/home/yostfundsadmin/client_etl_workflow/file_watcher/`).

- **To import a new file type**:
  - First, ensure the file is being placed in the `file_watcher/` directory (either by the Gmail processor or another process).
  - Add a new row to `dba.timportconfig`.
  - **`config_name`**: A unique name (e.g., 'ImportNewClientReport').
  - **`file_pattern`**: A regex pattern to match the file saved by the inbox processor (e.g., `^\\d{8}_NewClientReport\\.csv$`).
  - **`target_table`**: The destination table in the database (e.g., `public.tnewclientreport`).
  - **`importstrategyid`**: `1` if you want the script to automatically add new columns to the table if they appear in the source file.

- **To create a new scheduled job**:
  - Add a new row to `dba.tscheduler` for general scripts or `dba.treportmanager` for reports.
  - Define the cron `frequency` and the script to be run.
  - After adding the schedule to the database, you must regenerate the system's cron file by running:
    ```bash
    sudo /bin/bash /home/yostfundsadmin/client_etl_workflow/jobscripts/run_python_etl_script.sh update_cron_jobs.py
    ```

---

## File Descriptions
A brief overview of the most important scripts.

### `jobscripts/`
- **`run_gmail_inbox_processor.py`**: The main entry point for the email processing workflow. It should be scheduled in cron to run periodically.
- **`run_import_job.py`**: The entry point for the file import workflow. It takes a `config_id` from `dba.timportconfig` as an argument.
- **`run_python_etl_script.sh`**: A critical wrapper script that sets up the virtual environment and environment variables before executing any Python script. **All cron jobs should use this wrapper.**
- **`test_oauth_gmail_headless.py`**: A utility script used once during setup to generate the initial `token.json` for Gmail API access.

### `systemscripts/`
- **`gmail_inbox_processor.py`**: Contains the core logic for connecting to Gmail, reading emails, matching them against `dba.tinboxconfig`, and downloading files.
- **`generic_import.py`**: Contains the core logic for reading files from the `file_watcher/` directory and importing them into the database based on rules in `dba.timportconfig`.
- **`xls_to_csv.py`**: A utility script called by `generic_import.py` to handle XLS/XLSX to CSV conversion.


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
*   `run_gmail_inbox_processor.py`: Wrapper to run Gmail inbox processing for a specific `config_id`.
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
*   `create_inboxconfig_table.sql`: Creates and populates the `dba.tinboxconfig` table to configure Gmail inbox processing rules.
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
*   `gmail_inbox_processor.py`: Processes Gmail emails based on database configurations, downloading matching emails and attachments.
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

### Email API Configuration and Usage

The pipeline supports both inbound email processing (monitoring a Gmail inbox for matching emails) and outbound reporting (sending automated emails with data summaries or attachments). Inbound uses the Gmail API for secure access, while outbound uses SMTP (e.g., via Gmail).

#### Inbound Email Processing (Gmail API)
- **Purpose**: Monitors a Gmail inbox for emails matching rules in `dba.tinboxconfig` (e.g., specific subjects, senders, or attachments). Downloads raw email content and attachments to `file_watcher/` for ETL processing, then moves emails to "Processed" or "ErrorFolder" labels.
- **Scripts Involved**: `gmail_inbox_processor.py` (core logic), `run_gmail_inbox_processor.py` (wrapper for config_id execution).
- **Setup Steps**:
  1. Enable the Gmail API in the Google Cloud Console: Go to [Google APIs Console](https://console.developers.google.com/), create a project, enable "Gmail API", and download `credentials.json` (OAuth 2.0 Client IDs).
  2. Place `credentials.json` in `systemscripts/` (template provided).
  3. Install dependencies: Run `onboarding/sh/gmail_api.sh` to install `google-api-python-client`, `google-auth-oauthlib`, and `google-auth-httplib2`.
  4. On first run, authenticate: Execute `gmail_inbox_processor.py`—it will open a browser for OAuth consent, generating `token.json` for future sessions.
  5. Configure rules in `dba.tinboxconfig`: Insert rows with filters like `subject_contains` (e.g., "MeetMax Update"), `has_attachment` (true/false), and target directories.
- **Security Notes**: Use OAuth for read-only access (scope: `https://mail.google.com/`). Avoid hardcoding credentials; refresh tokens handle expiration.
- **Troubleshooting**: Check logs for OAuth errors. Test with `process_inbox.py` for manual runs.

#### Outbound Email Reporting (SMTP)
- **Purpose**: Sends reports based on `dba.treportmanager` (e.g., SQL query results as CSV attachments, job status summaries).
- **Scripts Involved**: `send_reports.py` (executes queries and sends emails), `testemail.py` (for validation).
- **Setup Steps**:
  1. Configure SMTP in scripts: Use Gmail (host: `smtp.gmail.com`, port: 587) with app passwords (enable 2FA and generate via Google Account settings).
  2. Populate `dba.treportmanager`: Define `reportID`, `query` (e.g., SELECT from tmeetmax), `recipients`, and `attachment_format` (CSV/XLS).
  3. Schedule via cron: Reports are triggered by `update_cron_jobs.py` based on database schedules.
- **Example**: A report might attach a CSV of new MeetMax events, with body text like "ETL job completed successfully."
- **Security Notes**: Use TLS (starttls) and app-specific passwords. Avoid plain-text credentials in code.

### Visual Overview

To better illustrate the ETL workflow, here's a high-level flowchart using Mermaid syntax (viewable in GitHub or Markdown renderers like Typora). It shows the sequence from data extraction to reporting.

```mermaid
flowchart TD
    A[Start: Cron Trigger] --> B[Extract: MeetMax URL Check/Download]
    B --> C[Process Inbox: Gmail API for Emails/Attachments]
    C --> D[Convert: XLS/XLSX to CSV]
    D --> E[Transform/Load: Generic Import to PostgreSQL]
    E --> F[Report: Send Emails via SMTP]
    F --> G[Archive/Cleanup: Move Files, Log Entries]
    G --> H[End: Secure File Exchange via SFTP]
    
    subgraph "Inbound Email"
    C
    end
    
    subgraph "Database Operations"
    E
    end
    
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style H fill:#bbf,stroke:#333,stroke-width:2px