# ETL Pipeline Project Tasks

## Objective
Develop an automated ETL pipeline to scrape data, process client XLSX/XLS files, load data into a PostgreSQL database, and send email reports, all within a secure homelab environment on Linux Mint Desktop.

## Tasks

1. **Set Up Environment**
   - Install and configure Linux Mint Desktop with required dependencies (e.g., Python 3.12, PostgreSQL, Git, cron, SSH).
   - Create a virtual environment and install Python packages (e.g., pandas, openpyxl, psycopg2, Scrapy, smtplib).
   - Configure user permissions (etl_user, etl_group) and directory structure for ETL workflows.

2. **Configure PostgreSQL**
   - Set up PostgreSQL with a dedicated database (e.g., "Feeds") and user (e.g., yostfundsadmin).
   - Implement authentication (md5) and grant necessary privileges (e.g., CREATEDB).
   - Create logging and configuration tables for tracking ETL jobs and data imports.

3. **Develop MeetMax URL Checker**
   - Write a Python script (`meetmax_url_check.py`) to scrape MeetMax event URLs (e.g., event IDs 119179–119184).
   - Check for valid events, downloadable XLS files, and private/public status.
   - Save results to a CSV file with details (EventID, URL, IfExists, IsDownloadable, DownloadLink, StatusCode, Title).

4. **Develop MeetMax URL Downloader**
   - Create a Python script (`meetmax_url_download.py`) to download XLS files from URLs identified by the URL checker.
   - Handle retries, rate limits, and errors with conservative threading (e.g., max_workers=1).
   - Save downloaded files to the file_watcher directory and log results to a CSV.

5. **Implement XLS to CSV Converter**
   - Develop a Python script (`xls_to_csv.py`) to convert XLS/XLSX files to CSV using openpyxl (and xlrd for legacy .xls).
   - Ensure output CSVs are saved in the same directory with appropriate permissions (e.g., 0o660, etl_group ownership).
   - Log conversion status and errors to files and PostgreSQL.

6. **Create Dataset-Driven Tracking Table**
   - Design a PostgreSQL table (e.g., `dba.tDataLoads`) to track data load metadata (e.g., job ID, filename, load status, timestamp).
   - Ensure the table supports querying and reporting for ETL job monitoring.

7. **Develop Generic Feeds Data Import Configuration**
   - Create a PostgreSQL configuration table (e.g., `dba.tImportConfig`) to store job details:
     - Filename patterns (regex).
     - Source and destination directories (e.g., move from file_watcher to processing directory).
     - File type (e.g., XLS, XLSX, CSV).
     - Conversion requirements (e.g., XLS/XLSX to CSV).
   - Ensure the table supports dynamic ETL job execution.

8. **Implement File Watcher Script**
   - Write a Python script to monitor the file_watcher directory for new files.
   - Query the `tImportConfig` table to determine file processing rules (e.g., move location, conversion needs).
   - Move files to the specified processing directory and log actions.

9. **Develop Generic Data Import Script**
   - Create a Python script to process files from the processing directory based on `tImportConfig` settings.
   - Handle file conversion (XLS/XLSX to CSV) if required.
   - Load CSV data into PostgreSQL tables using psycopg2, ensuring data integrity and error handling.
   - Log import status to the PostgreSQL logging table.

10. **Enhance Logging to PostgreSQL**
    - Modify all scripts to log directly to a PostgreSQL table (e.g., `dba.tLogEntry`) with fields:
      - log_id, timestamp, run_uuid, process_type, stepcounter, user, step_runtime, total_runtime, message.
    - Ensure fallback logging to CSV/TXT files in case of database errors.
    - Maintain consistent permissions (e.g., 0o660) for log files.

11. **Configure Automation**
    - Set up cron jobs for the etl_user to schedule weekly ETL pipeline runs (e.g., Monday at 2 AM).
    - Configure Bash scripts (e.g., `run_python_etl_script.sh`) to execute Python scripts within the virtual environment.
    - Ensure cron logs are saved to the logs directory for monitoring.

12. **Implement Email Reporting**
    - Develop a Python script using smtplib to send email notifications to clients.
    - Include job status (success/failure) and data summaries (e.g., number of records loaded).
    - Use a secure SMTP server (e.g., Gmail, Postfix) with SSL/TLS.

13. **Set Up Secure File Exchange**
    - Configure SFTP or cloud-based file sharing for client file uploads/downloads.
    - Ensure strong authentication and encryption (e.g., SSL/TLS).
    - Integrate with the file_watcher directory for seamless processing.

14. **Version Control and Documentation**
    - Maintain all code, scripts, and configurations in a Git repository.
    - Provide comprehensive documentation for setup, execution, and maintenance.
    - Include initial PowerShell scripts (to be migrated to Python/Bash) for reference.

## Assumptions
- **Data Volume**: ~1–10 MB per run.
- **Scraping Complexity**: Simple-to-moderate (static websites).
- **Input Formats**: XLSX/XLS, converted to CSV.
- **Hardware**: Linux Mint, 16GB RAM, 500GB storage, stable internet.
- **Security**: SSL/TLS for database and email, strong authentication for file exchange.
- **Maintenance**: Minimal, using open-source tools.
- **Frequency**: Weekly ETL runs with email reports.

## Deliverables
- Automated ETL pipeline for scraping, conversion, transformation, and loading.
- PostgreSQL database with logging and configuration tables.
- Email notification system for clients.
- Secure file exchange mechanism.
- Git repository with code, scripts, and documentation.