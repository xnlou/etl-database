# ClientConferenceScraper

20250505
Run Scripts in Order: Execute install_dependencies.sh, configure_etl_user.sh, setup_logging.sh, directory_management.py, and test run_etl.sh manually.
Create ETL Scripts: Develop scrape.py, convert_xlsx.py, load_to_db.py, and send_email.py based on your requirements.
Set Up SFTP: Configure SFTP for yostfundsadmin to allow client file uploads to /home/yostfundsadmin/etl_workflow/file_watcher/.
Monitor Cron: Ensure cron jobs run as etl_user and logs are written correctly.