import os


# Directory Management Module

# Root directory for all ETL workflows
ROOT_DIR = '/home/yostfundsadmintest1/etl_workflow'

# Directory for file watcher
FILE_WATCHER_DIR = os.path.join(ROOT_DIR, 'file_watcher')

# Directory for log files
LOG_DIR = os.path.join(ROOT_DIR, 'logs')

# Directory for archived files
ARCHIVE_DIR = os.path.join(ROOT_DIR, 'archive')

# Directory for job scripts
JOB_SCRIPTS_DIR = os.path.join(ROOT_DIR, 'jobscripts')

# Directory for system scripts
SYSTEM_SCRIPTS_DIR = os.path.join(ROOT_DIR, 'systemscripts')

def ensure_directory_exists(directory):
    """
    Ensure that the specified directory exists, creating it if it does not.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        os.chmod(directory, 0o770)  # Set permissions to rwxrwx---

def initialize_directories():
    """
    Initialize all necessary directories for the ETL workflows.
    """
    ensure_directory_exists(ROOT_DIR)
    ensure_directory_exists(FILE_WATCHER_DIR)
    ensure_directory_exists(LOG_DIR)
    ensure_directory_exists(ARCHIVE_DIR)
    ensure_directory_exists(JOB_SCRIPTS_DIR)
    ensure_directory_exists(SYSTEM_SCRIPTS_DIR)

# Example usage
if __name__ == "__main__":
    initialize_directories()
    print(f"Root Directory: {ROOT_DIR}")
    print(f"File Watcher Directory: {FILE_WATCHER_DIR}")
    print(f"Log Directory: {LOG_DIR}")
    print(f"Archive Directory: {ARCHIVE_DIR}")
    print(f"Job Scripts Directory: {JOB_SCRIPTS_DIR}")
    print(f"System Scripts Directory: {SYSTEM_SCRIPTS_DIR}")