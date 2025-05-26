import psycopg2
import os

# Database connection parameters
DB_PARAMS = {
    "dbname": "feeds",
    "user": "yostfundsadmin",
    "password": os.getenv("DB_PASSWORD", "etlserver2025!"),
    "host": "localhost",
    "port": "5432"
}

# Connect to PostgreSQL database
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Fetch active reports from dba.treportmanager
cur.execute("SELECT reportID, frequency FROM dba.treportmanager WHERE datastatusid = 1")
report_schedules = cur.fetchall()

# Fetch active tasks from dba.tscheduler
cur.execute("SELECT schedulerID, taskname, frequency, scriptpath, scriptargs FROM dba.tscheduler WHERE datastatusid = 1")
task_schedules = cur.fetchall()

# Generate cron file
cron_file = "/etc/cron.d/etl_jobs"
with open(cron_file, 'w') as f:
    # Add cron jobs for reports
    for report_id, frequency in report_schedules:
        cron_line = f"{frequency} etl_user /usr/bin/python3 /home/yostfundsadmin/client_etl_workflow/jobscripts/send_reports.py {report_id} >> /home/yostfundsadmin/client_etl_workflow/logs/send_reports.log 2>&1\n"
        f.write(cron_line)

    # Add cron jobs for other tasks
    for scheduler_id, taskname, frequency, scriptpath, scriptargs in task_schedules:
        scriptargs = scriptargs if scriptargs else ""
        cron_line = f"{frequency} etl_user /usr/bin/python3 {scriptpath} {scriptargs} >> /home/yostfundsadmin/client_etl_workflow/logs/{taskname}.log 2>&1\n"
        f.write(cron_line)

# Set permissions on the cron file
os.chmod(cron_file, 0o644)
os.chown(cron_file, 0, 0)  # root:root

cur.close()
conn.close()
print(f"Cron jobs updated in {cron_file}")