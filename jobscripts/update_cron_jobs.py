import sys
from pathlib import Path
sys.path.append(str(Path.home() / 'client_etl_workflow'))  # Add repository root to sys.path
import psycopg2
from systemscripts.db_config import DB_PARAMS  # Import centralized DB config

# Connect to PostgreSQL database
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Fetch active reports from dba.treportmanager
cur.execute("SELECT reportID, frequency FROM dba.treportmanager WHERE datastatusid = 1")
report_schedules = cur.fetchall()

# Fetch active tasks from dba.tscheduler
cur.execute("SELECT schedulerID, taskname, frequency, scriptpath, scriptargs FROM dba.tscheduler WHERE datastatusid = 1")
task_schedules = cur.fetchall()

# Generate cron file directly in /etc/cron.d/etl_jobs
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

cur.close()
conn.close()
print(f"Cron jobs updated in {cron_file}")