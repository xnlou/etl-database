import sys
sys.path.append('/home/yostfundsadmin/client_etl_workflow')  # Add repository root to sys.path
import psycopg2
import os
from systemscripts.db_config import DB_PARAMS  # Import centralized DB config
import grp

# Connect to PostgreSQL database
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Fetch active reports from dba.treportmanager
cur.execute("SELECT reportID, frequency FROM dba.treportmanager WHERE datastatusid = 1")
report_schedules = cur.fetchall()

# Fetch active tasks from dba.tscheduler
cur.execute("SELECT schedulerID, taskname, frequency, scriptpath, scriptargs FROM dba.tscheduler WHERE datastatusid = 1")
task_schedules = cur.fetchall()

# Generate cron file in /etc/cron.d/etl_jobs (requires sudo)
cron_file = "/etc/cron.d/etl_jobs"
with open(cron_file, 'w') as f:
    # Add environment sourcing and cron jobs for reports
    for report_id, frequency in report_schedules:
        cron_line = f"{frequency} etl_user PATH=/usr/local/bin:/usr/bin:/bin /bin/bash /home/yostfundsadmin/client_etl_workflow/jobscripts/run_python_etl_script.sh send_reports.py {report_id} >> /home/yostfundsadmin/client_etl_workflow/logs/etl_cron.log 2>&1\n"
        f.write(cron_line)

    # Add environment sourcing and cron jobs for other tasks
    for scheduler_id, taskname, frequency, scriptpath, scriptargs in task_schedules:
        scriptargs = scriptargs if scriptargs else ""
        script_name = os.path.basename(scriptpath)
        cron_line = f"{frequency} etl_user PATH=/usr/local/bin:/usr/bin:/bin /bin/bash /home/yostfundsadmin/client_etl_workflow/jobscripts/run_python_etl_script.sh {script_name} {scriptargs} >> /home/yostfundsadmin/client_etl_workflow/logs/etl_cron.log 2>&1\n"
        f.write(cron_line)

# Set permissions and ownership on the cron file
os.chmod(cron_file, 0o644)
try:
    group_id = grp.getgrnam('cron_etl').gr_gid
    os.chown(cron_file, 0, group_id)
except KeyError:
    print(f"Warning: Group 'cron_etl' not found; skipping chown for {cron_file}")

cur.close()
conn.close()
print(f"Cron jobs updated in {cron_file}")