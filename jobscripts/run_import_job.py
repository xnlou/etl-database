import sys
import os
import subprocess
import time
import uuid
from pathlib import Path
from datetime import datetime
import grp

# Debug logging before imports
debug_log_path = Path.home() / 'client_etl_workflow' / 'logs' / f"debug_run_import_job_{datetime.now().strftime('%Y%m%dT%H%M%S')}.log"
try:
    with open(debug_log_path, 'w') as f:
        f.write(f"[{datetime.now()}] Script started\n")
        f.write(f"[{datetime.now()}] Current working directory: {os.getcwd()}\n")
        f.write(f"[{datetime.now()}] sys.path before append: {sys.path}\n")
    os.chmod(debug_log_path, 0o660)
    try:
        group_id = grp.getgrnam('etl_group').gr_gid
        os.chown(debug_log_path, os.getuid(), group_id)
    except KeyError:
        print(f"Warning: Group 'etl_group' not found; skipping chown for {debug_log_path}")
except Exception as e:
    print(f"Failed to write debug log: {e}")

# Add root directory to sys.path
sys.path.append(str(Path.home() / 'client_etl_workflow'))

try:
    from systemscripts.user_utils import get_username
    from systemscripts.log_utils import log_message
    from systemscripts.directory_management import LOG_DIR, ensure_directory_exists
except ImportError as e:
    with open(debug_log_path, 'a') as f:
        f.write(f"[{datetime.now()}] ImportError: {e}\n")
        f.write(f"[{datetime.now()}] sys.path after append: {sys.path}\n")
    raise

def run_import_job(config_id):
    """Wrapper script to run generic_import.py with a specific config_id."""
    script_start_time = time.time()
    run_uuid = str(uuid.uuid4())
    user = get_username()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"run_import_job_{timestamp}"

    # Ensure log directory exists
    ensure_directory_exists(LOG_DIR)

    log_message(log_file, "Initialization", f"Script started at {timestamp} for config_id {config_id}",
                run_uuid=run_uuid, stepcounter="Initialization_0", user=user, script_start_time=script_start_time)

    # Path to generic_import.py
    system_script = Path.home() / 'client_etl_workflow' / 'systemscripts' / 'generic_import.py'

    if not system_script.exists():
        log_message(log_file, "Error", f"System script {system_script} not found",
                    run_uuid=run_uuid, stepcounter="Initialization_1", user=user, script_start_time=script_start_time)
        return

    # Run the system script using the virtual environment
    venv_python = Path.home() / 'client_etl_workflow' / 'venv' / 'bin' / 'python'
    cmd = [str(venv_python), str(system_script), str(config_id)]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            log_message(log_file, "Execution", f"Successfully executed generic_import.py for config_id {config_id}",
                        run_uuid=run_uuid, stepcounter="Execution_0", user=user, script_start_time=script_start_time)
        else:
            log_message(log_file, "Error", f"Failed to execute generic_import.py: {result.stderr}",
                        run_uuid=run_uuid, stepcounter="Execution_1", user=user, script_start_time=script_start_time)
    except Exception as e:
        log_message(log_file, "Error", f"Exception running generic_import.py: {str(e)}",
                    run_uuid=run_uuid, stepcounter="Execution_2", user=user, script_start_time=script_start_time)

    log_message(log_file, "Finalization", f"Completed job for config_id {config_id}",
                run_uuid=run_uuid, stepcounter="Finalization_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_import_job.py <config_id>")
        sys.exit(1)
    config_id = int(sys.argv[1])
    run_import_job(config_id)