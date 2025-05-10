import csv
import uuid
import time
from datetime import datetime
import threading
import os

# Global counter and lock for log_id
_log_id_counter = 0
_log_id_lock = threading.Lock()
_last_log_time = time.time()
_log_time_lock = threading.Lock()

def log_message(log_file, process_type, message, **kwargs):
    """Log a message to CSV and TXT files with configurable fields."""
    global _log_id_counter, _last_log_time
    current_time = time.time()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Handle log_id
    with _log_id_lock:
        log_id = _log_id_counter
        _log_id_counter += 1
    
    # Handle run_uuid
    run_uuid = kwargs.get("run_uuid", str(uuid.uuid4()))
    
    # Handle stepcounter
    stepcounter = kwargs.get("stepcounter", "")
    
    # Handle user
    user = kwargs.get("user", "")
    
    # Handle runtimes
    with _log_time_lock:
        step_runtime = current_time - _last_log_time
        _last_log_time = current_time
    total_runtime = current_time - kwargs.get("script_start_time", current_time)
    
    # Define log entry
    log_entry = {
        "log_id": log_id,
        "timestamp": timestamp,
        "run_uuid": run_uuid,
        "process_type": process_type,
        "stepcounter": stepcounter,
        "user": user,
        "step_runtime": f"{step_runtime:.3f}",
        "total_runtime": f"{total_runtime:.3f}",
        "message": message
    }
    
    # Define column order for CSV
    column_order = [
        "log_id",
        "timestamp",
        "run_uuid",
        "process_type",
        "stepcounter",
        "user",
        "step_runtime",
        "total_runtime",
        "message"
    ]
    
    # Write to CSV
    csv_file = f"{log_file}.csv"
    with open(csv_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=column_order)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow({k: log_entry[k] for k in column_order})
    os.chmod(csv_file, 0o660)
    
    # Write to TXT
    txt_file = f"{log_file}.txt"
    txt_entry = (
        f"{log_entry['run_uuid']} | {log_entry['timestamp']} | {log_entry['process_type']} | {log_entry['message']}\n"
    )
    with open(txt_file, "a") as f:
        # Write header if file is empty
        if f.tell() == 0:
            f.write("run_uuid | timestamp | process_type | message\n")
            f.write("------------------------------------|---------------------|------------------|--------------------------------\n")
        f.write(txt_entry)
    os.chmod(txt_file, 0o660)