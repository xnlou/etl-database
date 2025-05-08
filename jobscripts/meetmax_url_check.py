import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import threading
import requests
import re
import pwd
from pathlib import Path
import pandas as pd
from datetime import datetime
import time
import csv
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from systemscripts.directory_management import LOG_DIR, FILE_WATCHER_DIR, ensure_directory_exists

# Define constants
BASE_URL = "https://www.meetmax.com/sched/event_{}/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_RETRIES = 5
INITIAL_DELAY = 5.0
TASK_SUBMISSION_DELAY = 0.5
PERIODIC_INTERVAL = 60

# Define directories
FILE_WATCHER_TEMP_DIR = FILE_WATCHER_DIR / "file_watcher_temp"

# Ensure directories exist
ensure_directory_exists(LOG_DIR)
ensure_directory_exists(FILE_WATCHER_DIR)
ensure_directory_exists(FILE_WATCHER_TEMP_DIR)

# Define Event IDs range
event_ids = range(70841, 70950)

# Global lock and variables
results_lock = threading.Lock()
results = []
stop_event = threading.Event()
script_start_time = time.time()
last_log_time = script_start_time  # Track the last log time globally
log_id_counter = 0  # Auto-incrementing log ID
log_time_lock = threading.Lock()  # Lock for thread-safe access to last_log_time
run_uuid = str(uuid.uuid4())
process_counters = {}  # Track counters for non-event process types

def get_username():
    """Get the current username using multiple methods."""
    log_messages = []
    
    # Try environment variable SCRIPT_USER first
    script_user = os.environ.get('SCRIPT_USER')
    if script_user:
        log_messages.append(f"Username retrieved via SCRIPT_USER: {script_user}")
        return script_user, log_messages
    
    # Try pwd.getpwuid with os.getuid()
    try:
        username = pwd.getpwuid(os.getuid()).pw_name
        log_messages.append(f"Username retrieved via pwd.getpwuid: {username}")
        return username, log_messages
    except Exception as e:
        log_messages.append(f"pwd.getpwuid() failed: {str(e)}")

    # Try os.getlogin()
    try:
        username = os.getlogin()
        log_messages.append(f"Username retrieved via getlogin: {username}")
        return username, log_messages
    except OSError as e:
        log_messages.append(f"os.getlogin() failed: {str(e)}")

    # Try USER or LOGNAME environment variables
    username = os.environ.get('USER') or os.environ.get('LOGNAME')
    if username:
        log_messages.append(f"Username retrieved via USER/LOGNAME: {username}")
        return username, log_messages

    # Fallback to 'unknown'
    log_messages.append("All username retrieval methods failed, using 'unknown'")
    return "unknown", log_messages

def log_message(log_file, process_type, message, event_id=None):
    """Log a message to both a CSV and a TXT file with step_runtime as time between consecutive log_ids."""
    global log_id_counter, last_log_time, process_counters
    current_time = time.time()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_runtime = current_time - script_start_time
    
    with log_time_lock:
        step_runtime = current_time - last_log_time  # Time since the last log entry
        last_log_time = current_time  # Update last log time
        log_id = log_id_counter  # Assign current log_id
        log_id_counter += 1  # Increment for next entry

        # Determine stepcounter based on URL for events, process type for others
        if process_type == "EventProcessing" and event_id is not None:
            # Use event_id from the URL (e.g., https://www.meetmax.com/sched/event_{event_id}/)
            stepcounter = f"event_{event_id}"
        else:
            if process_type not in process_counters:
                process_counters[process_type] = 0
            else:
                process_counters[process_type] += 1
            stepcounter = f"{process_type}_{process_counters[process_type]}"
    
    log_entry = {
        "log_id": log_id,
        "timestamp": timestamp,
        "run_uuid": run_uuid,
        "process_type": process_type,
        "stepcounter": stepcounter,
        "user": user_cache,
        "step_runtime": f"{step_runtime:.3f}",
        "total_runtime": f"{total_runtime:.3f}",
        "message": message
    }
    
    # Define column order
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
    file_exists = os.path.exists(log_file)
    with open(log_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=column_order)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: log_entry[k] for k in column_order})
    os.chmod(log_file, 0o660)

    # Write to TXT
    txt_log_file = log_file.with_suffix('.txt')
    txt_entry = (
        f"Log ID: {log_entry['log_id']}\t"
        f"Timestamp: {log_entry['timestamp']}\t"
        f"Run UUID: {log_entry['run_uuid']}\t"
        f"Process Type: {log_entry['process_type']}\t"
        f"Stepcounter: {log_entry['stepcounter']}\t"
        f"User: {log_entry['user']}\t"
        f"Step Runtime: {log_entry['step_runtime']}\t"
        f"Total Runtime: {log_entry['total_runtime']}\t"
        f"Message: {log_entry['message']}\n"
    )
    with open(txt_log_file, "a") as f:
        f.write(txt_entry)
    os.chmod(txt_log_file, 0o660)

def fetch_url(session, url, event_id, log_file, headers=None):
    """Fetch a URL with retry logic and exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=15, headers=headers)
            status_code = response.status_code
            log_message(log_file, "EventProcessing", f"Attempt {attempt + 1} for EventID {event_id}: Status {status_code}", event_id=event_id)
            if status_code == 429:
                retry_after = response.headers.get('Retry-After', 10)
                log_message(log_file, "EventProcessing", f"Rate-limited, retrying after {retry_after}s", event_id=event_id)
                time.sleep(float(retry_after))
                continue
            response.raise_for_status()
            log_message(log_file, "EventProcessing", f"Response length: {len(response.text)} bytes", event_id=event_id)
            return response.text, status_code
        except requests.RequestException as e:
            status_code = e.response.status_code if e.response else "Unknown"
            log_message(log_file, "Error", f"Error fetching {url} (Attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}", event_id=event_id)
            if attempt < MAX_RETRIES - 1:
                delay = INITIAL_DELAY * (2 ** attempt)
                log_message(log_file, "EventProcessing", f"Retrying after {delay}s", event_id=event_id)
                time.sleep(delay)
            else:
                log_message(log_file, "Error", f"Failed after {MAX_RETRIES} attempts", event_id=event_id)
                return None, "Failed"
    return None, "MaxRetriesExceeded"

def process_event(event_id, log_file):
    """Process a single event ID."""
    log_message(log_file, "EventProcessing", f"Starting processing for EventID {event_id}", event_id=event_id)
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    })

    public_url = BASE_URL.format(event_id) + "__co-list_cp.html"
    private_url = BASE_URL.format(event_id) + "__private-co-list_cp.html"
    url_used = public_url
    is_private = False
    is_downloadable = 0
    download_link = ""
    if_exists = 0
    invalid_event_id = False
    status_code = None
    title = ""

    try:
        public_response, public_status = fetch_url(session, public_url, event_id, log_file)
        if public_response is None:
            log_message(log_file, "Error", f"Failed to fetch public page, Status: {public_status}", event_id=event_id)
            return {
                "EventID": event_id,
                "URL": url_used,
                "IfExists": 0,
                "InvalidEventID": False,
                "IsDownloadable": 0,
                "DownloadLink": "",
                "StatusCode": public_status,
                "Title": ""
            }

        status_code = public_status
        private_match = re.search(r'<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>', public_response, re.IGNORECASE)
        if private_match:
            is_private = True
            url_used = private_url
            log_message(log_file, "EventProcessing", f"Private site indicator found", event_id=event_id)

        response_text = public_response
        if is_private:
            response_text, status_code = fetch_url(session, private_url, event_id, log_file, headers={
                "Accept": "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "User-Agent": USER_AGENT
            })
            if response_text is None:
                log_message(log_file, "Error", f"Failed to fetch private page, Status: {status_code}", event_id=event_id)
                return {
                    "EventID": event_id,
                    "URL": url_used,
                    "IfExists": 0,
                    "InvalidEventID": False,
                    "IsDownloadable": 0,
                    "DownloadLink": "",
                    "StatusCode": status_code,
                    "Title": ""
                }

        title_match = re.search(r'<title>(.*?)</title>', response_text, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).replace(" - MeetMax", "").strip()

        invalid_match = re.search(r'<div class="alert alert-danger">Invalid Event ID: \d+</div>', response_text, re.IGNORECASE)
        if invalid_match:
            invalid_event_id = True
        else:
            if_exists = 1

        link_match = re.search(r'<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>', response_text, re.IGNORECASE)
        button_match = re.search(r'<[^>]*id="export"[^>]*>.*?<i class="fas fa-cloud-download-alt"> </i>\s*Download Company List', response_text, re.IGNORECASE | re.DOTALL)
        if link_match or button_match:
            is_downloadable = 1
            href = link_match.group(1) if link_match else f"__co-list_cp.xls?event_id={event_id}"
            if "?event_id=" in href:
                base_url, query = href.split("?event_id=", 1)
                event_id_part = query.split(";", 1)[0]
                href = f"{base_url}?event_id={event_id_part}"
            download_link = BASE_URL.format(event_id) + href.lstrip('/') if not href.startswith('http') else href

        result = {
            "EventID": event_id,
            "URL": url_used,
            "IfExists": if_exists,
            "InvalidEventID": invalid_event_id,
            "IsDownloadable": is_downloadable,
            "DownloadLink": download_link,
            "StatusCode": str(status_code),
            "Title": title
        }
        return result

    except Exception as e:
        log_message(log_file, "Error", f"Unexpected error: {str(e)}", event_id=event_id)
        return {
            "EventID": event_id,
            "URL": url_used,
            "IfExists": 0,
            "InvalidEventID": False,
            "IsDownloadable": 0,
            "DownloadLink": "",
            "StatusCode": "Error",
            "Title": ""
        }

def periodic_save(log_file, temp_csv_file):
    """Periodically save results to a temporary CSV file."""
    while not stop_event.is_set():
        log_message(log_file, "PeriodicSave", "Starting save cycle")
        time.sleep(PERIODIC_INTERVAL)
        with results_lock:
            if results:
                df = pd.DataFrame(results)
                df.to_csv(temp_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
                os.chmod(temp_csv_file, 0o660)
                log_message(log_file, "PeriodicSave", f"Saved {len(results)} rows")

def meetmax_url_check():
    """Check MeetMax event URLs and save results to CSV."""
    global results, user_cache, last_log_time, log_id_counter, process_counters
    results = []
    total = len(event_ids)
    event_counter = 0
    log_id_counter = 0
    process_counters = {}

    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"meetmax_url_check_{timestamp}.csv"
    temp_csv_file = FILE_WATCHER_TEMP_DIR / f"{timestamp}_MeetMaxURLCheck.csv"
    final_csv_file = FILE_WATCHER_DIR / f"{timestamp}_MeetMaxURLCheck.csv"

    user_cache, username_logs = get_username()
    for msg in username_logs:
        log_message(log_file, "Initialization", msg)

    log_message(log_file, "Initialization", f"Script started at {timestamp}")
    periodic_thread = threading.Thread(target=periodic_save, args=(log_file, temp_csv_file))
    periodic_thread.start()

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_event = {executor.submit(process_event, event_id, log_file): event_id for event_id in event_ids}
        for future in as_completed(future_to_event):
            event_id = future_to_event[future]
            try:
                result = future.result()
                with results_lock:
                    results.append(result)
                event_counter += 1
            except Exception as e:
                log_message(log_file, "Error", f"Exception processing EventID {event_id}: {str(e)}", event_id=event_id)
                with results_lock:
                    results.append({
                        "EventID": event_id,
                        "URL": BASE_URL.format(event_id) + "__co-list_cp.html",
                        "IfExists": 0,
                        "InvalidEventID": False,
                        "IsDownloadable": 0,
                        "DownloadLink": "",
                        "StatusCode": "Error",
                        "Title": ""
                    })
                event_counter += 1

    log_message(log_file, "Finalization", "Stopping periodic save thread")
    stop_event.set()
    periodic_thread.join()

    if results:
        df = pd.DataFrame(results)
        df.to_csv(final_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        os.chmod(final_csv_file, 0o660)
        log_message(log_file, "FinalSave", f"Wrote {len(results)} rows to {final_csv_file}")

    log_message(log_file, "Finalization", f"Completed: Processed {event_counter}/{total} URLs")

if __name__ == "__main__":
    meetmax_url_check()