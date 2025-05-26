import sys
import os
from pathlib import Path
# Add the absolute path to the parent directory
sys.path.append(str(Path.home() / 'client_etl_workflow'))
import threading
import requests
import re
import pandas as pd
from datetime import datetime
import time
import uuid
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from systemscripts.user_utils import get_username
from systemscripts.log_utils import log_message
from systemscripts.web_utils import fetch_url
from systemscripts.periodic_utils import periodic_task
from systemscripts.directory_management import LOG_DIR, FILE_WATCHER_DIR, ensure_directory_exists

# Define constants
BASE_URL = "https://www.meetmax.com/sched/event_{}/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_RETRIES = 3  # Reduced to avoid excessive retries on rate limits
INITIAL_DELAY = 15.0  # Increased for longer retry backoff
TASK_SUBMISSION_DELAY = 5.0  # Enforce delay between task submissions
PERIODIC_INTERVAL = 300  # Increased for longer runs

# Define directories
FILE_WATCHER_TEMP_DIR = FILE_WATCHER_DIR / "file_watcher_temp"

# Ensure directories exist
ensure_directory_exists(LOG_DIR)
ensure_directory_exists(FILE_WATCHER_DIR)
ensure_directory_exists(FILE_WATCHER_TEMP_DIR)

# Define Event IDs range
event_ids = range(119179, 119184)

# Global lock and variables
results_lock = threading.Lock()
results = []
stop_event = threading.Event()
script_start_time = time.time()
run_uuid = str(uuid.uuid4())
process_counters = {}
start_timestamp = None

def save_results():
    """Save current results to a temporary CSV file, overwriting with fixed timestamp."""
    global user_cache, log_file, start_timestamp
    log_message(log_file, "PeriodicSave", f"save_results called, current results length: {len(results)}", run_uuid=run_uuid, stepcounter="PeriodicSave_call", user=user_cache, script_start_time=script_start_time)
    with results_lock:
        if results:
            temp_csv_file = FILE_WATCHER_TEMP_DIR / f"{start_timestamp}_MeetMaxURLCheck.csv"
            log_message(log_file, "PeriodicSave", f"Attempting to save to {temp_csv_file}", run_uuid=run_uuid, stepcounter="PeriodicSave_1", user=user_cache, script_start_time=script_start_time)
            df = pd.DataFrame(results)
            df = df.sort_values(by='EventID')
            try:
                df.to_csv(temp_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
                os.chmod(temp_csv_file, 0o660)
                log_message(log_file, "PeriodicSave", f"Saved {len(results)} rows to {temp_csv_file}", run_uuid=run_uuid, stepcounter="PeriodicSave_0", user=user_cache, script_start_time=script_start_time)
            except (PermissionError, OSError) as e:
                log_message(log_file, "Error", f"Failed to save results to {temp_csv_file}: {str(e)}", run_uuid=run_uuid, stepcounter="PeriodicSave_0", user=user_cache, script_start_time=script_start_time)
        else:
            log_message(log_file, "PeriodicSave", "No results to save yet", run_uuid=run_uuid, stepcounter="PeriodicSave_2", user=user_cache, script_start_time=script_start_time)

def process_event(event_id, log_file):
    """Process a single event ID."""
    log_message(log_file, "EventProcessing", f"Starting processing for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
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
        public_response = fetch_url(session, public_url, retries=MAX_RETRIES, initial_delay=INITIAL_DELAY, log_file=log_file, run_uuid=run_uuid, user=user_cache, script_start_time=script_start_time)
        if public_response is None:
            log_message(log_file, "Error", f"Failed to fetch public page for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
            return {
                "EventID": event_id,
                "URL": url_used,
                "IfExists": 0,
                "InvalidEventID": False,
                "IsDownloadable": 0,
                "DownloadLink": "",
                "StatusCode": "Failed",
                "Title": ""
            }

        status_code = public_response.status_code
        log_message(log_file, "EventProcessing", f"Attempt 1 for EventID {event_id} at {public_url}: Status {status_code}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        log_message(log_file, "EventProcessing", f"Response length for EventID {event_id} at {public_url}: {len(public_response.text)} bytes", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

        private_match = re.search(r'<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>', public_response.text, re.IGNORECASE)
        log_message(log_file, "EventProcessing", f"Private site indicator match for EventID {event_id}: {bool(private_match)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        if private_match:
            is_private = True
            url_used = private_url
            log_message(log_file, "EventProcessing", f"Private site indicator found for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

        response_text = public_response.text
        if is_private:
            log_message(log_file, "EventProcessing", f"Fetching private page: {private_url}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
            private_response = fetch_url(session, private_url, retries=MAX_RETRIES, initial_delay=INITIAL_DELAY, headers={
                "Accept": "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "User-Agent": USER_AGENT
            }, log_file=log_file, run_uuid=run_uuid, user=user_cache, script_start_time=script_start_time)
            if private_response is None:
                log_message(log_file, "Error", f"Failed to fetch private page for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
                return {
                    "EventID": event_id,
                    "URL": url_used,
                    "IfExists": 0,
                    "InvalidEventID": False,
                    "IsDownloadable": 0,
                    "DownloadLink": "",
                    "StatusCode": "Failed",
                    "Title": ""
                }
            response_text = private_response.text
            status_code = private_response.status_code
            log_message(log_file, "EventProcessing", f"Attempt 1 for EventID {event_id} at {private_url}: Status {status_code}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
            log_message(log_file, "EventProcessing", f"Response length for EventID {event_id} at {private_url}: {len(response_text)} bytes", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

        title_match = re.search(r'<title>(.*?)</title>', response_text, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).replace(" - MeetMax", "").strip()
            log_message(log_file, "EventProcessing", f"Extracted title for EventID {event_id}: {title}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        else:
            log_message(log_file, "EventProcessing", f"No title found for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

        invalid_match = re.search(r'<div class="alert alert-danger">Invalid Event ID: \d+</div>', response_text, re.IGNORECASE)
        log_message(log_file, "EventProcessing", f"Invalid Event ID match for EventID {event_id}: {bool(invalid_match)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        if invalid_match:
            invalid_event_id = True
            log_message(log_file, "EventProcessing", f"Invalid Event ID tag found for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        else:
            if_exists = 1
            log_message(log_file, "EventProcessing", f"No Invalid Event ID tag found for EventID {event_id}, event exists", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

        log_message(log_file, "EventProcessing", f"Checking for downloadable link or export button for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        link_match = re.search(r'<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>', response_text, re.IGNORECASE)
        button_match = re.search(r'<[^>]*id="export"[^>]*>.*?<i class="fas fa-cloud-download-alt"> </i>\s*Download Company List', response_text, re.IGNORECASE | re.DOTALL)
        log_message(log_file, "EventProcessing", f"Download link match for EventID {event_id}: {bool(link_match)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        log_message(log_file, "EventProcessing", f"Export button match for EventID {event_id}: {bool(button_match)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        if link_match or button_match:
            is_downloadable = 1
            if link_match:
                href = link_match.group(1)
                log_message(log_file, "EventProcessing", f"Found downloadable link for EventID {event_id}, href: {href}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
            else:
                href = f"__co-list_cp.xls?event_id={event_id}"
                log_message(log_file, "EventProcessing", f"Found export button for EventID {event_id}, generating href: {href}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

            if "?event_id=" in href:
                base_url, query = href.split("?event_id=", 1)
                event_id_part = query.split(";", 1)[0]
                href = f"{base_url}?event_id={event_id_part}"
                log_message(log_file, "EventProcessing", f"Truncated href after event_id for EventID {event_id}: {href}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

            download_link = BASE_URL.format(event_id) + href.lstrip('/') if not href.startswith('http') else href
            log_message(log_file, "EventProcessing", f"Download URL for EventID {event_id}: {download_link}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        else:
            log_message(log_file, "EventProcessing", f"No downloadable link or export button found for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)

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
        log_message(log_file, "EventProcessing", f"Result for EventID {event_id}: IfExists={if_exists}, InvalidEventID={invalid_event_id}, IsDownloadable={is_downloadable}, DownloadLink={download_link}, StatusCode={status_code}, Title={title}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
        return result

    except Exception as e:
        log_message(log_file, "Error", f"Unexpected error processing EventID {event_id}: {str(e)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
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

def meetmax_url_check():
    """Check MeetMax event URLs and save results to CSV."""
    global results, stop_event, script_start_time, run_uuid, user_cache, log_file, start_timestamp
    results = []
    total = len(event_ids)
    event_counter = 0

    start_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"meetmax_url_check_{start_timestamp}"
    temp_csv_file = FILE_WATCHER_TEMP_DIR / f"{start_timestamp}_MeetMaxURLCheck.csv"
    final_csv_file = FILE_WATCHER_DIR / f"{start_timestamp}_MeetMaxURLCheck.csv"

    user_cache = get_username()
    log_message(log_file, "Initialization", f"Username: {user_cache}", run_uuid=run_uuid, stepcounter="Initialization_0", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Initialization", f"Script started at {start_timestamp}", run_uuid=run_uuid, stepcounter="Initialization_1", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Initialization", f"Final CSV path: {final_csv_file}", run_uuid=run_uuid, stepcounter="Initialization_2", user=user_cache, script_start_time=script_start_time)

    # Log active threads at start
    log_message(log_file, "Initialization", f"Active threads at start: {threading.active_count()}", run_uuid=run_uuid, stepcounter="Initialization_3", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Initialization", f"Thread names: {[t.name for t in threading.enumerate()]}", run_uuid=run_uuid, stepcounter="Initialization_4", user=user_cache, script_start_time=script_start_time)

    # Start periodic saving
    log_message(log_file, "Initialization", "Starting periodic save thread", run_uuid=run_uuid, stepcounter="Initialization_5", user=user_cache, script_start_time=script_start_time)
    periodic_thread = periodic_task(save_results, PERIODIC_INTERVAL, stop_event)
    log_message(log_file, "Initialization", f"Periodic thread started, is_alive: {periodic_thread.is_alive()}", run_uuid=run_uuid, stepcounter="Initialization_6", user=user_cache, script_start_time=script_start_time)

    with ThreadPoolExecutor(max_workers=1) as executor:
        for event_id in event_ids:
            log_message(log_file, "Processing", f"Submitting task for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"submit_{event_id}", user=user_cache, script_start_time=script_start_time)
            future = executor.submit(process_event, event_id, log_file)
            try:
                result = future.result()  # Wait for the task to complete
                with results_lock:
                    log_message(log_file, "EventProcessing", f"Appending result for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}_append", user=user_cache, script_start_time=script_start_time)
                    results.append(result)
                    log_message(log_file, "EventProcessing", f"Appended result for EventID {event_id}, current results length: {len(results)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}_appended", user=user_cache, script_start_time=script_start_time)
                event_counter += 1
            except Exception as e:
                log_message(log_file, "Error", f"Exception processing EventID {event_id}: {str(e)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
                with results_lock:
                    error_result = {
                        "EventID": event_id,
                        "URL": BASE_URL.format(event_id) + "__co-list_cp.html",
                        "IfExists": 0,
                        "InvalidEventID": False,
                        "IsDownloadable": 0,
                        "DownloadLink": "",
                        "StatusCode": "Error",
                        "Title": ""
                    }
                    log_message(log_file, "Error", f"Appending error result for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}_append", user=user_cache, script_start_time=script_start_time)
                    results.append(error_result)
                    log_message(log_file, "Error", f"Appended error result for EventID {event_id}, current results length: {len(results)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}_appended", user=user_cache, script_start_time=script_start_time)
                event_counter += 1
            time.sleep(TASK_SUBMISSION_DELAY)  # Delay between task submissions
        log_message(log_file, "Processing", f"Processed {event_counter} events", run_uuid=run_uuid, stepcounter="Processing_1", user=user_cache, script_start_time=script_start_time)

    # Log active threads after processing
    log_message(log_file, "Finalization", f"Active threads after processing: {threading.active_count()}", run_uuid=run_uuid, stepcounter="Finalization_0", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Finalization", f"Thread names: {[t.name for t in threading.enumerate()]}", run_uuid=run_uuid, stepcounter="Finalization_1", user=user_cache, script_start_time=script_start_time)

    # Stop periodic save thread
    log_message(log_file, "Finalization", "Setting stop_event for periodic save thread", run_uuid=run_uuid, stepcounter="Finalization_2", user=user_cache, script_start_time=script_start_time)
    stop_event.set()
    log_message(log_file, "Finalization", f"Stop event set, periodic thread is_alive: {periodic_thread.is_alive()}", run_uuid=run_uuid, stepcounter="Finalization_3", user=user_cache, script_start_time=script_start_time)

    # Attempt to join periodic thread with timeout
    log_message(log_file, "Finalization", "Joining periodic save thread with 10-second timeout", run_uuid=run_uuid, stepcounter="Finalization_4", user=user_cache, script_start_time=script_start_time)
    try:
        periodic_thread.join(timeout=10.0)
        if periodic_thread.is_alive():
            log_message(log_file, "Error", "Periodic thread did not terminate within 10 seconds", run_uuid=run_uuid, stepcounter="Finalization_5", user=user_cache, script_start_time=script_start_time)
        else:
            log_message(log_file, "Finalization", "Periodic thread terminated successfully", run_uuid=run_uuid, stepcounter="Finalization_6", user=user_cache, script_start_time=script_start_time)
    except Exception as e:
        log_message(log_file, "Error", f"Error joining periodic thread: {str(e)}", run_uuid=run_uuid, stepcounter="Finalization_7", user=user_cache, script_start_time=script_start_time)

    # Log active threads after joining
    log_message(log_file, "Finalization", f"Active threads after joining: {threading.active_count()}", run_uuid=run_uuid, stepcounter="Finalization_8", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Finalization", f"Thread names: {[t.name for t in threading.enumerate()]}", run_uuid=run_uuid, stepcounter="Finalization_9", user=user_cache, script_start_time=script_start_time)

    # Final save
    log_message(log_file, "Finalization", f"Starting final save, results length: {len(results)}", run_uuid=run_uuid, stepcounter="Finalization_10", user=user_cache, script_start_time=script_start_time)
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values(by='EventID')
        log_message(log_file, "FinalSave", f"Attempting to save {len(results)} rows to {final_csv_file}", run_uuid=run_uuid, stepcounter="FinalSave_0", user=user_cache, script_start_time=script_start_time)
        try:
            df.to_csv(final_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
            log_message(log_file, "FinalSave", f"CSV write completed for {final_csv_file}", run_uuid=run_uuid, stepcounter="FinalSave_1", user=user_cache, script_start_time=script_start_time)
            os.chmod(final_csv_file, 0o660)
            log_message(log_file, "FinalSave", f"Permissions set for {final_csv_file}, wrote {len(results)} rows", run_uuid=run_uuid, stepcounter="FinalSave_2", user=user_cache, script_start_time=script_start_time)
        except (PermissionError, OSError) as e:
            log_message(log_file, "Error", f"Failed to save final results to {final_csv_file}: {str(e)}", run_uuid=run_uuid, stepcounter="FinalSave_3", user=user_cache, script_start_time=script_start_time)
    else:
        log_message(log_file, "FinalSave", "No results to save", run_uuid=run_uuid, stepcounter="FinalSave_4", user=user_cache, script_start_time=script_start_time)

    # Log completion
    log_message(log_file, "Finalization", f"Completed: Processed {event_counter}/{total} URLs", run_uuid=run_uuid, stepcounter="Finalization_11", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Finalization", f"Active threads at end: {threading.active_count()}", run_uuid=run_uuid, stepcounter="Finalization_12", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Finalization", f"Thread names: {[t.name for t in threading.enumerate()]}", run_uuid=run_uuid, stepcounter="Finalization_13", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Finalization", "Script execution completed", run_uuid=run_uuid, stepcounter="Finalization_14", user=user_cache, script_start_time=script_start_time)

if __name__ == "__main__":
    meetmax_url_check()