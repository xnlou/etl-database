import sys
import os

# Add the absolute path to the parent directory
sys.path.append('/home/yostfundsadmintest1/client_etl_workflow')

import threading
import requests
import re
from pathlib import Path
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
MAX_RETRIES = 5
INITIAL_DELAY = 5.0
TASK_SUBMISSION_DELAY = 0.5
PERIODIC_INTERVAL = 1800

# Define directories
FILE_WATCHER_TEMP_DIR = FILE_WATCHER_DIR / "file_watcher_temp"

# Ensure directories exist
ensure_directory_exists(LOG_DIR)
ensure_directory_exists(FILE_WATCHER_DIR)
ensure_directory_exists(FILE_WATCHER_TEMP_DIR)

# Define Event IDs range
event_ids = range(70841, 112000)

# Global lock and variables
results_lock = threading.Lock()
results = []
stop_event = threading.Event()
script_start_time = time.time()
run_uuid = str(uuid.uuid4())
process_counters = {}  # Track counters for non-event process types
start_timestamp = None  # Will be set at script start

def save_results():
    """Save current results to a temporary CSV file, overwriting with fixed timestamp."""
    global user_cache, log_file, start_timestamp
    with results_lock:
        if results:
            temp_csv_file = FILE_WATCHER_TEMP_DIR / f"{start_timestamp}_MeetMaxURLCheck.csv"
            df = pd.DataFrame(results)
            try:
                df.to_csv(temp_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
                os.chmod(temp_csv_file, 0o660)
                log_message(log_file, "PeriodicSave", f"Saved {len(results)} rows to {temp_csv_file}", run_uuid=run_uuid, stepcounter="PeriodicSave_0", user=user_cache, script_start_time=script_start_time)
            except (PermissionError, OSError) as e:
                log_message(log_file, "Error", f"Failed to save results to {temp_csv_file}: {str(e)}", run_uuid=run_uuid, stepcounter="PeriodicSave_0", user=user_cache, script_start_time=script_start_time)

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
        public_response = fetch_url(session, public_url, retries=MAX_RETRIES, initial_delay=INITIAL_DELAY)
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
            })
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

    start_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")  # Set fixed timestamp at script start
    log_file = LOG_DIR / f"meetmax_url_check_{start_timestamp}"
    temp_csv_file = FILE_WATCHER_TEMP_DIR / f"{start_timestamp}_MeetMaxURLCheck.csv"
    final_csv_file = FILE_WATCHER_DIR / f"{start_timestamp}_MeetMaxURLCheck.csv"

    user_cache = get_username()  # Expect a single return value
    log_message(log_file, "Initialization", f"Username: {user_cache}", run_uuid=run_uuid, stepcounter="Initialization_0", user=user_cache, script_start_time=script_start_time)
    log_message(log_file, "Initialization", f"Script started at {start_timestamp}", run_uuid=run_uuid, stepcounter="Initialization_1", user=user_cache, script_start_time=script_start_time)

    # Start periodic saving
    periodic_thread = periodic_task(save_results, PERIODIC_INTERVAL, stop_event)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_event = {executor.submit(process_event, event_id, log_file): event_id for event_id in event_ids}
        for future in as_completed(future_to_event):
            event_id = future_to_event[future]
            try:
                result = future.result()
                with results_lock:
                    log_message(log_file, "EventProcessing", f"Before appending result for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
                    results.append(result)
                    log_message(log_file, "EventProcessing", f"Appended result for EventID {event_id}, current results length: {len(results)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
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
                    log_message(log_file, "Error", f"Before appending error result for EventID {event_id}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
                    results.append(error_result)
                    log_message(log_file, "Error", f"Appended error result for EventID {event_id}, current results length: {len(results)}", run_uuid=run_uuid, stepcounter=f"event_{event_id}", user=user_cache, script_start_time=script_start_time)
                event_counter += 1

    log_message(log_file, "Finalization", "Stopping periodic save thread", run_uuid=run_uuid, stepcounter="Finalization_0", user=user_cache, script_start_time=script_start_time)
    stop_event.set()
    periodic_thread.join()

    if results:
        df = pd.DataFrame(results)
        try:
            df.to_csv(final_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
            os.chmod(final_csv_file, 0o660)
            log_message(log_file, "FinalSave", f"Wrote {len(results)} rows to {final_csv_file}", run_uuid=run_uuid, stepcounter="FinalSave_0", user=user_cache, script_start_time=script_start_time)
        except (PermissionError, OSError) as e:
            log_message(log_file, "Error", f"Failed to save final results to {final_csv_file}: {str(e)}", run_uuid=run_uuid, stepcounter="FinalSave_0", user=user_cache, script_start_time=script_start_time)

    log_message(log_file, "Finalization", f"Completed: Processed {event_counter}/{total} URLs", run_uuid=run_uuid, stepcounter="Finalization_1", user=user_cache, script_start_time=script_start_time)

if __name__ == "__main__":
    meetmax_url_check()