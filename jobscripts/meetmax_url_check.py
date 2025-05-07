import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import threading
import requests
import re
from pathlib import Path
import pandas as pd
from datetime import datetime
import time
import csv
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

# Global lock and variables
results_lock = threading.Lock()
results = []
stop_event = threading.Event()

def log_message(message, log_file):
    """Log a message to the specified log file."""
    with open(log_file, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
    os.chmod(log_file, 0o660)

def fetch_url(session, url, event_id, log_file, headers=None):
    """Fetch a URL with retry logic and exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=15, headers=headers)
            status_code = response.status_code
            log_message(f"Attempt {attempt + 1} for EventID {event_id} at {url}: Status {status_code}", log_file)
            if status_code == 429:
                retry_after = response.headers.get('Retry-After', 10)
                log_message(f"Rate-limited for EventID {event_id}, retrying after {retry_after}s", log_file)
                time.sleep(float(retry_after))
                continue
            response.raise_for_status()
            log_message(f"Response length for EventID {event_id} at {url}: {len(response.text)} bytes", log_file)
            return response.text, status_code
        except requests.RequestException as e:
            status_code = e.response.status_code if e.response else "Unknown"
            log_message(f"Error fetching {url} for EventID {event_id} (Attempt {attempt + 1}/{MAX_RETRIES}): Status {status_code} - {str(e)}", log_file)
            if attempt < MAX_RETRIES - 1:
                delay = INITIAL_DELAY * (2 ** attempt)
                log_message(f"Retrying after {delay}s", log_file)
                time.sleep(delay)
            else:
                log_message(f"Failed to fetch {url} for EventID {event_id} after {MAX_RETRIES} attempts", log_file)
                return None, "Failed"
    return None, "MaxRetriesExceeded"

def process_event(event_id, log_file):
    """Process a single event ID."""
    log_message(f"---", log_file)
    log_message(f"Starting processing for EventID {event_id}", log_file)
    log_message(f"Checking public page: {BASE_URL.format(event_id)}__co-list_cp.html", log_file)

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
            log_message(f"Failed to fetch public page for EventID {event_id}, Status: {public_status}", log_file)
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

        log_message(f"Successfully fetched public page for EventID {event_id}, Status Code: {public_status}", log_file)
        status_code = public_status

        private_match = re.search(r'<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>', public_response, re.IGNORECASE)
        log_message(f"Private site indicator match for EventID {event_id}: {bool(private_match)}", log_file)
        if private_match:
            is_private = True
            url_used = private_url
            log_message(f"Private site indicator found for EventID {event_id}", log_file)
        else:
            log_message(f"No private site indicator found for EventID {event_id}", log_file)

        response_text = public_response
        if is_private:
            log_message(f"Fetching private page: {private_url}", log_file)
            response_text, status_code = fetch_url(session, private_url, event_id, log_file, headers={
                "Accept": "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "User-Agent": USER_AGENT
            })
            if response_text is None:
                log_message(f"Failed to fetch private page for EventID {event_id}, Status: {status_code}", log_file)
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
            log_message(f"Successfully fetched private page for EventID {event_id}, Status Code: {status_code}", log_file)
        else:
            log_message(f"Using already fetched public page for EventID {event_id}", log_file)

        title_match = re.search(r'<title>(.*?)</title>', response_text, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).replace(" - MeetMax", "").strip()
            log_message(f"Extracted title for EventID {event_id}: {title}", log_file)
        else:
            log_message(f"No title found for EventID {event_id}", log_file)

        invalid_match = re.search(r'<div class="alert alert-danger">Invalid Event ID: \d+</div>', response_text, re.IGNORECASE)
        log_message(f"Invalid Event ID match for EventID {event_id}: {bool(invalid_match)}", log_file)
        if invalid_match:
            invalid_event_id = True
            log_message(f"Invalid Event ID tag found for EventID {event_id}", log_file)
        else:
            if_exists = 1
            log_message(f"No Invalid Event ID tag found for EventID {event_id}, event exists", log_file)

        log_message(f"Checking for downloadable link or export button for EventID {event_id}", log_file)
        link_match = re.search(r'<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>', response_text, re.IGNORECASE)
        button_match = re.search(r'<[^>]*id="export"[^>]*>.*?<i class="fas fa-cloud-download-alt"> </i>\s*Download Company List', response_text, re.IGNORECASE | re.DOTALL)
        log_message(f"Download link match for EventID {event_id}: {bool(link_match)}", log_file)
        log_message(f"Export button match for EventID {event_id}: {bool(button_match)}", log_file)
        
        if link_match or button_match:
            is_downloadable = 1
            if link_match:
                href = link_match.group(1)
                log_message(f"Found downloadable link for EventID {event_id}, href: {href}", log_file)
            else:
                href = f"__co-list_cp.xls?event_id={event_id}"
                log_message(f"Found export button for EventID {event_id}, generating href: {href}", log_file)

            if "?event_id=" in href:
                base_url, query = href.split("?event_id=", 1)
                event_id_part = query.split(";", 1)[0]
                href = f"{base_url}?event_id={event_id_part}"
                log_message(f"Truncated href after event_id for EventID {event_id}: {href}", log_file)

            download_link = BASE_URL.format(event_id) + href.lstrip('/') if not href.startswith('http') else href
            log_message(f"Download URL for EventID {event_id}: {download_link}", log_file)
        else:
            log_message(f"No downloadable link or export button found for EventID {event_id}", log_file)

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
        log_message(f"Result for EventID {event_id}: IfExists={if_exists}, InvalidEventID={invalid_event_id}, IsDownloadable={is_downloadable}, DownloadLink={download_link}, StatusCode={status_code}, Title={title}", log_file)
        return result

    except Exception as e:
        log_message(f"Unexpected error processing EventID {event_id}: {str(e)}", log_file)
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
        log_message("Periodic save: Starting save cycle", log_file)
        time.sleep(PERIODIC_INTERVAL)
        log_message("Periodic save: Woke up from sleep", log_file)
        with results_lock:
            log_message("Periodic save: Lock acquired, copying results", log_file)
            results_copy = results.copy()
            log_message(f"Periodic save: Results copied, length = {len(results_copy)}", log_file)
        if not results_copy:
            log_message("Periodic save: No results to save yet", log_file)
            continue
        try:
            df = pd.DataFrame(results_copy)
            df.to_csv(temp_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
            os.chmod(temp_csv_file, 0o660)
            log_message(f"Periodic save: Successfully wrote {len(results_copy)} rows to {temp_csv_file}", log_file)
        except Exception as e:
            log_message(f"Periodic save error: {str(e)}", log_file)
    log_message("Periodic save: Thread stopping due to stop_event", log_file)

def meetmax_url_check():
    """Check MeetMax event URLs and save results to CSV."""
    global results
    results = []
    event_ids = range(70841, 71000)
    total = len(event_ids)
    counter = 0
    last_progress_update = time.time()

    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"meetmax_url_check_{timestamp}.log"
    temp_csv_file = FILE_WATCHER_TEMP_DIR / f"{timestamp}_MeetMaxURLCheck.csv"
    final_csv_file = FILE_WATCHER_DIR / f"{timestamp}_MeetMaxURLCheck.csv"

    log_message(f"Script execution started at {timestamp}", log_file)
    log_message(f"Temp CSV file path: {temp_csv_file}", log_file)

    log_message("Starting periodic save thread", log_file)
    periodic_thread = threading.Thread(target=periodic_save, args=(log_file, temp_csv_file))
    periodic_thread.start()
    log_message("Periodic save thread started", log_file)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_event = {executor.submit(process_event, event_id, log_file): event_id for event_id in event_ids}

        for future in as_completed(future_to_event):
            event_id = future_to_event[future]
            counter += 1
            try:
                result = future.result()
                with results_lock:
                    log_message(f"Before appending result for EventID {event_id}", log_file)
                    results.append(result)
                    log_message(f"Appended result for EventID {event_id}, current results length: {len(results)}", log_file)
            except Exception as e:
                log_message(f"Exception processing EventID {event_id}: {str(e)}", log_file)
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
                    log_message(f"Before appending error result for EventID {event_id}", log_file)
                    results.append(error_result)
                    log_message(f"Appended error result for EventID {event_id}, current results length: {len(results)}", log_file)

            if (time.time() - last_progress_update) >= 10:
                log_message(f"Processed {counter} out of {total} URLs", log_file)
                last_progress_update = time.time()

    log_message("Signaling periodic save thread to stop", log_file)
    stop_event.set()
    periodic_thread.join()
    log_message("Periodic save thread has stopped", log_file)

    # Final save to ensure results are written
    with results_lock:
        if results:
            log_message(f"Final save: Writing {len(results)} results to {temp_csv_file}", log_file)
            df = pd.DataFrame(results)
            df.to_csv(temp_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
            os.chmod(temp_csv_file, 0o660)
            log_message(f"Final save: Successfully wrote {len(results)} rows to {temp_csv_file}", log_file)
        else:
            log_message("Final save: No results to write", log_file)

    try:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        final_csv_file = FILE_WATCHER_DIR / f"{timestamp}_MeetMaxURLCheck.csv"
        log_message(f"Exporting results to CSV: {final_csv_file}", log_file)
        df = pd.DataFrame(results)
        df.to_csv(final_csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        os.chmod(final_csv_file, 0o660)
        log_message(f"Successfully wrote results to CSV file: {final_csv_file}", log_file)
    except Exception as e:
        log_message(f"Error writing to CSV file: {str(e)}", log_file)

    log_message(f"Completed: Processed {counter} out of {total} URLs", log_file)

if __name__ == "__main__":
    meetmax_url_check()