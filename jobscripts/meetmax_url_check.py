import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
import re
from pathlib import Path
import pandas as pd
from datetime import datetime
import time
import csv
from systemscripts.directory_management import LOG_DIR, FILE_WATCHER_DIR, ensure_directory_exists



# Define constants
BASE_URL = "https://www.meetmax.com/sched/event_{}/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_RETRIES = 3  # Max retries for failed requests
INITIAL_DELAY = 4.0  # Delay between requests (seconds)

# Ensure directories exist
ensure_directory_exists(LOG_DIR)
ensure_directory_exists(FILE_WATCHER_DIR)

def log_message(message, log_file):
    """Log a message to the specified log file."""
    with open(log_file, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
    os.chmod(log_file, 0o660)  # Set permissions to rw-rw----

def fetch_url(session, url, event_id, log_file, headers=None):
    """Fetch a URL with retry logic, returning response text and status code."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=15, headers=headers)
            status_code = response.status_code
            log_message(f"Attempt {attempt + 1} for EventID {event_id} at {url}: Status {status_code}", log_file)
            if status_code == 429:  # Rate-limited
                retry_after = response.headers.get('Retry-After', 10)
                log_message(f"Rate-limited for EventID {event_id}, retrying after {retry_after}s", log_file)
                time.sleep(float(retry_after))
                continue
            response.raise_for_status()
            response_text = response.text
            log_message(f"Response length for EventID {event_id}: {len(response_text)} bytes", log_file)
            return response_text, status_code
        except requests.RequestException as e:
            status_code = e.response.status_code if e.response else "Unknown"
            log_message(f"Error fetching {url} for EventID {event_id} (Attempt {attempt + 1}/{MAX_RETRIES}): Status {status_code} - {str(e)}", log_file)
            if attempt < MAX_RETRIES - 1:
                time.sleep(INITIAL_DELAY)
            else:
                return None, "Failed"
    return None, "Failed"

def meetmax_url_check():
    """Check MeetMax event URLs and output results to CSV with status codes."""
    results = []
    event_ids = range(119183, 119190)  # Adjust range as needed
    total = len(event_ids)
    counter = 0
    last_progress_update = time.time()

    # Create log file
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"meetmax_url_check_{timestamp}.log"
    log_message(f"Script execution started at {timestamp}", log_file)

    # Create session
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    })

    for event_id in event_ids:
        counter += 1
        print(f"Processing EventID {event_id}")
        log_message(f"Starting processing for EventID {event_id}", log_file)

        public_url = BASE_URL.format(event_id) + "__co-list_cp.html"
        private_url = BASE_URL.format(event_id) + "__private-co-list_cp.html"
        url_used = public_url
        is_private = False
        is_downloadable = 0
        download_link = ""
        if_exists = 0
        invalid_event_id = False
        status_code = None

        # Fetch public page
        public_response, public_status = fetch_url(session, public_url, event_id, log_file)
        if public_response is None:
            results.append({
                "EventID": event_id,
                "URL": url_used,
                "IfExists": 0,
                "InvalidEventID": False,
                "IsDownloadable": 0,
                "DownloadLink": "",
                "StatusCode": public_status
            })
            continue

        status_code = public_status

        # Check for private page
        private_match = re.search(r'<a[^>]*href=[^>]*__private-co-list_cp\.html[^>]*class=[^>]*nav-link[^>]*>Private Company List</a>', public_response, re.IGNORECASE)
        if private_match:
            is_private = True
            url_used = private_url
            response_text, status_code = fetch_url(session, private_url, event_id, log_file)
            if response_text is None:
                results.append({
                    "EventID": event_id,
                    "URL": url_used,
                    "IfExists": 0,
                    "InvalidEventID": False,
                    "IsDownloadable": 0,
                    "DownloadLink": "",
                    "StatusCode": status_code
                })
                continue
        else:
            response_text = public_response

        # Check for invalid event ID
        invalid_match = re.search(r'<div[^>]*class="[^"]*alert[^"]*alert-danger[^"]*"[^>]*>Invalid Event ID:[^<]+</div>', response_text, re.IGNORECASE)
        if invalid_match:
            invalid_event_id = True
        if_exists = 0 if invalid_event_id else 1

        # Check for downloadable link
        match = re.search(r'<a[^>]*href="([^"]*co-list_cp\.xls[^"]*)"[^>]*>', response_text, re.IGNORECASE)
        if match:
            is_downloadable = 1
            href = match.group(1)
            if "?event_id=" in href:
                base_url, query = href.split("?event_id=", 1)
                event_id_part = query.split(";", 1)[0]
                href = f"{base_url}?event_id={event_id_part}"
            download_link = href if href.startswith('http') else BASE_URL.format(event_id) + href.lstrip('/')

        results.append({
            "EventID": event_id,
            "URL": url_used,
            "IfExists": if_exists,
            "InvalidEventID": invalid_event_id,
            "IsDownloadable": is_downloadable,
            "DownloadLink": download_link,
            "StatusCode": status_code
        })

        # Progress update
        if time.time() - last_progress_update >= 10:
            print(f"Processed {counter}/{total} URLs")
            log_message(f"Processed {counter}/{total} URLs", log_file)
            last_progress_update = time.time()

        time.sleep(INITIAL_DELAY)

    # Export to CSV
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    csv_file = FILE_WATCHER_DIR / f"{timestamp}_MeetMaxURLCheck.csv"
    df = pd.DataFrame(results)
    df.to_csv(csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
    os.chmod(csv_file, 0o660)
    print(f"Results saved to {csv_file}")

if __name__ == "__main__":
    meetmax_url_check()