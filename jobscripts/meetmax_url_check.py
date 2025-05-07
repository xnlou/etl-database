import sys
import os

# Add the parent directory to sys.path
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
            log_message(f"Response length for EventID {event_id} at {url}: {len(response_text)} bytes", log_file)
            return response_text, status_code
        except requests.RequestException as e:
            status_code = e.response.status_code if e.response else "Unknown"
            log_message(f"Error fetching {url} for EventID {event_id} (Attempt {attempt + 1}/{MAX_RETRIES}): Status {status_code} - {str(e)}", log_file)
            if attempt < MAX_RETRIES - 1:
                time.sleep(INITIAL_DELAY)
            else:
                log_message(f"Failed to fetch {url} for EventID {event_id} after {MAX_RETRIES} attempts", log_file)
                return None, "Failed"
    return None, "MaxRetriesExceeded"

def meetmax_url_check():
    """
    Check MeetMax event URLs to determine if they exist, have private pages, and offer downloadable Excel files.
    Outputs results to a CSV file and logs progress/errors to a timestamped log file.
    """
    results = []
    event_ids = range(70841, 120000)  # Adjust range as needed
    total = len(event_ids)
    counter = 0
    last_progress_update = time.time()

    # Create a single log file for this execution
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"meetmax_url_check_{timestamp}.log"
    ensure_directory_exists(LOG_DIR)  # Ensure log directory exists
    
    # Log the start of the script
    log_message(f"Script execution started at {timestamp}", log_file)

    # Create a session to maintain cookies
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
        print(f"---\nStarting processing for EventID {event_id}")
        log_message(f"---", log_file)
        log_message(f"Starting processing for EventID {event_id}", log_file)

        # Construct URLs
        public_url = BASE_URL.format(event_id) + "__co-list_cp.html"
        private_url = BASE_URL.format(event_id) + "__private-co-list_cp.html"
        url_used = public_url  # Default to public_url to ensure non-empty
        is_private = False
        is_downloadable = 0
        download_link = ""
        if_exists = 0
        invalid_event_id = False
        status_code = None

        try:
            # Fetch the public page
            print(f"Checking public page: {public_url}")
            log_message(f"Checking public page: {public_url}", log_file)
            public_response, public_status = fetch_url(session, public_url, event_id, log_file)
            if public_response is None:
                print(f"Failed to fetch public page for EventID {event_id}, Status: {public_status}")
                log_message(f"Failed to fetch public page for EventID {event_id}, Status: {public_status}", log_file)
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

            print(f"Successfully fetched public page for EventID {event_id}, Status Code: {public_status}")
            log_message(f"Successfully fetched public page for EventID {event_id}, Status Code: {public_status}", log_file)
            status_code = public_status

            # Check for private site indicator
            private_match = re.search(r'<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>', public_response, re.IGNORECASE)
            log_message(f"Private site indicator match for EventID {event_id}: {bool(private_match)}", log_file)
            if private_match:
                is_private = True
                url_used = private_url
                print(f"Private site indicator found for EventID {event_id}")
                log_message(f"Private site indicator found for EventID {event_id}", log_file)
            else:
                print(f"No private site indicator found for EventID {event_id}")
                log_message(f"No private site indicator found for EventID {event_id}", log_file)

            # Fetch the selected page (private if indicated, else use public response)
            response_text = public_response
            if is_private:
                print(f"Fetching private page: {private_url}")
                log_message(f"Fetching private page: {private_url}", log_file)
                response_text, status_code = fetch_url(session, private_url, event_id, log_file, headers={
                    "Accept": "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive",
                    "User-Agent": USER_AGENT
                })
                if response_text is None:
                    print(f"Failed to fetch private page for EventID {event_id}, Status: {status_code}")
                    log_message(f"Failed to fetch private page for EventID {event_id}, Status: {status_code}", log_file)
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
                print(f"Successfully fetched private page for EventID {event_id}, Status Code: {status_code}")
                log_message(f"Successfully fetched private page for EventID {event_id}, Status Code: {status_code}", log_file)
            else:
                print(f"Using already fetched public page for EventID {event_id}")
                log_message(f"Using already fetched public page for EventID {event_id}", log_file)

            # Check for Invalid Event ID tag
            invalid_match = re.search(r'<div class="alert alert-danger">Invalid Event ID: \d+</div>', response_text, re.IGNORECASE)
            log_message(f"Invalid Event ID match for EventID {event_id}: {bool(invalid_match)}", log_file)
            if invalid_match:
                invalid_event_id = True
                print(f"Invalid Event ID tag found for EventID {event_id}")
                log_message(f"Invalid Event ID tag found for EventID {event_id}", log_file)
            else:
                invalid_event_id = False
                if_exists = 1  # If no invalid tag, the event exists
                print(f"No Invalid Event ID tag found for EventID {event_id}, event exists")
                log_message(f"No Invalid Event ID tag found for EventID {event_id}, event exists", log_file)

            # Check for downloadable link
            print(f"Checking for downloadable link for EventID {event_id}")
            log_message(f"Checking for downloadable link for EventID {event_id}", log_file)
            match = re.search(r'<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>', response_text, re.IGNORECASE)
            log_message(f"Download link match for EventID {event_id}: {bool(match)}", log_file)
            if match:
                is_downloadable = 1
                href = match.group(1)
                print(f"Found downloadable link for EventID {event_id}, href: {href}")
                log_message(f"Found downloadable link for EventID {event_id}, href: {href}", log_file)

                # Truncate href after event_id parameter (split on semicolon)
                if "?event_id=" in href:
                    base_url, query = href.split("?event_id=", 1)
                    event_id_part = query.split(";", 1)[0]
                    href = f"{base_url}?event_id={event_id_part}"
                    print(f"Truncated href after event_id for EventID {event_id}: {href}")
                    log_message(f"Truncated href after event_id for EventID {event_id}: {href}", log_file)

                if href.startswith('http'):
                    download_link = href
                else:
                    download_link = BASE_URL.format(event_id) + href.lstrip('/')
                print(f"Download URL for EventID {event_id}: {download_link}")
                log_message(f"Download URL for EventID {event_id}: {download_link}", log_file)
            else:
                print(f"No downloadable link found for EventID {event_id}")
                log_message(f"No downloadable link found for EventID {event_id}", log_file)

            # Create result object
            result = {
                "EventID": event_id,
                "URL": url_used,
                "IfExists": if_exists,
                "InvalidEventID": invalid_event_id,
                "IsDownloadable": is_downloadable,
                "DownloadLink": download_link,
                "StatusCode": str(status_code)
            }
            print(f"Result for EventID {event_id}: IfExists={if_exists}, InvalidEventID={invalid_event_id}, IsDownloadable={is_downloadable}, DownloadLink={download_link}, StatusCode={status_code}")
            log_message(f"Result for EventID {event_id}: IfExists={if_exists}, InvalidEventID={invalid_event_id}, IsDownloadable={is_downloadable}, DownloadLink={download_link}, StatusCode={status_code}", log_file)

            results.append(result)

        except Exception as e:
            print(f"Unexpected error processing EventID {event_id}: {str(e)}")
            log_message(f"Unexpected error processing EventID {event_id}: {str(e)}", log_file)
            results.append({
                "EventID": event_id,
                "URL": url_used,
                "IfExists": 0,
                "InvalidEventID": False,
                "IsDownloadable": 0,
                "DownloadLink": "",
                "StatusCode": "Error"
            })

        # Progress update every 10 seconds
        current_time = time.time()
        if (current_time - last_progress_update) >= 10:
            print(f"Processed {counter} out of {total} URLs")
            log_message(f"Processed {counter} out of {total} URLs", log_file)
            last_progress_update = current_time

        # Add a 4-second delay to avoid rate-limiting
        print(f"Pausing for 4 seconds before next EventID")
        log_message(f"Pausing for 4 seconds before next EventID", log_file)
        time.sleep(INITIAL_DELAY)

    # Export results to CSV with proper quoting
    try:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        csv_file = FILE_WATCHER_DIR / f"{timestamp}_MeetMaxURLCheck.csv"
        print(f"Exporting results to CSV: {csv_file}")
        log_message(f"Exporting results to CSV: {csv_file}", log_file)
        df = pd.DataFrame(results)
        df.to_csv(csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        os.chmod(csv_file, 0o660)  # Set permissions to rw-rw----
        print(f"Successfully wrote results to CSV file: {csv_file}")
        log_message(f"Successfully wrote results to CSV file: {csv_file}", log_file)
    except Exception as e:
        print(f"Error writing to CSV file: {str(e)}")
        log_message(f"Error writing to CSV file: {str(e)}", log_file)

    print(f"Completed: Processed {counter} out of {total} URLs")
    log_message(f"Completed: Processed {counter} out of {total} URLs", log_file)

if __name__ == "__main__":
    meetmax_url_check()