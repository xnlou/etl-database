

import requests
import re
from pathlib import Path
import pandas as pd
from datetime import datetime
import time
import csv
import os
from systemscripts.directory_management import LOG_DIR, FILE_WATCHER_DIR, ensure_directory_exists

# Define constants
EVENT_IDS = [92567, 100333, 119981,119183,100332]
BASE_URL = "https://www.meetmax.com/sched/event_{}/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Ensure directories exist
ensure_directory_exists(LOG_DIR)
ensure_directory_exists(FILE_WATCHER_DIR)

def log_message(message, log_file):
    """Log a message to the specified log file."""
    with open(log_file, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
    os.chmod(log_file, 0o660)  # Set permissions to rw-rw----

def meetmax_url_check():
    """
    Check MeetMax event URLs to determine if they exist, have private pages, and offer downloadable Excel files.
    Outputs results to a CSV file and logs progress/errors to a timestamped log file.
    """
    results = []
    total = len(EVENT_IDS)
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
        "Accept-Encoding": "gzip, deflate"
    })

    for event_id in EVENT_IDS:
        counter += 1
        print(f"---\nStarting processing for EventID {event_id}")
        log_message(f"---", log_file)
        log_message(f"Starting processing for EventID {event_id}", log_file)

        # Construct URLs
        public_url = BASE_URL.format(event_id) + "__co-list_cp.html"
        private_url = BASE_URL.format(event_id) + "__private-co-list_cp.html"
        url_used = ""
        is_private = False
        is_downloadable = 0
        download_link = ""
        if_exists = 0
        invalid_event_id = False

        try:
            # Fetch the public page
            print(f"Checking public page: {public_url}")
            log_message(f"Checking public page: {public_url}", log_file)
            response = session.get(public_url, timeout=15)
            response.raise_for_status()
            print(f"Successfully fetched public page for EventID {event_id}, Status Code: {response.status_code}")
            log_message(f"Successfully fetched public page for EventID {event_id}, Status Code: {response.status_code}", log_file)

            # Check for private site indicator
            if re.search(r'<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>', response.text, re.IGNORECASE):
                print(f"Private site indicator found for EventID {event_id}")
                log_message(f"Private site indicator found for EventID {event_id}", log_file)
                is_private = True
            else:
                print(f"No private site indicator found for EventID {event_id}")
                log_message(f"No private site indicator found for EventID {event_id}", log_file)

            # Select URL to use
            url_used = private_url if is_private else public_url
            print(f"Using {'private' if is_private else 'public'} URL: {url_used}")
            log_message(f"Using {'private' if is_private else 'public'} URL: {url_used}", log_file)

            # Fetch the selected page (private if indicated, else use public response)
            if is_private:
                print(f"Fetching private page: {private_url}")
                log_message(f"Fetching private page: {private_url}", log_file)
                response = session.get(private_url, timeout=15, headers={
                    "Accept": "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                })
                response.raise_for_status()
                print(f"Successfully fetched private page for EventID {event_id}, Status Code: {response.status_code}")
                log_message(f"Successfully fetched private page for EventID {event_id}, Status Code: {response.status_code}", log_file)
            else:
                print(f"Using already fetched public page for EventID {event_id}")
                log_message(f"Using already fetched public page for EventID {event_id}", log_file)

            # Check for Invalid Event ID tag
            if re.search(r'<div class="alert alert-danger">Invalid Event ID: \d+</div>', response.text, re.IGNORECASE):
                invalid_event_id = True
                print(f"Invalid Event ID tag found for EventID {event_id}")
                log_message(f"Invalid Event ID tag found for EventID {event_id}", log_file)
            else:
                print(f"No Invalid Event ID tag found for EventID {event_id}")
                log_message(f"No Invalid Event ID tag found for EventID {event_id}", log_file)

            # Determine IfExists (URL exists if no Invalid Event ID tag)
            if_exists = 0 if invalid_event_id else 1
            print(f"IfExists for EventID {event_id}: {if_exists}")
            log_message(f"IfExists for EventID {event_id}: {if_exists}", log_file)

            # Check for downloadable link
            print(f"Checking for downloadable link for EventID {event_id}")
            log_message(f"Checking for downloadable link for EventID {event_id}", log_file)
            match = re.search(r'<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>', response.text, re.IGNORECASE)
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
                    print(f"href is a full URL for EventID {event_id}")
                    log_message(f"href is a full URL for EventID {event_id}", log_file)
                    download_link = href
                else:
                    print(f"href is relative for EventID {event_id}, appending to base URL")
                    log_message(f"href is relative for EventID {event_id}, appending to base URL", log_file)
                    download_link = BASE_URL.format(event_id) + href
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
                "DownloadLink": download_link
            }
            print(f"Result for EventID {event_id}: IfExists={if_exists}, InvalidEventID={invalid_event_id}, IsDownloadable={is_downloadable}, DownloadLink={download_link}")
            log_message(f"Result for EventID {event_id}: IfExists={if_exists}, InvalidEventID={invalid_event_id}, IsDownloadable={is_downloadable}, DownloadLink={download_link}", log_file)

            results.append(result)

        except requests.RequestException as e:
            status_code = e.response.status_code if e.response else "Unknown"
            print(f"Error processing EventID {event_id}: Status {status_code} - {str(e)}")
            log_message(f"Error processing EventID {event_id}: Status {status_code} - {str(e)}", log_file)
            results.append({
                "EventID": event_id,
                "URL": url_used,
                "IfExists": 0,
                "InvalidEventID": False,
                "IsDownloadable": 0,
                "DownloadLink": ""
            })

        # Progress update every 10 seconds
        current_time = time.time()
        if (current_time - last_progress_update Facelets = True
        if (current_time - last_progress_update) >= 10:
            print(f"Processed {counter} out of {total} URLs")
            log_message(f"Processed {counter} out of {total} URLs", log_file)
            last_progress_update = current_time

        # Add a 4-second delay to avoid rate-limiting
        print(f"Pausing for 4 seconds before next EventID")
        log_message(f"Pausing for 4 seconds before next EventID", log_file)
        time.sleep(4)

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
        log_message(f"Successfully wrote Breton results to CSV file: {csv_file}", log_file)
    except Exception as e:
        print(f"Error writing to CSV file: {str(e)}")
        log_message(f"Error writing to CSV file: {str(e)}", log_file)

    print(f"Completed: Processed {counter} out of {total} URLs")
    log_message(f"Completed: Processed {counter} out of {total} URLs", log_file)

if __name__ == "__main__":
    meetmax_url_check()