import requests
import re
from pathlib import Path
import pandas as pd
from datetime import datetime
import time

# Define constants
EVENT_IDS = [112601, 112621, 112643, 112657, 112663, 112687, 112691]
LOG_DIR = Path("/home/yostfundsadmintest/etl_workflow/logs")
OUTPUT_DIR = Path("/home/yostfundsadmintest/etl_workflow/file_watcher")
BASE_URL = "https://www.meetmax.com/sched/event_{}/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Ensure directories exist
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def log_message(message):
    """Log a message to a timestamped log file."""
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"{timestamp}_meetmax_url_check.log"
    with open(log_file, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def meetmax_url_check():
    results = []
    total = len(EVENT_IDS)
    counter = 0
    last_progress_update = time.time()

    # Create a session to maintain cookies
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate"
    })

    for event_id in EVENT_IDS:
        counter += 1
        log_message(f"---")
        log_message(f"Starting processing for EventID {event_id}")

        # Construct URLs
        public_url = BASE_URL.format(event_id) + "__co-list_cp.html"
        private_url = BASE_URL.format(event_id) + "__private-co-list_cp.html"
        url_used = ""
        is_private = False
        is_downloadable = 0
        download_link = ""
        if_exists = 0
        conference_name = ""

        try:
            # Fetch the public page
            log_message(f"Checking public page for private site indicator: {public_url}")
            response = session.get(public_url, timeout=15)
            response.raise_for_status()
            log_message(f"Successfully fetched public page for EventID {event_id}, Status Code: {response.status_code}")

            # Check for private site indicator
            if re.search(r'<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>', response.text, re.IGNORECASE):
                log_message(f"Private site indicator found for EventID {event_id} in public page HTML")
                is_private = True
            else:
                log_message(f"No private site indicator found for EventID {event_id} in public page HTML")

            # Select URL to use
            url_used = private_url if is_private else public_url
            log_message(f"Using {'private' if is_private else 'public'} URL for EventID {event_id}: {url_used}")

            # Fetch the selected page (private if indicated, else use public response)
            if is_private:
                log_message(f"Fetching private page for EventID {event_id} from {private_url}")
                response = session.get(private_url, timeout=15, headers={
                    "Accept": "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                })
                response.raise_for_status()
                log_message(f"Successfully fetched private page for EventID {event_id}, Status Code: {response.status_code}")
            else:
                log_message(f"Using already fetched public page for EventID {event_id}")

            # Determine IfExists
            if_exists = 0 if "Invalid Event ID" in response.text else 1
            log_message(f"IfExists for EventID {event_id}: {if_exists}")

            # Check for downloadable link
            log_message(f"Checking for downloadable link in HTML for EventID {event_id}")
            match = re.search(r'<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>', response.text, re.IGNORECASE)
            if match:
                is_downloadable = 1
                href = match.group(1)
                log_message(f"Condition 1: Found downloadable link for EventID {event_id}, href: {href}")
                if href.startswith('http'):
                    log_message(f"Subcondition 1a: href is a full URL for EventID {event_id}")
                    download_link = href
                else:
                    log_message(f"Subcondition 1b: href is relative for EventID {event_id}, appending to base URL")
                    download_link = BASE_URL.format(event_id) + href
                log_message(f"Using download URL for EventID {event_id}: {download_link}")
            else:
                log_message(f"Condition 2: No downloadable link found in HTML for EventID {event_id}")

            # Extract ConferenceName
            if if_exists:
                log_message(f"Extracting ConferenceName for EventID {event_id}")
                match = re.search(r"<title>(.*?)</title>", response.text)
                if match:
                    conference_name = match.group(1).replace(" - MeetMax", "")
                    log_message(f"ConferenceName for EventID {event_id}: {conference_name}")
                else:
                    log_message(f"No ConferenceName found for EventID {event_id}")

            # Create result object
            result = {
                "EventID": event_id,
                "URL": url_used,
                "ConferenceName": conference_name,
                "IfExists": if_exists,
                "IsDownloadable": is_downloadable,
                "DownloadLink": download_link
            }
            log_message(f"Created result object for EventID {event_id}: IfExists={if_exists}, IsDownloadable={is_downloadable}, DownloadLink={download_link}")
            results.append(result)

        except requests.RequestException as e:
            status_code = e.response.status_code if e.response else "Unknown"
            log_message(f"Error processing EventID {event_id}: Status {status_code} - {str(e)}")
            results.append({
                "EventID": event_id,
                "URL": url_used,
                "ConferenceName": "",
                "IfExists": 0,
                "IsDownloadable": 0,
                "DownloadLink": ""
            })

        # Progress update every 10 seconds
        current_time = time.time()
        if (current_time - last_progress_update) >= 10:
            log_message(f"Processed {counter} out of {total} URLs")
            last_progress_update = current_time

        # Add a 4-second delay to avoid rate-limiting
        log_message(f"Pausing for 4 seconds before next EventID")
        time.sleep(4)

    # Export results to CSV
    try:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        csv_file = OUTPUT_DIR / f"{timestamp}_MeetMaxURLCheck.csv"
        log_message(f"Exporting results to CSV: {csv_file}")
        df = pd.DataFrame(results)
        df.to_csv(csv_file, index=False)
        log_message(f"Successfully wrote results to CSV file: {csv_file}")
    except Exception as e:
        log_message(f"Error writing to CSV file: {str(e)}")

    log_message(f"Completed: Processed {counter} out of {total} URLs")

if __name__ == "__main__":
    meetmax_url_check()