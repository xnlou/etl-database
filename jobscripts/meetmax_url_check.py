import sys
import os
import asyncio
import aiohttp
import re
from pathlib import Path
import pandas as pd
from datetime import datetime
import csv
from tqdm import tqdm
from systemscripts.directory_management import LOG_DIR, FILE_WATCHER_DIR, ensure_directory_exists

# Define constants
BASE_URL = "https://www.meetmax.com/sched/event_{}/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
CONCURRENT_REQUESTS = 5  # Limit concurrent requests to avoid rate-limiting
INITIAL_DELAY = 0.5  # Initial delay between batches (seconds)
MAX_RETRIES = 3  # Max retries for failed requests
BACKOFF_FACTOR = 2  # Exponential backoff factor for retries

# Ensure directories exist
ensure_directory_exists(LOG_DIR)
ensure_directory_exists(FILE_WATCHER_DIR)

def log_message(message, log_file):
    """Log a message to the specified log file."""
    with open(log_file, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
    os.chmod(log_file, 0o660)  # Set permissions to rw-rw----

async def fetch_url(session, url, event_id, log_file, headers=None):
    """Fetch a URL with retry logic and rate-limiting handling."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=15, headers=headers) as response:
                if response.status == 429:  # Rate-limited
                    retry_after = response.headers.get('Retry-After', 10)
                    log_message(f"Rate-limited for EventID {event_id}, retrying after {retry_after}s", log_file)
                    await asyncio.sleep(float(retry_after))
                    continue
                response.raise_for_status()
                return await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            status = getattr(e, 'status', 'Unknown')
            if status == 429:
                retry_after = 10  # Default retry-after if not specified
                log_message(f"Rate-limited for EventID {event_id}, retrying after {retry_after}s", log_file)
                await asyncio.sleep(retry_after)
                continue
            log_message(f"Error fetching {url} for EventID {event_id} (Attempt {attempt + 1}/{MAX_RETRIES}): Status {status} - {str(e)}", log_file)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(INITIAL_DELAY * (BACKOFF_FACTOR ** attempt))
            else:
                return None
    return None

async def process_event(event_id, session, semaphore, log_file):
    """Process a single event ID, checking public/private pages and downloadable links."""
    async with semaphore:
        print(f"Starting processing for EventID {event_id}")
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
            log_message(f"Checking public page: {public_url}", log_file)
            public_response = await fetch_url(session, public_url, event_id, log_file)
            if public_response is None:
                log_message(f"Failed to fetch public page for EventID {event_id}", log_file)
                return {
                    "EventID": event_id,
                    "URL": url_used,
                    "IfExists": 0,
                    "InvalidEventID": False,
                    "IsDownloadable": 0,
                    "DownloadLink": ""
                }

            log_message(f"Successfully fetched public page for EventID {event_id}", log_file)

            # Check for private site indicator
            if re.search(r'<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>', public_response, re.IGNORECASE):
                log_message(f"Private site indicator found for EventID {event_id}", log_file)
                is_private = True
            else:
                log_message(f"No private site indicator found for EventID {event_id}", log_file)

            # Select URL to use
            url_used = private_url if is_private else public_url
            log_message(f"Using {'private' if is_private else 'public'} URL: {url_used}", log_file)

            # Fetch the selected page (private if indicated, else use public response)
            response_text = public_response
            if is_private:
                log_message(f"Fetching private page: {private_url}", log_file)
                response_text = await fetch_url(session, private_url, event_id, log_file, headers={
                    "Accept": "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                })
                if response_text is None:
                    log_message(f"Failed to fetch private page for EventID {event_id}", log_file)
                    return {
                        "EventID": event_id,
                        "URL": url_used,
                        "IfExists": 0,
                        "InvalidEventID": False,
                        "IsDownloadable": 0,
                        "DownloadLink": ""
                    }
                log_message(f"Successfully fetched private page for EventID {event_id}", log_file)
            else:
                log_message(f"Using already fetched public page for EventID {event_id}", log_file)

            # Check for Invalid Event ID tag
            if re.search(r'<div class="alert alert-danger">Invalid Event ID: \d+</div>', response_text, re.IGNORECASE):
                invalid_event_id = True
                log_message(f"Invalid Event ID tag found for EventID {event_id}", log_file)
            else:
                log_message(f"No Invalid Event ID tag found for EventID {event_id}", log_file)

            # Determine IfExists (URL exists if no Invalid Event ID tag)
            if_exists = 0 if invalid_event_id else 1
            log_message(f"IfExists for EventID {event_id}: {if_exists}", log_file)

            # Check for downloadable link
            log_message(f"Checking for downloadable link for EventID {event_id}", log_file)
            match = re.search(r'<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>', response_text, re.IGNORECASE)
            if match:
                is_downloadable = 1
                href = match.group(1)
                log_message(f"Found downloadable link for EventID {event_id}, href: {href}", log_file)

                # Truncate href after event_id parameter (split on semicolon)
                if "?event_id=" in href:
                    base_url, query = href.split("?event_id=", 1)
                    event_id_part = query.split(";", 1)[0]
                    href = f"{base_url}?event_id={event_id_part}"
                    log_message(f"Truncated href after event_id for EventID {event_id}: {href}", log_file)

                if href.startswith('http'):
                    log_message(f"href is a full URL for EventID {event_id}", log_file)
                    download_link = href
                else:
                    log_message(f"href is relative for EventID {event_id}, appending to base URL", log_file)
                    download_link = BASE_URL.format(event_id) + href
                log_message(f"Download URL for EventID {event_id}: {download_link}", log_file)
            else:
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
            log_message(f"Result for EventID {event_id}: IfExists={if_exists}, InvalidEventID={invalid_event_id}, IsDownloadable={is_downloadable}, DownloadLink={download_link}", log_file)
            return result

        except Exception as e:
            log_message(f"Unexpected error processing EventID {event_id}: {str(e)}", log_file)
            return {
                "EventID": event_id,
                "URL": url_used,
                "IfExists": 0,
                "InvalidEventID": False,
                "IsDownloadable": 0,
                "DownloadLink": ""
            }

async def meetmax_url_check():
    """
    Check MeetMax event URLs to determine if they exist, have private pages, and offer downloadable Excel files.
    Outputs results to a CSV file and logs progress/errors to a timestamped log file.
    """
    results = []
    event_ids = range(70841, 70845)  # Adjust range as needed
    total = len(event_ids)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"meetmax_url_check_{timestamp}.log"
    ensure_directory_exists(LOG_DIR)  # Ensure log directory exists

    # Log the start of the script
    log_message(f"Script execution started at {timestamp}", log_file)

    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate"
    }) as session:
        # Process all event IDs concurrently with progress bar
        tasks = [process_event(event_id, session, semaphore, log_file) for event_id in event_ids]
        for task in tqdm(asyncio.as_completed(tasks), total=total, desc="Processing Event IDs"):
            result = await task
            results.append(result)
            await asyncio.sleep(INITIAL_DELAY)  # Small delay between batches

    # Export results to CSV with proper quoting
    try:
        csv_file = FILE_WATCHER_DIR / f"{timestamp}_MeetMaxURLCheck.csv"
        log_message(f"Exporting results to CSV: {csv_file}", log_file)
        df = pd.DataFrame(results)
        df.to_csv(csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        os.chmod(csv_file, 0o660)  # Set permissions to rw-rw----
        log_message(f"Successfully wrote results to CSV file: {csv_file}", log_file)
    except Exception as e:
        log_message(f"Error writing to CSV file: {str(e)}", log_file)

    log_message(f"Completed: Processed {total} URLs", log_file)

if __name__ == "__main__":
    asyncio.run(meetmax_url_check())