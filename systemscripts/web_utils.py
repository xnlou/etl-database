import time
import requests
from requests.exceptions import RequestException, HTTPError

def fetch_url(session, url, retries=5, initial_delay=5.0, headers=None, log_file=None, run_uuid=None, user=None, script_start_time=None):
    """Fetch a URL with retry logic for handling errors, including rate limiting."""
    delay = initial_delay
    for attempt in range(retries):
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except HTTPError as e:
            if e.response.status_code == 429 and log_file is not None:
                from systemscripts.log_utils import log_message
                log_message(log_file, "RateLimit", f"Rate limit hit for {url}, attempt {attempt + 1}/{retries}, waiting {delay}s", run_uuid=run_uuid, stepcounter=f"fetch_{url}", user=user, script_start_time=script_start_time)
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= 2  # Exponential backoff
        except RequestException as e:
            if log_file is not None:
                from systemscripts.log_utils import log_message
                log_message(log_file, "Error", f"Request error for {url}, attempt {attempt + 1}/{retries}: {str(e)}", run_uuid=run_uuid, stepcounter=f"fetch_{url}", user=user, script_start_time=script_start_time)
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= 2
    return None