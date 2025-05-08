import time
import requests
from requests.exceptions import RequestException

def fetch_url(session, url, retries=5, initial_delay=5.0, headers=None):
    """Fetch a URL with retry logic for handling errors."""
    delay = initial_delay
    for attempt in range(retries):
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= 2  # Exponential backoff
    return None