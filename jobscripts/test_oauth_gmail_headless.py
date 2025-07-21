import smtplib
import os
import json
from urllib.parse import urlparse, parse_qs
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build  # For Gmail API reading

# Paths
CREDENTIALS_FILE = '/home/yostfundsadmin/client_etl_workflow/systemscripts/credentials.json'
TOKEN_FILE = '/home/yostfundsadmin/client_etl_workflow/systemscripts/token.json'  # Securely store refresh_token here
SCOPES = ['https://mail.google.com/']  # Full Gmail access (read/send)
EMAIL = 'yostfundsdata@gmail.com'  # Your Gmail address
LOCALHOST_PORT = 8080  # Arbitrary port for loopback

def load_credentials():
    """Load client secrets from JSON."""
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")
    with open(CREDENTIALS_FILE, 'r') as f:
        creds_data = json.load(f)['installed']
    return creds_data

def get_or_refresh_credentials():
    """Get or refresh OAuth credentials; headless loopback flow if token missing."""
    creds_data = load_credentials()
    credentials = None

    # Load existing token if available
    if os.path.exists(TOKEN_FILE):
        print("Status: Token file found. Loading existing credentials.")
        with open(TOKEN_FILE, 'r') as token:
            token_data = json.load(token)
        credentials = Credentials.from_authorized_user_info(token_data, SCOPES)
    else:
        print("Status: No token file found. Initiating headless loopback authorization flow (one-time only).")

    # Check validity and refresh if needed
    if credentials and credentials.valid:
        print("Status: Credentials are valid. No refresh or auth needed.")
    elif credentials and credentials.expired and credentials.refresh_token:
        print("Status: Credentials expired. Attempting automatic refresh.")
        try:
            credentials.refresh(Request())
            print("Status: Refresh successful.")
            # Update token file
            with open(TOKEN_FILE, 'w') as token:
                json.dump(json.loads(credentials.to_json()), token)  # Parse str to dict for dump
            os.chmod(TOKEN_FILE, 0o600)
        except Exception as e:
            print(f"Status: Refresh failed: {str(e)}. Falling back to manual auth.")
            credentials = None
    else:
        print("Status: No valid credentials. Starting headless loopback authorization flow (one-time consent).")
        print("Note: Visit the URL, consent, then copy the FULL redirect URL from the browser address bar (it will show a connection error page).")
        flow = InstalledAppFlow.from_client_config(
            {'installed': creds_data},
            SCOPES
        )
        flow.redirect_uri = f'http://localhost:{LOCALHOST_PORT}'
        auth_url, _ = flow.authorization_url(prompt='consent')
        print(f"Authorization URL: {auth_url}")
        redirect_url = input("Paste the full redirect URL here (after consent): ").strip()
        # Extract code from redirect URL
        try:
            parsed_url = urlparse(redirect_url)
            code = parse_qs(parsed_url.query)['code'][0]
            flow.fetch_token(code=code)
            credentials = flow.credentials
            print("Status: Token fetched successfully from redirect code.")
            # Save token
            with open(TOKEN_FILE, 'w') as token:
                json.dump(json.loads(credentials.to_json()), token)  # Parse str to dict for dump
            os.chmod(TOKEN_FILE, 0o600)
        except Exception as e:
            print(f"Status: Failed to extract/fetch token from redirect URL: {str(e)}")
            return None

    return credentials

def test_oauth_smtp_send():
    """Test OAuth2 for sending (SMTP)."""
    try:
        credentials = get_or_refresh_credentials()
        if not credentials:
            print("Status: Failed to obtain credentials for sending.")
            return False

        print("Status: Testing SMTP send authentication.")
        auth_string = f'user={EMAIL}\x01auth=Bearer {credentials.token}\x01\x01'.encode('ascii')
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.ehlo()
        response = server.docmd('AUTH', 'XOAUTH2 ' + auth_string.decode('ascii'))
        if response[0] == 235:
            print("Status: SMTP authentication successful (ready to send emails).")
        else:
            print(f"Status: SMTP authentication response: {response}")
        server.quit()
        return True
    except Exception as e:
        print(f"Status: SMTP send test failed: {str(e)}")
        return False

def test_oauth_gmail_read():
    """Test OAuth2 for reading emails (Gmail API)."""
    try:
        credentials = get_or_refresh_credentials()
        if not credentials:
            print("Status: Failed to obtain credentials for reading.")
            return False

        print("Status: Testing Gmail API read (listing 5 recent emails).")
        service = build('gmail', 'v1', credentials=credentials)
        results = service.users().messages().list(userId='me', maxResults=5).execute()
        messages = results.get('messages', [])
        if messages:
            print("Status: Gmail read successful. Recent email IDs:")
            for msg in messages:
                print(f" - {msg['id']}")
        else:
            print("Status: Gmail read successful, but no recent emails found.")
        return True
    except Exception as e:
        print(f"Status: Gmail read test failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_oauth_smtp_send()
    test_oauth_gmail_read()