import os
import json
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Paths (reuse from test script)
CREDENTIALS_FILE = '/home/yostfundsadmin/client_etl_workflow/systemscripts/credentials.json'
TOKEN_FILE = '/home/yostfundsadmin/client_etl_workflow/systemscripts/token.json'
SCOPES = ['https://mail.google.com/']  # Full access
EMAIL = 'yostfundsdata@gmail.com'
OUTPUT_DIR = '/home/yostfundsadmin/client_etl_workflow/file_watcher/'  # For ETL inputs

def load_credentials():
    """Load client secrets from JSON."""
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")
    with open(CREDENTIALS_FILE, 'r') as f:
        creds_data = json.load(f)['installed']
    return creds_data

def get_or_refresh_credentials():
    """Get or refresh OAuth credentials (reuse from test script)."""
    creds_data = load_credentials()
    credentials = None

    if os.path.exists(TOKEN_FILE):
        print("Status: Token file found. Loading existing credentials.")
        with open(TOKEN_FILE, 'r') as token:
            token_data = json.load(token)
        credentials = Credentials.from_authorized_user_info(token_data, SCOPES)

    if credentials and credentials.valid:
        print("Status: Credentials are valid.")
    elif credentials and credentials.expired and credentials.refresh_token:
        print("Status: Refreshing credentials.")
        credentials.refresh(Request())
        with open(TOKEN_FILE, 'w') as token:
            json.dump(json.loads(credentials.to_json()), token)
        os.chmod(TOKEN_FILE, 0o600)
    else:
        # Fallback to manual flow if needed (should be rare post-setup)
        print("Status: Starting manual authorization flow.")
        flow = InstalledAppFlow.from_client_config({'installed': creds_data}, SCOPES)
        flow.redirect_uri = 'http://localhost:8080'
        auth_url, _ = flow.authorization_url(prompt='consent')
        print(f"Authorization URL: {auth_url}")
        redirect_url = input("Paste the full redirect URL here: ").strip()
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(redirect_url)
        code = parse_qs(parsed.query)['code'][0]
        flow.fetch_token(code=code)
        credentials = flow.credentials
        with open(TOKEN_FILE, 'w') as token:
            json.dump(json.loads(credentials.to_json()), token)
        os.chmod(TOKEN_FILE, 0o600)

    return credentials

def read_emails(max_results=5, query='is:unread'):
    """Read recent emails via Gmail API, extract content/attachments, save XLSX/XLS."""
    credentials = get_or_refresh_credentials()
    service = build('gmail', 'v1', credentials=credentials)
    results = service.users().messages().list(userId='me', q=query, maxResults=max_results).execute()
    messages = results.get('messages', [])
    if not messages:
        print("No emails found.")
        return

    for msg in messages:
        email_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        print(f"Processing email ID: {msg['id']}")
        
        # Extract subject/body
        payload = email_data['payload']
        headers = payload['headers']
        subject = next(h['value'] for h in headers if h['name'] == 'Subject')
        print(f"Subject: {subject}")
        
        # Body (simplified; handle multipart if needed)
        if 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] == 'text/plain':
                    body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                    print(f"Body excerpt: {body[:100]}...")

        # Attachments (save XLSX/XLS)
        if 'parts' in payload:
            for part in payload['parts']:
                if 'filename' in part and part['filename'].lower().endswith(('.xlsx', '.xls')):
                    attach_id = part['body']['attachmentId']
                    attach_data = service.users().messages().attachments().get(userId='me', messageId=msg['id'], id=attach_id).execute()
                    file_data = base64.urlsafe_b64decode(attach_data['data'])
                    output_path = os.path.join(OUTPUT_DIR, part['filename'])
                    with open(output_path, 'wb') as f:
                        f.write(file_data)
                    os.chmod(output_path, 0o660)
                    print(f"Saved attachment: {output_path} for ETL processing.")

if __name__ == "__main__":
    # Example: Read 5 unread emails
    read_emails(max_results=5, query='is:unread')