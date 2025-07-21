import os
import base64
import psycopg2
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from systemscripts.db_config import DB_PARAMS

# --- Gmail API Setup ---
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

def get_gmail_service():
    """Authenticates with the Gmail API and returns a service object."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def get_inbox_configs():
    """Fetches active inbox processing configurations from the database."""
    configs = []
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM dba.tinboxconfig WHERE is_active = TRUE;")
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                for row in rows:
                    configs.append(dict(zip(columns, row)))
    except psycopg2.Error as e:
        print(f"Database error fetching inbox configs: {e}")
    return configs

def search_emails(service, query):
    """Searches for emails matching the given query."""
    try:
        response = service.users().messages().list(userId='me', q=query).execute()
        return response.get('messages', [])
    except Exception as e:
        print(f"An error occurred while searching for emails: {e}")
        return []

def process_email(service, msg_id, config):
    """
    Processes a single email: downloads content and attachments, and moves the email.
    (Placeholder for detailed implementation)
    """
    print(f"Processing email with ID: {msg_id} for config: {config['config_name']}")
    # 1. Get the full email details.
    # 2. Check if it matches all the criteria in the config.
    # 3. If it has an attachment, download it to config['download_location'].
    # 4. Download the email body and save it as a .eml or .txt file.
    # 5. Move the email to the 'processed' or 'errorprocessed' label in Gmail.
    # This is a placeholder for the full implementation.
    pass

def main():
    """Main function to orchestrate the inbox processing."""
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"Error: {CREDENTIALS_FILE} not found. Please create it from the Google Cloud Console.")
        return

    service = get_gmail_service()
    configs = get_inbox_configs()

    for config in configs:
        query_parts = []
        if config.get('subject_filter'):
            query_parts.append(f"subject:({config['subject_filter']})")
        if config.get('sender_filter'):
            query_parts.append(f"from:({config['sender_filter']})")
        if config.get('has_attachment_filter'):
            query_parts.append("has:attachment")
        
        query = " ".join(query_parts)
        if not query:
            print(f"Skipping config '{config['config_name']}' because it has no filters.")
            continue

        print(f"Searching for emails with query: {query}")
        messages = search_emails(service, query)
        
        if not messages:
            print("No matching emails found.")
            continue

        for msg in messages:
            process_email(service, msg['id'], config)

if __name__ == '__main__':
    main()
