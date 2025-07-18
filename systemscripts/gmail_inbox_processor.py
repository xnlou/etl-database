import sys
import os
import base64
import re
import uuid
import time
from datetime import datetime
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import psycopg2
from psycopg2 import sql
sys.path.append(str(Path.home() / 'client_etl_workflow'))
from systemscripts.db_config import DB_PARAMS
from systemscripts.log_utils import log_message
from systemscripts.directory_management import ensure_directory_exists
from systemscripts.user_utils import get_username

# Constants
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']  # For read and modify (labels)
CREDENTIALS_FILE = Path(__file__).parent / 'credentials.json'
TOKEN_FILE = Path(__file__).parent / 'token.json'
PROCESSED_LABEL = 'Processed'
ERROR_LABEL = 'ErrorFolder'
MAX_RETRIES = 3
INITIAL_DELAY = 5.0

def get_gmail_service(log_file, run_uuid, user, script_start_time):
    """Authenticate and return Gmail API service."""
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
        os.chmod(TOKEN_FILE, 0o600)
    try:
        service = build('gmail', 'v1', credentials=creds)
        log_message(log_file, "Auth", "Gmail API authenticated successfully", run_uuid=run_uuid, stepcounter="Auth_0", user=user, script_start_time=script_start_time)
        return service
    except HttpError as e:
        log_message(log_file, "Error", f"Gmail API authentication failed: {str(e)}", run_uuid=run_uuid, stepcounter="Auth_1", user=user, script_start_time=script_start_time)
        return None

def get_or_create_label(service, label_name, log_file, run_uuid, user, script_start_time):
    """Get label ID, create if not exists."""
    try:
        labels = service.users().labels().list(userId='me').execute().get('labels', [])
        for label in labels:
            if label['name'].upper() == label_name.upper():
                return label['id']
        # Create if not found
        new_label = service.users().labels().create(userId='me', body={'name': label_name, 'labelListVisibility': 'labelShow', 'messageListVisibility': 'show'}).execute()
        log_message(log_file, "Label", f"Created label {label_name} with ID {new_label['id']}", run_uuid=run_uuid, stepcounter="Label_Create", user=user, script_start_time=script_start_time)
        return new_label['id']
    except HttpError as e:
        log_message(log_file, "Error", f"Failed to get/create label {label_name}: {str(e)}", run_uuid=run_uuid, stepcounter="Label_Error", user=user, script_start_time=script_start_time)
        return None

def fetch_configs(log_file, run_uuid, user, script_start_time, config_id=None):
    """Fetch active configs from dba.tinboxconfig, optionally for a specific config_id."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                if config_id:
                    cur.execute("""
                        SELECT config_id, config_name, gmail_account, subject_pattern, has_attachment, attachment_name_pattern, local_repository_path
                        FROM dba.tinboxconfig
                        WHERE config_id = %s AND is_active = TRUE;
                    """, (config_id,))
                else:
                    cur.execute("""
                        SELECT config_id, config_name, gmail_account, subject_pattern, has_attachment, attachment_name_pattern, local_repository_path
                        FROM dba.tinboxconfig
                        WHERE is_active = TRUE;
                    """)
                configs = cur.fetchall()
                fetched_count = len(configs)
                step = f"ConfigFetch_{config_id}" if config_id else "ConfigFetch_0"
                log_message(log_file, "ConfigFetch", f"Fetched {fetched_count} active configs{' for ID ' + str(config_id) if config_id else ''}", run_uuid=run_uuid, stepcounter=step, user=user, script_start_time=script_start_time)
                return [{'id': row[0], 'name': row[1], 'account': row[2], 'subject_pattern': row[3], 'has_attachment': row[4], 'attachment_pattern': row[5], 'local_path': row[6]} for row in configs]
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to fetch configs{' for ID ' + str(config_id) if config_id else ''}: {str(e)}", run_uuid=run_uuid, stepcounter="ConfigFetch_1", user=user, script_start_time=script_start_time)
        return []

def process_email(service, msg_id, config, processed_label_id, error_label_id, log_file, run_uuid, user, script_start_time):
    """Fetch, validate, download, and move email."""
    try:
        message = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
        subject = next((header['value'] for header in message['payload']['headers'] if header['name'] == 'Subject'), '')
        
        # Validate subject (using re.search for matches anywhere, case-insensitive)
        if config['subject_pattern'] and not re.search(config['subject_pattern'], subject, re.IGNORECASE):
            raise ValueError(f"Subject '{subject}' does not match pattern {config['subject_pattern']}")
        
        # Get attachments
        attachments = []
        has_valid_attachment = False
        if 'parts' in message['payload']:
            for part in message['payload']['parts']:
                if part['filename']:
                    filename = part['filename']
                    # Validate attachment name (using re.search for matches anywhere, case-insensitive)
                    if config['attachment_pattern'] and not re.search(config['attachment_pattern'], filename, re.IGNORECASE):
                        continue  # Skip non-matching
                    if 'data' in part['body']:
                        data = part['body']['data']
                    else:
                        att_id = part['body']['attachmentId']
                        att = service.users().messages().attachments().get(userId='me', messageId=msg_id, id=att_id).execute()
                        data = att['data']
                    attachments.append((filename, data))
                    has_valid_attachment = True
        
        if config['has_attachment'] and not has_valid_attachment:
            raise ValueError("No valid attachment found")
        
        # Download raw email as .eml
        raw_msg = service.users().messages().get(userId='me', id=msg_id, format='raw').execute()
        raw_bytes = base64.urlsafe_b64decode(raw_msg['raw'])
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        email_dir = Path(config['local_path']) / f"{timestamp}_{msg_id}"
        ensure_directory_exists(email_dir)
        eml_path = email_dir / f"{msg_id}.eml"
        with open(eml_path, 'wb') as f:
            f.write(raw_bytes)
        os.chmod(eml_path, 0o660)
        
        # Download attachments
        for filename, data in attachments:
            file_bytes = base64.urlsafe_b64decode(data)
            att_path = email_dir / filename
            with open(att_path, 'wb') as f:
                f.write(file_bytes)
            os.chmod(att_path, 0o660)
        
        # Move to Processed
        service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds': ['INBOX'], 'addLabelIds': [processed_label_id]}).execute()
        log_message(log_file, "Process", f"Processed email {msg_id}: Saved to {email_dir}", run_uuid=run_uuid, stepcounter=f"Email_{msg_id}", user=user, script_start_time=script_start_time)
        return True
    except Exception as e:
        # Move to ErrorFolder
        service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds': ['INBOX'], 'addLabelIds': [error_label_id]}).execute()
        log_message(log_file, "Error", f"Failed to process email {msg_id}: {str(e)} - Moved to ErrorFolder", run_uuid=run_uuid, stepcounter=f"Email_{msg_id}_Error", user=user, script_start_time=script_start_time)
        return False

def gmail_inbox_processor(config_id=None):
    script_start_time = time.time()
    run_uuid = str(uuid.uuid4())
    user = get_username()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = Path.home() / 'client_etl_workflow' / 'logs' / f"gmail_inbox_processor_{timestamp}"
    
    log_message(log_file, "Initialization", f"Script started at {timestamp}{' for config_id ' + str(config_id) if config_id else ''}", run_uuid=run_uuid, stepcounter="Init_0", user=user, script_start_time=script_start_time)
    
    service = get_gmail_service(log_file, run_uuid, user, script_start_time)
    if not service:
        return
    
    processed_id = get_or_create_label(service, PROCESSED_LABEL, log_file, run_uuid, user, script_start_time)
    error_id = get_or_create_label(service, ERROR_LABEL, log_file, run_uuid, user, script_start_time)
    if not processed_id or not error_id:
        return
    
    configs = fetch_configs(log_file, run_uuid, user, script_start_time, config_id=config_id)
    for config in configs:
        # Note: 'account' field is informational (for DB clarity); API uses authenticated 'me' for security.
        log_message(log_file, "Config", f"Processing config {config['name']} for {config['account']}", run_uuid=run_uuid, stepcounter=f"Config_{config['id']}", user=user, script_start_time=script_start_time)
        
        query = 'in:inbox'
        if config['subject_pattern']:
            query += f" subject:\"{config['subject_pattern']}\""
        if config['has_attachment']:
            query += ' has:attachment'
        try:
            results = service.users().messages().list(userId='me', q=query).execute()
            messages = results.get('messages', [])
            log_message(log_file, "Search", f"Found {len(messages)} matching emails for config {config['name']}", run_uuid=run_uuid, stepcounter=f"Search_{config['id']}", user=user, script_start_time=script_start_time)
            
            for msg in messages:
                for attempt in range(MAX_RETRIES):
                    try:
                        process_email(service, msg['id'], config, processed_id, error_id, log_file, run_uuid, user, script_start_time)
                        break
                    except HttpError as e:
                        if attempt == MAX_RETRIES - 1:
                            raise
                        time.sleep(INITIAL_DELAY * (2 ** attempt))
        except HttpError as e:
            log_message(log_file, "Error", f"Search failed for config {config['name']}: {str(e)}", run_uuid=run_uuid, stepcounter=f"Search_Error_{config['id']}", user=user, script_start_time=script_start_time)
    
    log_message(log_file, "Finalization", "Script completed", run_uuid=run_uuid, stepcounter="Final_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    config_id = int(sys.argv[1]) if len(sys.argv) > 1 else None
    gmail_inbox_processor(config_id=config_id)