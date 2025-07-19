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
sys.path.append(str(Path.home() / 'client_etl_workflow'))
from systemscripts.db_config import DB_PARAMS
from systemscripts.log_utils import log_message
from systemscripts.directory_management import ensure_directory_exists
from systemscripts.user_utils import get_username

# Constants
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
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

def fetch_configs(log_file, run_uuid, user, script_start_time):
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT config_id, config_name, gmail_account, subject_pattern, has_attachment, attachment_name_pattern, local_repository_path
                    FROM dba.tinboxconfig
                    WHERE is_active = TRUE;
                """)
                configs = cur.fetchall()
                log_message(log_file, "ConfigFetch", f"Fetched {len(configs)} active configs", run_uuid=run_uuid, stepcounter="ConfigFetch_0", user=user, script_start_time=script_start_time)
                return [{'id': row[0], 'name': row[1], 'account': row[2], 'subject_pattern': row[3], 'has_attachment': row[4], 'attachment_pattern': row[5], 'local_path': row[6]} for row in configs]
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to fetch configs: {str(e)}", run_uuid=run_uuid, stepcounter="ConfigFetch_1", user=user, script_start_time=script_start_time)
        return []

def email_matches_config(message, config):
    subject = next((header['value'] for header in message['payload']['headers'] if header['name'].lower() == 'subject'), '')
    if config['subject_pattern'] and not re.search(config['subject_pattern'], subject, re.IGNORECASE):
        return False

    has_matching_attachment = False
    if 'parts' in message['payload']:
        for part in message['payload']['parts']:
            if part.get('filename'):
                if config['attachment_pattern']:
                    if re.search(config['attachment_pattern'], part['filename'], re.IGNORECASE):
                        has_matching_attachment = True
                        break
                else:
                    has_matching_attachment = True
                    break
    
    if config['has_attachment'] and not has_matching_attachment:
        return False

    return True

def process_email(service, msg_id, config, processed_label_id, log_file, run_uuid, user, script_start_time):
    message = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
    
    # Download raw email as .eml
    raw_msg = service.users().messages().get(userId='me', id=msg_id, format='raw').execute()
    raw_bytes = base64.urlsafe_b64decode(raw_msg['raw'])

    # Define the base directory for saving files, without creating a subdirectory
    save_dir = Path(config['local_path'])
    ensure_directory_exists(save_dir)

    # Save the .eml file directly in the save_dir, named by its unique message ID
    eml_filename = f"{msg_id}.eml"
    eml_path = save_dir / eml_filename
    with open(eml_path, 'wb') as f:
        f.write(raw_bytes)
    os.chmod(eml_path, 0o660)

    # Download matching attachments
    if 'parts' in message['payload']:
        for part in message['payload']['parts']:
            if part.get('filename'):
                original_filename = part['filename']
                if not config['attachment_pattern'] or re.search(config['attachment_pattern'], original_filename, re.IGNORECASE):
                    if 'data' in part['body']:
                        data = part['body']['data']
                    else:
                        att_id = part['body']['attachmentId']
                        att = service.users().messages().attachments().get(userId='me', messageId=msg_id, id=att_id).execute()
                        data = att['data']
                    
                    file_bytes = base64.urlsafe_b64decode(data)
                    
                    # Use the original filename for the attachment
                    att_path = save_dir / original_filename
                    with open(att_path, 'wb') as f:
                        f.write(file_bytes)
                    os.chmod(att_path, 0o660)

    # Move to Processed
    service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds': ['INBOX'], 'addLabelIds': [processed_label_id]}).execute()
    log_message(log_file, "Process", f"Processed email {msg_id}: Saved to {save_dir}", run_uuid=run_uuid, stepcounter=f"Email_{msg_id}", user=user, script_start_time=script_start_time)

def gmail_inbox_processor():
    script_start_time = time.time()
    run_uuid = str(uuid.uuid4())
    user = get_username()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = Path.home() / 'client_etl_workflow' / 'logs' / f"gmail_inbox_processor_{timestamp}"
    
    log_message(log_file, "Initialization", f"Script started at {timestamp}", run_uuid=run_uuid, stepcounter="Init_0", user=user, script_start_time=script_start_time)
    
    service = get_gmail_service(log_file, run_uuid, user, script_start_time)
    if not service:
        return
    
    processed_id = get_or_create_label(service, PROCESSED_LABEL, log_file, run_uuid, user, script_start_time)
    error_id = get_or_create_label(service, ERROR_LABEL, log_file, run_uuid, user, script_start_time)
    if not processed_id or not error_id:
        return
    
    configs = fetch_configs(log_file, run_uuid, user, script_start_time)
    if not configs:
        log_message(log_file, "Warning", "No active inbox configurations found.", run_uuid=run_uuid, stepcounter="Config_Warning", user=user, script_start_time=script_start_time)

    try:
        results = service.users().messages().list(userId='me', q='in:inbox').execute()
        messages = results.get('messages', [])
        log_message(log_file, "Search", f"Found {len(messages)} emails in inbox.", run_uuid=run_uuid, stepcounter="Inbox_Search", user=user, script_start_time=script_start_time)

        for msg in messages:
            msg_id = msg['id']
            message_details = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
            
            matched_config = None
            for config in configs:
                if email_matches_config(message_details, config):
                    matched_config = config
                    break
            
            try:
                if matched_config:
                    process_email(service, msg_id, matched_config, processed_id, log_file, run_uuid, user, script_start_time)
                else:
                    # No config matched, move to ErrorFolder
                    service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds': ['INBOX'], 'addLabelIds': [error_id]}).execute()
                    log_message(log_file, "Process", f"Email {msg_id} did not match any config. Moved to ErrorFolder.", run_uuid=run_uuid, stepcounter=f"Email_{msg_id}_NoMatch", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Failed to process email {msg_id}: {str(e)}", run_uuid=run_uuid, stepcounter=f"Email_{msg_id}_Error", user=user, script_start_time=script_start_time)
                try:
                    service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds': ['INBOX'], 'addLabelIds': [error_id]}).execute()
                except Exception as move_e:
                    log_message(log_file, "Error", f"Failed to move email {msg_id} to ErrorFolder after processing error: {str(move_e)}", run_uuid=run_uuid, stepcounter=f"Email_{msg_id}_MoveError", user=user, script_start_time=script_start_time)

    except HttpError as e:
        log_message(log_file, "Error", f"Failed to list inbox messages: {str(e)}", run_uuid=run_uuid, stepcounter="Inbox_Search_Error", user=user, script_start_time=script_start_time)
    
    log_message(log_file, "Finalization", "Script completed", run_uuid=run_uuid, stepcounter="Final_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    gmail_inbox_processor()