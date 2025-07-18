import sys
from pathlib import Path
sys.path.append(str(Path.home() / 'client_etl_workflow' / 'systemscripts'))
from gmail_inbox_processor import gmail_inbox_processor

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_gmail_inbox_processor.py <config_id>")
        sys.exit(1)
    try:
        config_id = int(sys.argv[1])
        gmail_inbox_processor(config_id=config_id)
    except ValueError:
        print("Error: config_id must be an integer.")
        sys.exit(1)