import sys
from pathlib import Path
sys.path.append(str(Path.home() / 'client_etl_workflow' / 'systemscripts'))
from gmail_inbox_processor import gmail_inbox_processor

if __name__ == "__main__":
    gmail_inbox_processor()
