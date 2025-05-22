import sys
import os
from pathlib import Path
import csv
import uuid
import time
import pandas as pd
from datetime import datetime
from user_utils import get_username
from log_utils import log_message
from directory_management import ensure_directory_exists, ROOT_DIR

# Add the root directory to sys.path
sys.path.append(str(Path.home() / 'client_etl_workflow'))

# Configuration
LOG_DIR = ROOT_DIR / 'logs'

def xls_to_csv(input_filepath):
    """Convert an XLS/XLSX file to CSV and save it in the same directory."""
    script_start_time = time.time()
    run_uuid = str(uuid.uuid4())
    user = get_username()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"xls_to_csv_{timestamp}"
    
    # Ensure log directory exists
    ensure_directory_exists(LOG_DIR)
    
    # Log initialization
    log_message(log_file, "Initialization", f"Script started at {timestamp}", 
                run_uuid=run_uuid, stepcounter="Initialization_0", user=user, script_start_time=script_start_time)
    log_message(log_file, "Initialization", f"Input XLS filepath: {input_filepath}", 
                run_uuid=run_uuid, stepcounter="Initialization_1", user=user, script_start_time=script_start_time)
    
    # Validate input filepath
    input_path = Path(input_filepath)
    if not input_path.exists():
        log_message(log_file, "Error", f"Input file does not exist: {input_filepath}", 
                    run_uuid=run_uuid, stepcounter="Validation_0", user=user, script_start_time=script_start_time)
        return
    if not input_path.suffix.lower() in ('.xls', '.xlsx'):
        log_message(log_file, "Error", f"Input file is not an XLS/XLSX file: {input_filepath}", 
                    run_uuid=run_uuid, stepcounter="Validation_1", user=user, script_start_time=script_start_time)
        return
    
    # Define output CSV filepath (same directory, .csv extension)
    output_filepath = input_path.with_suffix('.csv')
    
    log_message(log_file, "Processing", f"Converting {input_filepath} to {output_filepath}", 
                run_uuid=run_uuid, stepcounter="Conversion_0", user=user, script_start_time=script_start_time)
    
    # Convert XLS to CSV
    try:
        # Try openpyxl for .xlsx or modern formats
        engine = "openpyxl"
        try:
            df = pd.read_excel(input_path, engine=engine)
            log_message(log_file, "Conversion", f"Using engine: {engine}", 
                        run_uuid=run_uuid, stepcounter="Conversion_1", user=user, script_start_time=script_start_time)
        except Exception as e:
            # Fall back to xlrd for legacy .xls if available
            engine = "xlrd"
            try:
                df = pd.read_excel(input_path, engine=engine)
                log_message(log_file, "Conversion", f"Using engine: {engine}", 
                            run_uuid=run_uuid, stepcounter="Conversion_1", user=user, script_start_time=script_start_time)
            except ImportError:
                log_message(log_file, "Error", f"Failed to convert {input_filepath}: xlrd not installed for legacy .xls support", 
                            run_uuid=run_uuid, stepcounter="Conversion_2", user=user, script_start_time=script_start_time)
                return
            except Exception as e:
                log_message(log_file, "Error", f"Failed to convert {input_filepath}: {str(e)}", 
                            run_uuid=run_uuid, stepcounter="Conversion_2", user=user, script_start_time=script_start_time)
                return
        
        df.to_csv(output_filepath, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        log_message(log_file, "Conversion", f"CSV file written to {output_filepath}", 
                    run_uuid=run_uuid, stepcounter="Conversion_3", user=user, script_start_time=script_start_time)
        
        # Set file permissions
        try:
            os.chmod(output_filepath, 0o660)
            log_message(log_file, "Conversion", f"Set permissions to 660 on {output_filepath}", 
                        run_uuid=run_uuid, stepcounter="Conversion_4", user=user, script_start_time=script_start_time)
        except Exception as e:
            log_message(log_file, "Error", f"Failed to set permissions on {output_filepath}: {str(e)}", 
                        run_uuid=run_uuid, stepcounter="Conversion_5", user=user, script_start_time=script_start_time)
        
        # Set file ownership to include etl_group
        try:
            import grp
            gid = grp.getgrnam('etl_group').gr_gid
            os.chown(output_filepath, os.getuid(), gid)
            log_message(log_file, "Conversion", f"Set ownership to user {os.getuid()} and group etl_group on {output_filepath}", 
                        run_uuid=run_uuid, stepcounter="Conversion_6", user=user, script_start_time=script_start_time)
        except (AttributeError, KeyError, OSError) as e:
            log_message(log_file, "Warning", f"Failed to set group ownership to etl_group on {output_filepath}: {str(e)}. Proceeding with default ownership.", 
                        run_uuid=run_uuid, stepcounter="Conversion_7", user=user, script_start_time=script_start_time)
            # Log current group information for debugging
            try:
                import subprocess
                groups = subprocess.check_output(['groups'], text=True).strip()
                log_message(log_file, "Debug", f"Current user groups: {groups}", 
                            run_uuid=run_uuid, stepcounter="Conversion_8", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Debug", f"Failed to retrieve user groups: {str(e)}", 
                            run_uuid=run_uuid, stepcounter="Conversion_9", user=user, script_start_time=script_start_time)
    
    except Exception as e:
        log_message(log_file, "Error", f"Failed to convert {input_filepath}: {str(e)}", 
                    run_uuid=run_uuid, stepcounter="Conversion_2", user=user, script_start_time=script_start_time)
        return
    
    log_message(log_file, "Finalization", f"Script completed for {input_filepath}", 
                run_uuid=run_uuid, stepcounter="Finalization_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python xls_to_csv.py <input_filepath>")
        sys.exit(1)
    input_filepath = sys.argv[1]
    xls_to_csv(input_filepath)