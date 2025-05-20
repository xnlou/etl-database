import sys
import os
import re
import glob
import uuid
import time
import pandas as pd
import psycopg2
from pathlib import Path
# Add root directory to sys.path
sys.path.append(str(Path(__file__).parent.parent))
from datetime import datetime
import csv
import shutil
from systemscripts.user_utils import get_username
from systemscripts.log_utils import log_message
from systemscripts.directory_management import ensure_directory_exists, LOG_DIR, FILE_WATCHER_DIR
from systemscripts.xls_to_csv import xls_to_csv



# Database connection parameters
DB_PARAMS = {
    "dbname": "Feeds",
    "user": "yostfundsadmin",
    "password": "etlserver2025!",
    "host": "localhost"
}

def get_config(config_id, log_file, run_uuid, user, script_start_time):
    """Retrieve import configuration from timportconfig table."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT config_name, DataSource, DataSetType, source_directory, archive_directory,
                           file_pattern, file_type, metadata_label_source, metadata_label_location,
                           DateConfig, DateLocation, delimiter, target_table, is_active
                    FROM dba.timportconfig
                    WHERE config_id = %s AND is_active = '1';
                """, (config_id,))
                config = cur.fetchone()
                if not config:
                    log_message(log_file, "Error", f"No active configuration found for config_id {config_id}",
                                run_uuid=run_uuid, stepcounter="ConfigFetch_0", user=user, script_start_time=script_start_time)
                    return None
                return {
                    "config_name": config[0],
                    "DataSource": config[1],
                    "DataSetType": config[2],
                    "source_directory": config[3],
                    "archive_directory": config[4],
                    "file_pattern": config[5],
                    "file_type": config[6],
                    "metadata_label_source": config[7],
                    "metadata_label_location": config[8],
                    "DateConfig": config[9],
                    "DateLocation": config[10],
                    "delimiter": config[11],
                    "target_table": config[12]
                }
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Database error fetching config_id {config_id}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="ConfigFetch_1", user=user, script_start_time=script_start_time)
        return None

def parse_metadata(filename, config, field_source, field_location, delimiter, log_file, run_uuid, user, script_start_time):
    """Parse metadata label or date based on configuration."""
    if field_source == "filename":
        if not delimiter or not field_location:
            log_message(log_file, "Error", f"Invalid {field_source} configuration: delimiter or location missing",
                        run_uuid=run_uuid, stepcounter="MetadataParse_0", user=user, script_start_time=script_start_time)
            return None
        try:
            match = re.search(field_location, filename)
            if match:
                return match.group(1) if match.groups() else match.group(0)
            log_message(log_file, "Error", f"No match for {field_location} in filename {filename}",
                        run_uuid=run_uuid, stepcounter="MetadataParse_1", user=user, script_start_time=script_start_time)
            return None
        except re.error as e:
            log_message(log_file, "Error", f"Regex error for {field_location}: {str(e)}",
                        run_uuid=run_uuid, stepcounter="MetadataParse_2", user=user, script_start_time=script_start_time)
            return None
    elif field_source == "static":
        return field_location
    elif field_source == "file_content":
        return field_location  # Will be handled during data loading
    return None

def load_data_to_postgres(df, target_table, metadata_label, event_date, log_file, run_uuid, user, script_start_time):
    """Load DataFrame to PostgreSQL table with metadata and date."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # Get table columns
                cur.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema || '.' || table_name = %s
                    ORDER BY ordinal_position;
                """, (target_table,))
                columns = [row[0] for row in cur.fetchall()]
                
                # Add metadata and date columns if configured
                if metadata_label and "metadata_label" in columns:
                    df["metadata_label"] = metadata_label
                if event_date and "event_date" in columns:
                    df["event_date"] = event_date
                
                # Filter DataFrame to match table columns
                df = df[[col for col in df.columns if col in columns]]
                
                # Convert DataFrame to list of tuples
                records = [tuple(row) for row in df.to_numpy()]
                
                # Prepare insert query
                placeholders = ",".join(["%s"] * len(df.columns))
                insert_query = f"""
                    INSERT INTO {target_table} ({','.join(df.columns)})
                    VALUES ({placeholders})
                """
                
                # Execute insert
                cur.executemany(insert_query, records)
                conn.commit()
                log_message(log_file, "DataLoad", f"Loaded {len(records)} rows to {target_table}",
                            run_uuid=run_uuid, stepcounter="DataLoad_0", user=user, script_start_time=script_start_time)
                return True
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Database error loading to {target_table}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="DataLoad_1", user=user, script_start_time=script_start_time)
        return False

def generic_import(config_id):
    """Generic import script to process files based on timportconfig."""
    script_start_time = time.time()
    run_uuid = str(uuid.uuid4())
    user = get_username()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"generic_import_{timestamp}"

    # Ensure log directory exists
    ensure_directory_exists(LOG_DIR)

    log_message(log_file, "Initialization", f"Script started at {timestamp} for config_id {config_id}",
                run_uuid=run_uuid, stepcounter="Initialization_0", user=user, script_start_time=script_start_time)

    # Fetch configuration
    config = get_config(config_id, log_file, run_uuid, user, script_start_time)
    if not config:
        log_message(log_file, "Error", "Failed to retrieve configuration. Exiting.",
                    run_uuid=run_uuid, stepcounter="Initialization_1", user=user, script_start_time=script_start_time)
        return

    log_message(log_file, "Initialization", f"Configuration loaded: {config['config_name']}",
                run_uuid=run_uuid, stepcounter="Initialization_2", user=user, script_start_time=script_start_time)

    # Ensure directories exist
    ensure_directory_exists(config["source_directory"])
    ensure_directory_exists(config["archive_directory"])

    # Find matching files
    file_pattern = os.path.join(config["source_directory"], config["file_pattern"])
    files = glob.glob(file_pattern)
    if not files:
        log_message(log_file, "Warning", f"No files found matching {file_pattern}",
                    run_uuid=run_uuid, stepcounter="FileSearch_0", user=user, script_start_time=script_start_time)
        return

    log_message(log_file, "Processing", f"Found {len(files)} files to process",
                run_uuid=run_uuid, stepcounter="FileSearch_1", user=user, script_start_time=script_start_time)

    for file_path in files:
        filename = os.path.basename(file_path)
        log_message(log_file, "Processing", f"Processing file: {filename}",
                    run_uuid=run_uuid, stepcounter=f"File_{filename}_0", user=user, script_start_time=script_start_time)

        # Convert XLS/XLSX to CSV if needed
        csv_path = file_path
        if config["file_type"] in ["XLS", "XLSX"]:
            csv_path = os.path.join(FILE_WATCHER_DIR, f"{timestamp}_{filename}.csv")
            try:
                xls_to_csv(file_path)
                if not os.path.exists(csv_path):
                    log_message(log_file, "Error", f"Failed to convert {filename} to CSV",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_1", user=user, script_start_time=script_start_time)
                    continue
                log_message(log_file, "Conversion", f"Converted {filename} to {csv_path}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_2", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Conversion error for {filename}: {str(e)}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_3", user=user, script_start_time=script_start_time)
                continue

        # Parse metadata
        metadata_label = parse_metadata(filename, config, config["metadata_label_source"],
                                       config["metadata_label_location"], config["delimiter"],
                                       log_file, run_uuid, user, script_start_time)
        event_date = parse_metadata(filename, config, config["DateConfig"],
                                    config["DateLocation"], config["delimiter"],
                                    log_file, run_uuid, user, script_start_time)

        # Read CSV
        try:
            df = pd.read_csv(csv_path)
            log_message(log_file, "Processing", f"Read {len(df)} rows from {csv_path}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_4", user=user, script_start_time=script_start_time)
        except Exception as e:
            log_message(log_file, "Error", f"Failed to read CSV {csv_path}: {str(e)}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_5", user=user, script_start_time=script_start_time)
            if csv_path != file_path:
                os.remove(csv_path)  # Clean up temporary CSV
            continue

        # Load data to PostgreSQL
        if load_data_to_postgres(df, config["target_table"], metadata_label, event_date,
                                log_file, run_uuid, user, script_start_time):
            # Move file to archive
            archive_path = os.path.join(config["archive_directory"], filename)
            try:
                shutil.move(file_path, archive_path)
                os.chmod(archive_path, 0o660)
                os.chown(archive_path, os.getuid(), os.getgrnam('etl_group').gr_gid)
                log_message(log_file, "Processing", f"Moved {filename} to {archive_path}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_6", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Failed to move {filename} to archive: {str(e)}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_7", user=user, script_start_time=script_start_time)
        else:
            log_message(log_file, "Error", f"Failed to load data from {filename} to {config['target_table']}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_8", user=user, script_start_time=script_start_time)

        # Clean up temporary CSV if created
        if csv_path != file_path and os.path.exists(csv_path):
            os.remove(csv_path)

    log_message(log_file, "Finalization", f"Completed processing for config_id {config_id}",
                run_uuid=run_uuid, stepcounter="Finalization_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generic_import.py <config_id>")
        sys.exit(1)
    config_id = int(sys.argv[1])
    generic_import(config_id)