import sys
import os
import re
import glob
import uuid
import time
import pandas as pd
import psycopg2
from psycopg2 import sql
from pathlib import Path
# Add root directory to sys.path
sys.path.append(str(Path(__file__).parent.parent))
from datetime import datetime
import csv
import shutil
import traceback
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

def log_to_tlogentry(config_id, message, stepcounter, log_file, run_uuid, user, script_start_time):
    """Insert a log entry into dba.tlogentry."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dba.tlogentry (timestamp, run_uuid, process_type, stepcounter, user_name, message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (datetime.now(), run_uuid, 'Import', stepcounter, user, message))
                conn.commit()
                log_message(log_file, "Logentry", f"Logged to tlogentry: stepcounter={stepcounter}, message={message}",
                            run_uuid=run_uuid, stepcounter="Logentry_Insert", user=user, script_start_time=script_start_time)
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to log to tlogentry: {str(e)}\n{traceback.format_exc()}",
                    run_uuid=run_uuid, stepcounter="Logentry_Error", user=user, script_start_time=script_start_time)

def get_config(config_id, log_file, run_uuid, user, script_start_time):
    """Retrieve import configuration from timportconfig table."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT config_name, DataSource, DataSetType, source_directory, archive_directory,
                           file_pattern, file_type, metadata_label_source, metadata_label_location,
                           DateConfig, DateLocation, delimiter, target_table, importstrategyid, is_active, DateFormat
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
                    "target_table": config[12],
                    "importstrategyid": config[13],
                    "DateFormat": config[15]
                }
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Database error fetching config_id {config_id}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="ConfigFetch_1", user=user, script_start_time=script_start_time)
        return None

def parse_metadata(filename, config, field_source, field_location, delimiter, log_file, run_uuid, user, script_start_time, date_format=None):
    """Parse metadata label or date based on configuration."""
    if field_source == "filename":
        if not delimiter or field_location is None:
            log_message(log_file, "Error", f"Invalid {field_source} configuration: delimiter or location missing",
                        run_uuid=run_uuid, stepcounter="MetadataParse_0", user=user, script_start_time=script_start_time)
            return None
        try:
            # Split filename on delimiter
            parts = filename.split(delimiter)
            location = int(field_location)
            if location < 0 or location >= len(parts):
                log_message(log_file, "Error", f"Invalid DateLocation {field_location} for filename '{filename}' with {len(parts)} parts",
                            run_uuid=run_uuid, stepcounter="MetadataParse_1", user=user, script_start_time=script_start_time)
                return None
            value = parts[location]
            # If parsing a date, apply date_format
            if date_format:
                try:
                    # Convert DateFormat to Python strptime format
                    python_format = date_format.replace('yyyy', '%Y').replace('MM', '%m').replace('dd', '%d')\
                                              .replace('HH', '%H').replace('mm', '%M').replace('ss', '%S').replace('T', 'T')
                    parsed_date = datetime.strptime(value, python_format).date()
                    return parsed_date
                except ValueError as e:
                    log_message(log_file, "Error", f"Failed to parse date '{value}' with format '{date_format}': {str(e)}",
                                run_uuid=run_uuid, stepcounter="MetadataParse_2", user=user, script_start_time=script_start_time)
                    return None
            return value
        except ValueError as e:
            log_message(log_file, "Error", f"Invalid DateLocation value '{field_location}': {str(e)}",
                        run_uuid=run_uuid, stepcounter="MetadataParse_3", user=user, script_start_time=script_start_time)
            return None
    elif field_source == "static":
        if date_format:
            try:
                python_format = date_format.replace('yyyy', '%Y').replace('MM', '%m').replace('dd', '%d')\
                                          .replace('HH', '%H').replace('mm', '%M').replace('ss', '%S').replace('T', 'T')
                return datetime.strptime(field_location, python_format).date()
            except ValueError as e:
                log_message(log_file, "Error", f"Failed to parse static date '{field_location}' with format '{date_format}': {str(e)}",
                            run_uuid=run_uuid, stepcounter="MetadataParse_4", user=user, script_start_time=script_start_time)
                return None
        return field_location
    elif field_source == "file_content":
        if date_format:
            try:
                python_format = date_format.replace('yyyy', '%Y').replace('MM', '%m').replace('dd', '%d')\
                                          .replace('HH', '%H').replace('mm', '%M').replace('ss', '%S').replace('T', 'T')
                return datetime.strptime(field_location, python_format).date()
            except ValueError as e:
                log_message(log_file, "Error", f"Failed to parse file_content date '{field_location}' with format '{date_format}': {str(e)}",
                            run_uuid=run_uuid, stepcounter="MetadataParse_5", user=user, script_start_time=script_start_time)
                return None
        return field_location
    return None

def table_exists(cursor, table_name, log_file, run_uuid, user, script_start_time):
    """Check if a table exists in the database."""
    try:
        schema, table = table_name.split('.')
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """, (schema, table))
        exists = cursor.fetchone()[0]
        log_message(log_file, "TableCheck", f"Table {table_name} exists: {exists}",
                    run_uuid=run_uuid, stepcounter="TableCheck_0", user=user, script_start_time=script_start_time)
        return exists
    except Exception as e:
        log_message(log_file, "Error", f"Failed to check table existence for {table_name}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="TableCheck_1", user=user, script_start_time=script_start_time)
        return False

def get_table_columns(cursor, table_name, log_file, run_uuid, user, script_start_time):
    """Get the columns of a table."""
    try:
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema || '.' || table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        columns = [row[0] for row in cursor.fetchall()]
        log_message(log_file, "TableColumns", f"Columns for {table_name}: {', '.join(columns)}",
                    run_uuid=run_uuid, stepcounter="TableColumns_0", user=user, script_start_time=script_start_time)
        return columns
    except Exception as e:
        log_message(log_file, "Error", f"Failed to get columns for {table_name}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="TableColumns_1", user=user, script_start_time=script_start_time)
        return []

def get_column_lengths(df):
    """Determine the maximum length of data in each column."""
    lengths = {}
    for col in df.columns:
        max_length = df[col].astype(str).str.len().max()
        lengths[col] = max_length if not pd.isna(max_length) else 255
    return lengths

def ensure_lookup_ids(cursor, datasource, dataset_type, user, log_file, run_uuid, script_start_time):
    """Ensure DataSource and DataSetType exist in tDataSource and tDataSetType, inserting if necessary."""
    try:
        # Ensure DataSource exists
        cursor.execute("""
            INSERT INTO dba.tdatasource (sourcename, createddate, createdby)
            VALUES (%s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (sourcename) DO NOTHING
            RETURNING datasourceid;
        """, (datasource, user))
        row = cursor.fetchone()
        if row:
            datasource_id = row[0]
        else:
            cursor.execute("""
                SELECT datasourceid
                FROM dba.tdatasource
                WHERE sourcename = %s;
            """, (datasource,))
            datasource_id = cursor.fetchone()[0]
        log_message(log_file, "LookupInsert", f"Ensured DataSource {datasource} with DataSourceID {datasource_id}",
                    run_uuid=run_uuid, stepcounter="LookupInsert_0", user=user, script_start_time=script_start_time)

        # Ensure DataSetType exists
        cursor.execute("""
            INSERT INTO dba.tdatasettype (typename, createddate, createdby)
            VALUES (%s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (typename) DO NOTHING
            RETURNING datasettypeid;
        """, (dataset_type, user))
        row = cursor.fetchone()
        if row:
            dataset_type_id = row[0]
        else:
            cursor.execute("""
                SELECT datasettypeid
                FROM dba.tdatasettype
                WHERE typename = %s;
            """, (dataset_type,))
            dataset_type_id = cursor.fetchone()[0]
        log_message(log_file, "LookupInsert", f"Ensured DataSetType {dataset_type} with DataSetTypeID {dataset_type_id}",
                    run_uuid=run_uuid, stepcounter="LookupInsert_1", user=user, script_start_time=script_start_time)

        return datasource_id, dataset_type_id
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to ensure lookup IDs: {str(e)}",
                    run_uuid=run_uuid, stepcounter="LookupInsert_2", user=user, script_start_time=script_start_time)
        return None, None

def add_columns_to_table(cursor, table_name, new_columns, column_lengths, log_file, run_uuid, user, script_start_time):
    """Add new columns to the target table with appropriate VARCHAR length."""
    try:
        for column in new_columns:
            column_lower = column.lower().replace(' ', '_').replace('-', '_')
            varchar_length = 1000 if column_lengths.get(column, 255) > 255 else 255
            cursor.execute(f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS "{column_lower}" VARCHAR({varchar_length});
            """)
            log_message(log_file, "SchemaUpdate", f"Added column {column_lower} as VARCHAR({varchar_length}) to {table_name}",
                        run_uuid=run_uuid, stepcounter=f"SchemaUpdate_{column_lower}", user=user, script_start_time=script_start_time)
        return True
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to add columns to {table_name}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="SchemaUpdate_Error", user=user, script_start_time=script_start_time)
        return False

def load_data_to_postgres(df, target_table, dataset_id, metadata_label, event_date, log_file, run_uuid, user, script_start_time):
    """Load DataFrame to PostgreSQL table with datasetid, metadata, and date."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # Get table columns
                table_columns = get_table_columns(cur, target_table, log_file, run_uuid, user, script_start_time)
                table_columns_lower = [col.lower() for col in table_columns]
                log_message(log_file, "DataLoadPrep", f"Table columns for {target_table}: {', '.join(table_columns)}",
                            run_uuid=run_uuid, stepcounter="DataLoadPrep_0", user=user, script_start_time=script_start_time)
                
                # Convert DataFrame column names to lowercase
                column_map = {col: col.lower().replace(' ', '_').replace('-', '_') for col in df.columns}
                df = df.rename(columns=column_map)
                df_columns = list(df.columns)
                log_message(log_file, "DataLoadPrep", f"Source columns after lowercase: {', '.join(df_columns)}",
                            run_uuid=run_uuid, stepcounter="DataLoadPrep_1", user=user, script_start_time=script_start_time)
                
                # Add datasetid, metadata, and date columns
                df["datasetid"] = dataset_id
                if metadata_label and "metadata_label" in table_columns_lower:
                    df["metadata_label"] = metadata_label
                if event_date and "event_date" in table_columns_lower:
                    df["event_date"] = event_date
                
                # Filter DataFrame to match table columns (case-insensitive)
                matching_columns = []
                column_mapping = {}
                for col in df_columns + ["datasetid", "metadata_label", "event_date"]:
                    col_lower = col.lower()
                    for table_col in table_columns:
                        if col_lower == table_col.lower():
                            matching_columns.append(col)
                            column_mapping[col] = table_col
                            break
                if not matching_columns:
                    log_message(log_file, "Error", f"No matching columns between source ({', '.join(df.columns)}) and table ({', '.join(table_columns)})",
                                run_uuid=run_uuid, stepcounter="DataLoad_2", user=user, script_start_time=script_start_time)
                    return False
                
                log_message(log_file, "DataLoadPrep", f"Matching columns after mapping: {', '.join(matching_columns)}",
                            run_uuid=run_uuid, stepcounter="DataLoadPrep_2", user=user, script_start_time=script_start_time)
                
                df = df[matching_columns].rename(columns=column_mapping)
                
                # Check DataFrame state
                if df.empty:
                    log_message(log_file, "Error", f"DataFrame is empty after filtering for {target_table}",
                                run_uuid=run_uuid, stepcounter="DataLoad_3", user=user, script_start_time=script_start_time)
                    return False
                
                log_message(log_file, "DataLoadPrep", f"DataFrame rows: {len(df)}, columns: {', '.join(df.columns)}",
                            run_uuid=run_uuid, stepcounter="DataLoadPrep_3", user=user, script_start_time=script_start_time)
                
                # Convert DataFrame to list of tuples
                records = [tuple(row) for row in df.to_numpy()]
                if not records:
                    log_message(log_file, "Error", f"No records to insert into {target_table}",
                                run_uuid=run_uuid, stepcounter="DataLoad_4", user=user, script_start_time=script_start_time)
                    return False
                
                # Prepare insert query
                placeholders = ",".join(["%s"] * len(df.columns))
                insert_query = f"""
                    INSERT INTO {target_table} ({','.join(f'"{col}"' for col in df.columns)})
                    VALUES ({placeholders})
                """
                
                # Execute insert
                log_message(log_file, "DataLoadPrep", f"Executing INSERT query for {len(records)} records into {target_table}",
                            run_uuid=run_uuid, stepcounter="DataLoadPrep_4", user=user, script_start_time=script_start_time)
                cur.executemany(insert_query, records)
                conn.commit()
                log_message(log_file, "DataLoad", f"Loaded {len(records)} rows to {target_table} with columns: {', '.join(df.columns)}",
                            run_uuid=run_uuid, stepcounter="DataLoad_0", user=user, script_start_time=script_start_time)
                return True
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Database error loading to {target_table}: {str(e)}\n{traceback.format_exc()}",
                    run_uuid=run_uuid, stepcounter="DataLoad_1", user=user, script_start_time=script_start_time)
        return False
    except Exception as e:
        log_message(log_file, "Error", f"Unexpected error loading to {target_table}: {str(e)}\n{traceback.format_exc()}",
                    run_uuid=run_uuid, stepcounter="DataLoad_5", user=user, script_start_time=script_start_time)
        return False

def generic_import(config_id):
    """Generic import script to process files based on timportconfig, creating one dataset only with valid data."""
    script_start_time = time.time()
    run_uuid = str(uuid.uuid4())
    user = get_username()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = LOG_DIR / f"generic_import_{timestamp}"

    try:
        ensure_directory_exists(LOG_DIR)
        log_message(log_file, "Initialization", f"Script started at {timestamp} for config_id {config_id}",
                    run_uuid=run_uuid, stepcounter="Initialization_0", user=user, script_start_time=script_start_time)
    except Exception as e:
        print(f"Error initializing log directory: {str(e)}")
        log_to_tlogentry(config_id, f"Failed: Error initializing log directory: {str(e)}",
                         "Initialization_0", log_file, run_uuid, user, script_start_time)
        sys.exit(1)

    config = get_config(config_id, log_file, run_uuid, user, script_start_time)
    if not config:
        log_message(log_file, "Error", "Failed to retrieve configuration. Exiting.",
                    run_uuid=run_uuid, stepcounter="Initialization_1", user=user, script_start_time=script_start_time)
        log_to_tlogentry(config_id, "Failed: No active configuration found for config_id",
                         "Initialization_1", log_file, run_uuid, user, script_start_time)
        sys.exit(1)

    log_message(log_file, "Initialization", f"Configuration loaded: {config['config_name']}",
                run_uuid=run_uuid, stepcounter="Initialization_2", user=user, script_start_time=script_start_time)

    # Validate configuration before proceeding
    required_fields = ['source_directory', 'archive_directory', 'file_pattern', 'file_type', 'target_table']
    missing_fields = [field for field in required_fields if not config.get(field)]
    if missing_fields:
        error_msg = f"Failed: Missing required configuration fields: {', '.join(missing_fields)}"
        log_message(log_file, "Error", error_msg,
                    run_uuid=run_uuid, stepcounter="Initialization_3", user=user, script_start_time=script_start_time)
        log_to_tlogentry(config_id, error_msg,
                         "Initialization_3", log_file, run_uuid, user, script_start_time)
        sys.exit(1)

    # Parse dataset date from the first file before creating the dataset
    dataset_date = datetime.now().date()  # Default, will be overridden if parsed
    files = []
    try:
        regex_pattern = config["file_pattern"].replace('\\\\', '\\')
        pattern = re.compile(regex_pattern)
        all_files = os.listdir(config["source_directory"])
        log_message(log_file, "FileSearch", f"Files in {config['source_directory']}: {', '.join(all_files)}",
                    run_uuid=run_uuid, stepcounter="FileSearch_0", user=user, script_start_time=script_start_time)
        
        for filename in all_files:
            if pattern.match(filename):
                full_path = os.path.join(config["source_directory"], filename)
                if os.path.isfile(full_path):
                    files.append(full_path)
                    log_message(log_file, "FileSearch", f"Matched file: {filename}",
                                run_uuid=run_uuid, stepcounter=f"FileSearch_Match_{filename}", user=user, script_start_time=script_start_time)
        
        if not files:
            error_msg = f"Failed: No files found matching pattern {regex_pattern} in {config['source_directory']}"
            log_message(log_file, "Error", error_msg,
                        run_uuid=run_uuid, stepcounter="FileSearch_1", user=user, script_start_time=script_start_time)
            log_to_tlogentry(config_id, error_msg,
                             "FileSearch_1", log_file, run_uuid, user, script_start_time)
            sys.exit(1)

        # Parse date from the first file
        first_filename = os.path.basename(files[0])
        log_message(log_file, "Debug", f"Attempting to parse date from filename '{first_filename}' with DateLocation {config['DateLocation']} and DateFormat {config['DateFormat']}",
                    run_uuid=run_uuid, stepcounter="FileSearch_2", user=user, script_start_time=script_start_time)
        parsed_date = parse_metadata(first_filename, config, config["DateConfig"], config["DateLocation"],
                                    config["delimiter"], log_file, run_uuid, user, script_start_time,
                                    date_format=config["DateFormat"])
        if parsed_date:
            dataset_date = parsed_date
            log_message(log_file, "Info", f"Parsed dataset_date {dataset_date} from filename '{first_filename}'",
                        run_uuid=run_uuid, stepcounter="FileSearch_3", user=user, script_start_time=script_start_time)
        else:
            error_msg = f"Failed: Failed to parse date from filename '{first_filename}'"
            log_message(log_file, "Error", error_msg,
                        run_uuid=run_uuid, stepcounter="FileSearch_4", user=user, script_start_time=script_start_time)
            log_to_tlogentry(config_id, error_msg,
                             "FileSearch_4", log_file, run_uuid, user, script_start_time)
            sys.exit(1)
    except re.error as e:
        error_msg = f"Failed: Invalid regex pattern {config['file_pattern']}: {str(e)}"
        log_message(log_file, "Error", error_msg,
                    run_uuid=run_uuid, stepcounter="FileSearch_5", user=user, script_start_time=script_start_time)
        log_to_tlogentry(config_id, error_msg,
                         "FileSearch_5", log_file, run_uuid, user, script_start_time)
        sys.exit(1)
    except Exception as e:
        error_msg = f"Failed: Unexpected error in file search: {str(e)}\n{traceback.format_exc()}"
        log_message(log_file, "Error", error_msg,
                    run_uuid=run_uuid, stepcounter="FileSearch_6", user=user, script_start_time=script_start_time)
        log_to_tlogentry(config_id, error_msg,
                         "FileSearch_6", log_file, run_uuid, user, script_start_time)
        sys.exit(1)

    # Create a single dataset with 'New' status using the parsed date
    dataset_id = None
    label = parse_metadata("", config, config["metadata_label_source"], config["metadata_label_location"],
                          config["delimiter"], log_file, run_uuid, user, script_start_time)
    if not label:
        label = config["config_name"]
    
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                log_message(log_file, "Debug", f"Creating dataset with date {dataset_date}",
                            run_uuid=run_uuid, stepcounter="Initialization_1", user=user, script_start_time=script_start_time)
                cur.execute("SELECT dba.f_dataset_iu(%s, %s, %s, %s, %s, %s, %s)",
                            (None, dataset_date, config["DataSetType"], config["DataSource"], label, 'New', user))
                dataset_id = cur.fetchone()[0]
                conn.commit()
                # Verify the datasetdate after creation
                cur.execute("SELECT datasetdate FROM dba.tdataset WHERE datasetid = %s", (dataset_id,))
                created_date = cur.fetchone()[0]
                log_message(log_file, "DatasetInsert", f"Created dataset with ID {dataset_id} and status 'New' with label '{label}' and date {created_date}",
                            run_uuid=run_uuid, stepcounter="Initialization_2", user=user, script_start_time=script_start_time)
    except psycopg2.Error as e:
        error_msg = f"Failed: Failed to create dataset: {str(e)}\n{traceback.format_exc()}"
        log_message(log_file, "Error", error_msg,
                    run_uuid=run_uuid, stepcounter="Initialization_3", user=user, script_start_time=script_start_time)
        log_to_tlogentry(config_id, error_msg,
                         "Initialization_3", log_file, run_uuid, user, script_start_time)
        sys.exit(1)

    log_message(log_file, "Processing", f"Found {len(files)} files to process: {', '.join(os.path.basename(f) for f in files)}",
                run_uuid=run_uuid, stepcounter="FileSearch_7", user=user, script_start_time=script_start_time)

    ensure_directory_exists(config["source_directory"])
    ensure_directory_exists(config["archive_directory"])

    success = True
    for file_path in files:
        filename = os.path.basename(file_path)
        file_success = True
        log_message(log_file, "Processing", f"Processing file: {filename}",
                    run_uuid=run_uuid, stepcounter=f"File_{filename}_0", user=user, script_start_time=script_start_time)

        csv_path = file_path
        if config["file_type"] in ["XLS", "XLSX"]:
            csv_path = os.path.join(FILE_WATCHER_DIR, f"{timestamp}_{filename}.csv")
            try:
                xls_to_csv(file_path)
                if not os.path.exists(csv_path):
                    log_message(log_file, "Error", f"Failed to convert {filename} to CSV",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_4", user=user, script_start_time=script_start_time)
                    success = False
                    file_success = False
                else:
                    log_message(log_file, "Conversion", f"Converted {filename} to {csv_path}",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_5", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Conversion error for {filename}: {str(e)}\n{traceback.format_exc()}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_6", user=user, script_start_time=script_start_time)
                success = False
                file_success = False

        if file_success:
            try:
                df = pd.read_csv(csv_path)
                log_message(log_file, "Processing", f"Read {len(df)} rows from {csv_path} with columns: {', '.join(df.columns)}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_7", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Failed to read CSV {csv_path}: {str(e)}\n{traceback.format_exc()}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_8", user=user, script_start_time=script_start_time)
                success = False
                file_success = False
                if csv_path != file_path and os.path.exists(csv_path):
                    try:
                        os.remove(csv_path)
                        log_message(log_file, "Processing", f"Removed temporary CSV {csv_path}",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_20", user=user, script_start_time=script_start_time)
                    except Exception as e:
                        log_message(log_file, "Warning", f"Failed to remove temporary CSV {csv_path}: {str(e)}\n{traceback.format_exc()}",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_21", user=user, script_start_time=script_start_time)

        if file_success:
            try:
                column_lengths = get_column_lengths(df)
                log_message(log_file, "Processing", f"Computed column lengths: {column_lengths}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_9", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Failed to compute column lengths for {filename}: {str(e)}\n{traceback.format_exc()}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_10", user=user, script_start_time=script_start_time)
                success = False
                file_success = False

        if file_success:
            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cur:
                    try:
                        table_name = config["target_table"].split('.')[-1]
                        if not table_exists(cur, config["target_table"], log_file, run_uuid, user, script_start_time):
                            if config["importstrategyid"] == 1:
                                columns = []
                                columns.append(f'"{table_name}id" SERIAL PRIMARY KEY')
                                columns.append('"datasetid" INT NOT NULL REFERENCES dba.tdataset(datasetid)')
                                for col in df.columns:
                                    col_lower = col.lower().replace(' ', '_').replace('-', '_')
                                    varchar_length = 1000 if column_lengths.get(col, 255) > 255 else 255
                                    columns.append(f'"{col_lower}" VARCHAR({varchar_length})')
                                if config["metadata_label_source"] != "none":
                                    columns.append('"metadata_label" VARCHAR(255)')
                                if config["DateConfig"] != "none":
                                    columns.append('"event_date" VARCHAR(50)')
                                create_query = f"""
                                    CREATE TABLE {config["target_table"]} (
                                        {', '.join(columns)}
                                    );
                                """
                                try:
                                    cur.execute(create_query)
                                    conn.commit()
                                    log_message(log_file, "SchemaUpdate", f"Created table {config['target_table']} with columns: {table_name}id, datasetid, {', '.join(col.lower() for col in df.columns)}",
                                                run_uuid=run_uuid, stepcounter="SchemaCreate_0", user=user, script_start_time=script_start_time)
                                except psycopg2.Error as e:
                                    log_message(log_file, "Error", f"Failed to create table {config['target_table']}: {str(e)}\n{traceback.format_exc()}",
                                                run_uuid=run_uuid, stepcounter="SchemaCreate_1", user=user, script_start_time=script_start_time)
                                    success = False
                                    file_success = False
                            else:
                                log_message(log_file, "Error", f"Table {config['target_table']} does not exist and importstrategyid {config['importstrategyid']} does not allow creation",
                                            run_uuid=run_uuid, stepcounter="SchemaCheck_0", user=user, script_start_time=script_start_time)
                                success = False
                                file_success = False

                        if file_success:
                            table_columns = get_table_columns(cur, config["target_table"], log_file, run_uuid, user, script_start_time)
                            source_columns = list(df.columns)
                            log_message(log_file, "SchemaCheck", f"Table columns: {', '.join(table_columns)}",
                                        run_uuid=run_uuid, stepcounter="SchemaCheck_1", user=user, script_start_time=script_start_time)
                            log_message(log_file, "SchemaCheck", f"Source columns: {', '.join(source_columns)}",
                                        run_uuid=run_uuid, stepcounter="SchemaCheck_2", user=user, script_start_time=script_start_time)

                            new_columns = [col for col in source_columns if col.lower() not in [tc.lower() for tc in table_columns]]
                            missing_columns = [col for col in table_columns if col.lower() not in [sc.lower() for sc in source_columns] and col.lower() not in [f"{table_name}id", 'datasetid', 'metadata_label', 'event_date']]

                            if config["importstrategyid"] == 1:
                                if new_columns:
                                    if not add_columns_to_table(cur, config["target_table"], new_columns, column_lengths, log_file, run_uuid, user, script_start_time):
                                        success = False
                                        file_success = False
                                    else:
                                        conn.commit()
                            elif config["importstrategyid"] == 2:
                                pass
                            elif config["importstrategyid"] == 3:
                                if missing_columns:
                                    log_message(log_file, "Error", f"Missing required columns in source file: {', '.join(missing_columns)}",
                                                run_uuid=run_uuid, stepcounter="SchemaCheck_3", user=user, script_start_time=script_start_time)
                                    success = False
                                    file_success = False

                            if file_success:
                                metadata_label = parse_metadata(filename, config, config["metadata_label_source"],
                                                               config["metadata_label_location"], config["delimiter"],
                                                               log_file, run_uuid, user, script_start_time)
                                event_date = parse_metadata(filename, config, config["DateConfig"],
                                                            config["DateLocation"], config["delimiter"],
                                                            log_file, run_uuid, user, script_start_time,
                                                            date_format=config["DateFormat"])

                                log_message(log_file, "Processing", f"Calling load_data_to_postgres for {filename} with dataset_id {dataset_id}",
                                            run_uuid=run_uuid, stepcounter=f"File_{filename}_11", user=user, script_start_time=script_start_time)
                                if not load_data_to_postgres(df, config["target_table"], dataset_id, metadata_label, event_date,
                                                            log_file, run_uuid, user, script_start_time):
                                    log_message(log_file, "Error", f"Failed to load data from {filename} to {config['target_table']}",
                                                run_uuid=run_uuid, stepcounter=f"File_{filename}_14", user=user, script_start_time=script_start_time)
                                    success = False
                                    file_success = False
                                else:
                                    archive_path = os.path.join(config["archive_directory"], filename)
                                    try:
                                        shutil.move(file_path, archive_path)
                                        os.chmod(archive_path, 0o660)
                                        # Removed os.chown due to os.getgrnam error
                                        log_message(log_file, "Processing", f"Moved {filename} to {archive_path}",
                                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_12", user=user, script_start_time=script_start_time)
                                    except Exception as e:
                                        log_message(log_file, "Warning", f"Failed to move {filename} to archive: {str(e)}\n{traceback.format_exc()}",
                                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_13", user=user, script_start_time=script_start_time)
                                        # Archiving is non-critical

                    except Exception as e:
                        log_message(log_file, "Error", f"Unexpected error processing {filename}: {str(e)}\n{traceback.format_exc()}",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_17", user=user, script_start_time=script_start_time)
                        success = False
                        file_success = False

                    if csv_path != file_path and os.path.exists(csv_path):
                        try:
                            os.remove(csv_path)
                            log_message(log_file, "Processing", f"Removed temporary CSV {csv_path}",
                                        run_uuid=run_uuid, stepcounter=f"File_{filename}_20", user=user, script_start_time=script_start_time)
                        except Exception as e:
                            log_message(log_file, "Warning", f"Failed to remove temporary CSV {csv_path}: {str(e)}\n{traceback.format_exc()}",
                                        run_uuid=run_uuid, stepcounter=f"File_{filename}_21", user=user, script_start_time=script_start_time)
                            # Removing temporary CSV is non-critical

        # Update dataset status to 'Failed' if file processing failed
        if not file_success:
            try:
                with psycopg2.connect(**DB_PARAMS) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT dba.f_dataset_iu(%s, %s, %s, %s, %s, %s, %s)",
                                    (dataset_id, dataset_date, config["DataSetType"], config["DataSource"], label, 'Failed', user))
                        conn.commit()
                        log_message(log_file, "DatasetUpdate", f"Updated dataset {dataset_id} to status 'Failed' due to processing error",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_18", user=user, script_start_time=script_start_time)
            except psycopg2.Error as e:
                log_message(log_file, "Error", f"Failed to update dataset {dataset_id} to 'Failed': {str(e)}\n{traceback.format_exc()}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_19", user=user, script_start_time=script_start_time)

    # Update dataset status based on overall success
    final_status = 'Active' if success else 'Failed'
    log_message(log_file, "Debug", f"Setting final status for dataset {dataset_id} to '{final_status}' with success={success} and dataset_date {dataset_date}",
                run_uuid=run_uuid, stepcounter="Finalization_1", user=user, script_start_time=script_start_time)
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # Log status and date before update
                cur.execute("SELECT statusname, datasetdate FROM dba.tdataset JOIN dba.tdatastatus ON tdataset.datastatusid = tdatastatus.datastatusid WHERE datasetid = %s",
                            (dataset_id,))
                before_status, before_date = cur.fetchone()
                log_message(log_file, "Debug", f"Status before update: {before_status}, Date before update: {before_date}",
                            run_uuid=run_uuid, stepcounter="Finalization_2", user=user, script_start_time=script_start_time)

                # Update status, passing dataset_date to maintain consistency
                cur.execute("SELECT dba.f_dataset_iu(%s, %s, %s, %s, %s, %s, %s)",
                            (dataset_id, dataset_date, config["DataSetType"], config["DataSource"], label, final_status, user))
                conn.commit()

                # Log status and date after update
                cur.execute("SELECT statusname, datasetdate FROM dba.tdataset JOIN dba.tdatastatus ON tdataset.datastatusid = tdatastatus.datastatusid WHERE datasetid = %s",
                            (dataset_id,))
                after_status, after_date = cur.fetchone()
                log_message(log_file, "DatasetUpdate", f"Updated dataset {dataset_id} to status '{after_status}' with date {after_date}",
                            run_uuid=run_uuid, stepcounter="Finalization_3", user=user, script_start_time=script_start_time)
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to update dataset {dataset_id} to '{final_status}': {str(e)}\n{traceback.format_exc()}",
                    run_uuid=run_uuid, stepcounter="Finalization_4", user=user, script_start_time=script_start_time)

    log_message(log_file, "Finalization", f"Completed processing for config_id {config_id} with status '{final_status}'",
                run_uuid=run_uuid, stepcounter="Finalization_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generic_import.py <config_id>")
        log_to_tlogentry(0, "Failed: Invalid arguments: Usage: python generic_import.py <config_id>",
                         "Main_0", "generic_import.log", str(uuid.uuid4()), "unknown", time.time())
        sys.exit(1)
    config_id = int(sys.argv[1])
    generic_import(config_id)