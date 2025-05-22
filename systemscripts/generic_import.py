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
import grp  # Added for getgrnam
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
    "dbname": "feeds",
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
                    INSERT INTO dba.tlogentry (timestamp, run_uuid, processtype, stepcounter, username, message)
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
                    SELECT config_name, datasource, datasettype, source_directory, archive_directory,
                           file_pattern, file_type, metadata_label_source, metadata_label_location,
                           dateconfig, datelocation, dateformat, delimiter, target_table, importstrategyid, is_active
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
                    "datasource": config[1],
                    "datasettype": config[2],
                    "source_directory": config[3],
                    "archive_directory": config[4],
                    "file_pattern": config[5],
                    "file_type": config[6],
                    "metadata_label_source": config[7],
                    "metadata_label_location": config[8],
                    "dateconfig": config[9],
                    "datelocation": config[10],
                    "dateformat": config[11],
                    "delimiter": config[12],
                    "target_table": config[13],
                    "importstrategyid": config[14]
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
        # Handle numeric index
        if field_location.isdigit():
            try:
                index = int(field_location)
                # Remove extension and split by delimiter
                base_name = os.path.splitext(filename)[0]
                parts = base_name.split(delimiter)
                if index < len(parts):
                    return parts[index]
                log_message(log_file, "Error", f"Invalid index {index} for filename '{filename}' with {len(parts)} parts",
                            run_uuid=run_uuid, stepcounter="MetadataParse_1", user=user, script_start_time=script_start_time)
                return None
            except ValueError as e:
                log_message(log_file, "Error", f"Index error for {field_location}: {str(e)}",
                            run_uuid=run_uuid, stepcounter="MetadataParse_2", user=user, script_start_time=script_start_time)
                return None
        # Handle regex pattern
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

def get_table_column_lengths(cursor, table_name, log_file, run_uuid, user, script_start_time):
    """Get the maximum length of each VARCHAR column in the table."""
    try:
        cursor.execute("""
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema || '.' || table_name = %s
              AND data_type = 'character varying';
        """, (table_name,))
        column_lengths = {row[0]: row[1] for row in cursor.fetchall()}
        log_message(log_file, "TableColumnLengths", f"Column lengths for {table_name}: {column_lengths}",
                    run_uuid=run_uuid, stepcounter="TableColumnLengths_0", user=user, script_start_time=script_start_time)
        return column_lengths
    except Exception as e:
        log_message(log_file, "Error", f"Failed to get column lengths for {table_name}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="TableColumnLengths_1", user=user, script_start_time=script_start_time)
        return {}

def get_column_lengths(df):
    """Determine the maximum length of data in each column with a safety margin."""
    lengths = {}
    for col in df.columns:
        # Replace NaN/None with empty string to avoid underestimation
        series = df[col].fillna('').astype(str)
        max_length = series.str.len().max()
        # Apply a 1.5x safety margin, cap at 4000 to avoid excessive lengths
        safe_length = min(int(max_length * 1.5) if not pd.isna(max_length) else 255, 4000)
        lengths[col] = safe_length
    return lengths

def ensure_lookup_ids(cursor, datasource, dataset_type, user, log_file, run_uuid, script_start_time):
    """Ensure datasource and datasettype exist in tdatasource and tdatasettype, inserting if necessary."""
    try:
        # Ensure datasource exists
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
        log_message(log_file, "LookupInsert", f"Ensured datasource {datasource} with datasourceid {datasource_id}",
                    run_uuid=run_uuid, stepcounter="LookupInsert_0", user=user, script_start_time=script_start_time)

        # Ensure datasettype exists
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
        log_message(log_file, "LookupInsert", f"Ensured datasettype {dataset_type} with datasettypeid {dataset_type_id}",
                    run_uuid=run_uuid, stepcounter="LookupInsert_1", user=user, script_start_time=script_start_time)

        return datasource_id, dataset_type_id
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to ensure lookup IDs: {str(e)}",
                    run_uuid=run_uuid, stepcounter="LookupInsert_2", user=user, script_start_time=script_start_time)
        return None, None

def insert_dataset(cursor, config_name, dataset_date, label, datasource_id, dataset_type_id, log_file, run_uuid, user, script_start_time):
    """Insert a new dataset into tdataset and return its ID."""
    try:
        cursor.execute("""
            INSERT INTO dba.tdataset (datasetdate, label, datasettypeid, datasourceid, datastatusid, isactive, createddate, createdby, effthrudate)
            VALUES (%s, %s, %s, %s, 1, TRUE, CURRENT_TIMESTAMP, %s, '9999-01-01')
            RETURNING datasetid;
        """, (dataset_date, label, dataset_type_id, datasource_id, user))
        dataset_id = cursor.fetchone()[0]
        log_message(log_file, "DatasetInsert", f"Inserted dataset {config_name} with datasetid {dataset_id}",
                    run_uuid=run_uuid, stepcounter="DatasetInsert_0", user=user, script_start_time=script_start_time)
        return dataset_id
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to insert dataset {config_name}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="DatasetInsert_1", user=user, script_start_time=script_start_time)
        return None

def update_dataset_status(cursor, dataset_id, datasource_id, dataset_type_id, label, dataset_date, log_file, run_uuid, user, script_start_time):
    """Deactivate other datasets with same datasourceid, datasettypeid, label, and datasetdate."""
    try:
        cursor.execute("""
            UPDATE dba.tdataset
            SET isactive = FALSE, datastatusid = 2, effthrudate = CURRENT_TIMESTAMP
            WHERE datasourceid = %s
              AND datasettypeid = %s
              AND label = %s
              AND datasetdate = %s
              AND datasetid != %s
              AND isactive = TRUE;
        """, (datasource_id, dataset_type_id, label, dataset_date, dataset_id))
        log_message(log_file, "DatasetUpdate", f"Deactivated other datasets for datasetid={dataset_id}, datasourceid={datasource_id}, datasettypeid={dataset_type_id}, label={label}, datasetdate={dataset_date}",
                    run_uuid=run_uuid, stepcounter="DatasetUpdate_0", user=user, script_start_time=script_start_time)
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to deactivate other datasets for datasetid {dataset_id}: {str(e)}\n{traceback.format_exc()}",
                    run_uuid=run_uuid, stepcounter="DatasetUpdate_1", user=user, script_start_time=script_start_time)

def add_columns_to_table(cursor, table_name, new_columns, column_lengths, log_file, run_uuid, user, script_start_time):
    """Add new columns or update existing ones to the target table with appropriate VARCHAR length."""
    try:
        # Get current column lengths
        existing_lengths = get_table_column_lengths(cursor, table_name, log_file, run_uuid, user, script_start_time)
        
        for column in new_columns:
            column_lower = column.lower().replace(' ', '_').replace('-', '_')
            required_length = min(column_lengths.get(column, 1000), 4000)  # Default to 1000, cap at 4000
            existing_length = existing_lengths.get(column_lower)
            
            if existing_length is None:
                # Add new column
                cursor.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN IF NOT EXISTS "{column_lower}" VARCHAR({required_length});
                """)
                log_message(log_file, "SchemaUpdate", f"Added column {column_lower} as VARCHAR({required_length}) to {table_name}",
                            run_uuid=run_uuid, stepcounter=f"SchemaUpdate_{column_lower}", user=user, script_start_time=script_start_time)
            elif existing_length < required_length:
                # Update existing column length
                cursor.execute(f"""
                    ALTER TABLE {table_name}
                    ALTER COLUMN "{column_lower}" TYPE VARCHAR({required_length});
                """)
                log_message(log_file, "SchemaUpdate", f"Updated column {column_lower} to VARCHAR({required_length}) in {table_name}",
                            run_uuid=run_uuid, stepcounter=f"SchemaUpdate_{column_lower}", user=user, script_start_time=script_start_time)
        return True
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to add or update columns in {table_name}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="SchemaUpdate_Error", user=user, script_start_time=script_start_time)
        return False

def load_data_to_postgres(df, target_table, dataset_id, metadata_label, event_date, log_file, run_uuid, user, script_start_time):
    """Load DataFrame to PostgreSQL table with datasetid, metadata, and date, truncating long values."""
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # Get table columns and their lengths
                table_columns = get_table_columns(cur, target_table, log_file, run_uuid, user, script_start_time)
                table_columns_lower = [col.lower() for col in table_columns]
                table_column_lengths = get_table_column_lengths(cur, target_table, log_file, run_uuid, user, script_start_time)
                log_message(log_file, "DataLoadPrep", f"Table columns for {target_table}: {', '.join(table_columns)}",
                            run_uuid=run_uuid, stepcounter="DataLoadPrep_0", user=user, script_start_time=script_start_time)
                
                # Convert DataFrame column names to lowercase
                column_map = {col: col.lower().replace(' ', '_').replace('-', '_') for col in df.columns}
                df = df.rename(columns=column_map)
                df_columns = list(df.columns)
                log_message(log_file, "DataLoadPrep", f"Source columns after lowercase: {', '.join(df_columns)}",
                            run_uuid=run_uuid, stepcounter="DataLoadPrep_1", user=user, script_start_time=script_start_time)
                
                # Add datasetid (mandatory), and metadata/date only if columns exist in table
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
                
                # Truncate values to fit column lengths
                for col in df.columns:
                    col_lower = col.lower()
                    max_length = table_column_lengths.get(col_lower, 255)  # Default to 255 if unknown
                    if max_length:
                        # Convert to string and truncate
                        df[col] = df[col].astype(str).str.slice(0, max_length)
                        # Log any truncated values
                        long_values = df[df[col].str.len() >= max_length][col]
                        if not long_values.empty:
                            log_message(log_file, "Warning", f"Truncated {len(long_values)} values in column {col} to {max_length} characters",
                                        run_uuid=run_uuid, stepcounter=f"DataLoad_Truncate_{col}", user=user, script_start_time=script_start_time)
                
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
    """Generic import script to process files based on timportconfig."""
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
        sys.exit(1)

    config = get_config(config_id, log_file, run_uuid, user, script_start_time)
    if not config:
        log_message(log_file, "Error", "Failed to retrieve configuration. Exiting.",
                    run_uuid=run_uuid, stepcounter="Initialization_1", user=user, script_start_time=script_start_time)
        return

    log_message(log_file, "Initialization", f"Configuration loaded: {config['config_name']}",
                run_uuid=run_uuid, stepcounter="Initialization_2", user=user, script_start_time=script_start_time)

    ensure_directory_exists(config["source_directory"])
    ensure_directory_exists(config["archive_directory"])

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
            else:
                log_message(log_file, "FileSearch", f"File {filename} does not match pattern {regex_pattern}",
                            run_uuid=run_uuid, stepcounter=f"FileSearch_NoMatch_{filename}", user=user, script_start_time=script_start_time)
        
        if not files:
            log_message(log_file, "Warning", f"No files found matching pattern {regex_pattern} in {config['source_directory']}",
                        run_uuid=run_uuid, stepcounter="FileSearch_1", user=user, script_start_time=script_start_time)
            return
        
        log_message(log_file, "Processing", f"Found {len(files)} files to process: {', '.join(os.path.basename(f) for f in files)}",
                    run_uuid=run_uuid, stepcounter="FileSearch_2", user=user, script_start_time=script_start_time)
    except re.error as e:
        log_message(log_file, "Error", f"Invalid regex pattern {config['file_pattern']}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="FileSearch_3", user=user, script_start_time=script_start_time)
        return
    except Exception as e:
        log_message(log_file, "Error", f"Unexpected error in file search: {str(e)}\n{traceback.format_exc()}",
                    run_uuid=run_uuid, stepcounter="FileSearch_4", user=user, script_start_time=script_start_time)
        return

    success = True
    dataset_ids = []
    for file_path in files:
        filename = os.path.basename(file_path)
        file_success = True
        log_message(log_file, "Processing", f"Processing file: {filename}",
                    run_uuid=run_uuid, stepcounter=f"File_{filename}_0", user=user, script_start_time=script_start_time)

        # Parse date from filename
        date_string = parse_metadata(filename, config, config["dateconfig"], config["datelocation"], config["delimiter"], log_file, run_uuid, user, script_start_time)
        if date_string:
            try:
                dataset_date = datetime.strptime(date_string, '%Y%m%dT%H%M%S').date()
                log_message(log_file, "Processing", f"Parsed dataset_date {dataset_date} from filename '{filename}'",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_Date", user=user, script_start_time=script_start_time)
            except ValueError as e:
                log_message(log_file, "Error", f"Failed to parse date '{date_string}' with format {config['dateformat']}: {str(e)}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_Date", user=user, script_start_time=script_start_time)
                dataset_date = datetime.now().date()
        else:
            dataset_date = datetime.now().date()

        # Parse label
        label = parse_metadata(filename, config, config["metadata_label_source"], config["metadata_label_location"], config["delimiter"], log_file, run_uuid, user, script_start_time)
        if not label:
            label = config["config_name"]

        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                try:
                    datasource_id, dataset_type_id = ensure_lookup_ids(cur, config["datasource"], config["datasettype"], user, log_file, run_uuid, script_start_time)
                    if not datasource_id or not dataset_type_id:
                        log_message(log_file, "Error", f"Failed to ensure lookup IDs for file {filename}. Skipping.",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_1", user=user, script_start_time=script_start_time)
                        success = False
                        file_success = False
                        continue

                    dataset_id = insert_dataset(cur, config["config_name"], dataset_date, label, datasource_id, dataset_type_id, log_file, run_uuid, user, script_start_time)
                    if not dataset_id:
                        log_message(log_file, "Error", f"Failed to create dataset for file {filename}. Skipping.",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_2", user=user, script_start_time=script_start_time)
                        success = False
                        file_success = False
                        continue
                    dataset_ids.append((dataset_id, datasource_id, dataset_type_id, label, dataset_date))
                    conn.commit()

                    update_dataset_status(cur, dataset_id, datasource_id, dataset_type_id, label, dataset_date, log_file, run_uuid, user, script_start_time)
                    conn.commit()
                except Exception as e:
                    log_message(log_file, "Error", f"Unexpected error in dataset setup for {filename}: {str(e)}\n{traceback.format_exc()}",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_3", user=user, script_start_time=script_start_time)
                    success = False
                    file_success = False
                    continue

        csv_path = file_path
        if config["file_type"] in ["XLS", "XLSX"]:
            csv_path = os.path.splitext(file_path)[0] + '.csv'
            try:
                xls_to_csv(file_path)
                if not os.path.exists(csv_path):
                    log_message(log_file, "Error", f"Failed to find CSV at {csv_path} after conversion of {filename}. Check xls_to_csv log at {LOG_DIR}/xls_to_csv_{timestamp}.txt",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_4", user=user, script_start_time=script_start_time)
                    success = False
                    file_success = False
                    continue
                log_message(log_file, "Conversion", f"Converted {filename} to {csv_path}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_5", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Conversion error for {filename}: {str(e)}\n{traceback.format_exc()}. Check xls_to_csv log at {LOG_DIR}/xls_to_csv_{timestamp}.txt",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_6", user=user, script_start_time=script_start_time)
                success = False
                file_success = False
                continue

        try:
            df = pd.read_csv(csv_path)
            log_message(log_file, "Processing", f"Read {len(df)} rows from {csv_path} with columns: {', '.join(df.columns)}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_7", user=user, script_start_time=script_start_time)
            
            # Validate data for long values
            column_lengths = get_column_lengths(df)
            for col, length in column_lengths.items():
                if length > 1000:
                    log_message(log_file, "Warning", f"Column {col} has maximum length {length} exceeding 1000 characters. Values may be truncated.",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_Validate_{col}", user=user, script_start_time=script_start_time)
            
            log_message(log_file, "Processing", f"Computed column lengths: {column_lengths}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_9", user=user, script_start_time=script_start_time)
        except Exception as e:
            log_message(log_file, "Error", f"Failed to read CSV {csv_path}: {str(e)}\n{traceback.format_exc()}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_8", user=user, script_start_time=script_start_time)
            success = False
            file_success = False
            if csv_path != file_path and os.path.exists(csv_path):
                os.remove(csv_path)
            continue

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
                                varchar_length = min(column_lengths.get(col, 1000), 4000)  # Default to 1000, cap at 4000
                                columns.append(f'"{col_lower}" VARCHAR({varchar_length})')
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
                                continue
                        else:
                            log_message(log_file, "Error", f"Table {config['target_table']} does not exist and importstrategyid {config['importstrategyid']} does not allow creation",
                                        run_uuid=run_uuid, stepcounter="SchemaCheck_0", user=user, script_start_time=script_start_time)
                            success = False
                            file_success = False
                            continue

                    table_columns = get_table_columns(cur, config["target_table"], log_file, run_uuid, user, script_start_time)
                    source_columns = list(df.columns)
                    log_message(log_file, "SchemaCheck", f"Table columns: {', '.join(table_columns)}",
                                run_uuid=run_uuid, stepcounter="SchemaCheck_1", user=user, script_start_time=script_start_time)
                    log_message(log_file, "SchemaCheck", f"Source columns: {', '.join(source_columns)}",
                                run_uuid=run_uuid, stepcounter="SchemaCheck_2", user=user, script_start_time=script_start_time)

                    new_columns = [col for col in source_columns if col.lower() not in [tc.lower() for tc in table_columns]]
                    missing_columns = [col for col in table_columns if col.lower() not in [sc.lower() for sc in source_columns] and col.lower() not in [f"{table_name}id", 'datasetid']]

                    if config["importstrategyid"] == 1:
                        if new_columns:
                            if not add_columns_to_table(cur, config["target_table"], new_columns, column_lengths, log_file, run_uuid, user, script_start_time):
                                success = False
                                file_success = False
                                continue
                            conn.commit()
                    elif config["importstrategyid"] == 2:
                        pass
                    elif config["importstrategyid"] == 3:
                        if missing_columns:
                            log_message(log_file, "Error", f"Missing required columns in source file: {', '.join(missing_columns)}",
                                        run_uuid=run_uuid, stepcounter="SchemaCheck_3", user=user, script_start_time=script_start_time)
                            success = False
                            file_success = False
                            continue

                    metadata_label = parse_metadata(filename, config, config["metadata_label_source"],
                                                   config["metadata_label_location"], config["delimiter"],
                                                   log_file, run_uuid, user, script_start_time)
                    event_date = parse_metadata(filename, config, config["dateconfig"],
                                                config["datelocation"], config["delimiter"],
                                                log_file, run_uuid, user, script_start_time)

                    log_message(log_file, "Processing", f"Calling load_data_to_postgres for {filename} with dataset_id {dataset_id}",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_11", user=user, script_start_time=script_start_time)
                    if load_data_to_postgres(df, config["target_table"], dataset_id, metadata_label, event_date,
                                            log_file, run_uuid, user, script_start_time):
                        archive_path = os.path.join(config["archive_directory"], filename)
                        try:
                            shutil.move(file_path, archive_path)
                            os.chmod(archive_path, 0o660)
                            try:
                                group_id = grp.getgrnam('etl_group').gr_gid
                                os.chown(archive_path, os.getuid(), group_id)
                                log_message(log_file, "Processing", f"Moved {filename} to {archive_path}",
                                            run_uuid=run_uuid, stepcounter=f"File_{filename}_12", user=user, script_start_time=script_start_time)
                            except KeyError:
                                log_message(log_file, "Warning", f"Group 'etl_group' not found; skipping chown for {archive_path}",
                                            run_uuid=run_uuid, stepcounter=f"File_{filename}_13", user=user, script_start_time=script_start_time)
                        except Exception as e:
                            log_message(log_file, "Error", f"Failed to move {filename} to archive: {str(e)}\n{traceback.format_exc()}",
                                        run_uuid=run_uuid, stepcounter=f"File_{filename}_13", user=user, script_start_time=script_start_time)
                            success = False
                            file_success = False
                    else:
                        log_message(log_file, "Error", f"Failed to load data from {filename} to {config['target_table']}",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_14", user=user, script_start_time=script_start_time)
                        success = False
                        file_success = False
                except Exception as e:
                    log_message(log_file, "Error", f"Unexpected error processing {filename}: {str(e)}\n{traceback.format_exc()}",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_15", user=user, script_start_time=script_start_time)
                    success = False
                    file_success = False
                    continue

                if csv_path != file_path and os.path.exists(csv_path):
                    try:
                        os.remove(csv_path)
                        log_message(log_file, "Processing", f"Removed temporary CSV {csv_path}",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_16", user=user, script_start_time=script_start_time)
                    except Exception as e:
                        log_message(log_file, "Error", f"Failed to remove temporary CSV {csv_path}: {str(e)}\n{traceback.format_exc()}",
                                    run_uuid=run_uuid, stepcounter=f"File_{filename}_17", user=user, script_start_time=script_start_time)

    log_message(log_file, "Finalization", f"Completed processing for config_id {config_id} with overall success={success}",
                run_uuid=run_uuid, stepcounter="Finalization_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generic_import.py <config_id>")
        sys.exit(1)
    config_id = int(sys.argv[1])
    generic_import(config_id)