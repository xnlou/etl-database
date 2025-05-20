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

# Add root directory to sys.path
sys.path.append(str(Path(__file__).parent.parent))

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
                           DateConfig, DateLocation, delimiter, target_table, importstrategyid, is_active
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
                    "importstrategyid": config[13]
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

def table_exists(cursor, table_name):
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
        return cursor.fetchone()[0]
    except Exception as e:
        return False

def get_table_columns(cursor, table_name):
    """Get the columns of a table."""
    try:
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema || '.' || table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        return []

def get_column_lengths(df):
    """Determine the maximum length of data in each column."""
    lengths = {}
    for col in df.columns:
        max_length = df[col].astype(str).str.len().max()
        lengths[col] = max_length if not pd.isna(max_length) else 255
    return lengths

def insert_dataset(cursor, config_name, dataset_date, label, log_file, run_uuid, user, script_start_time):
    """Insert a new dataset into tDataSet and return its ID."""
    try:
        cursor.execute("""
            INSERT INTO dba.tDataSet (DataSetDate, Label, DataSetTypeID, DataSourceID, DataStatusID, IsActive, CreatedDate, CreatedBy)
            VALUES (%s, %s, 1, 1, 2, FALSE, CURRENT_TIMESTAMP, %s)
            RETURNING DataSetID;
        """, (dataset_date, label, user))
        dataset_id = cursor.fetchone()[0]
        log_message(log_file, "DatasetInsert", f"Inserted dataset {config_name} with DataSetID {dataset_id}",
                    run_uuid=run_uuid, stepcounter="DatasetInsert_0", user=user, script_start_time=script_start_time)
        return dataset_id
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to insert dataset {config_name}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="DatasetInsert_1", user=user, script_start_time=script_start_time)
        return None

def update_dataset_status(cursor, dataset_id, is_active, log_file, run_uuid, user, script_start_time):
    """Update the IsActive status and DataStatusID of a dataset."""
    try:
        status_id = 1 if is_active else 2  # 1 = Active, 2 = Inactive
        cursor.execute("""
            UPDATE dba.tDataSet
            SET IsActive = %s, DataStatusID = %s, EffThruDate = CASE WHEN %s THEN '9999-01-01' ELSE CURRENT_TIMESTAMP END
            WHERE DataSetID = %s;
        """, (is_active, status_id, is_active, dataset_id))
        log_message(log_file, "DatasetUpdate", f"Updated DataSetID {dataset_id} to IsActive = {is_active}, DataStatusID = {status_id}",
                    run_uuid=run_uuid, stepcounter="DatasetUpdate_0", user=user, script_start_time=script_start_time)
    except psycopg2.Error as e:
        log_message(log_file, "Error", f"Failed to update DataSetID {dataset_id}: {str(e)}",
                    run_uuid=run_uuid, stepcounter="DatasetUpdate_1", user=user, script_start_time=script_start_time)

def add_columns_to_table(cursor, table_name, new_columns, column_lengths, log_file, run_uuid, user, script_start_time):
    """Add new columns to the target table with appropriate VARCHAR length."""
    try:
        for column in new_columns:
            varchar_length = 1000 if column_lengths.get(column, 255) > 255 else 255
            cursor.execute(f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS "{column}" VARCHAR({varchar_length});
            """)
            log_message(log_file, "SchemaUpdate", f"Added column {column} as VARCHAR({varchar_length}) to {table_name}",
                        run_uuid=run_uuid, stepcounter=f"SchemaUpdate_{column}", user=user, script_start_time=script_start_time)
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
                table_columns = get_table_columns(cur, target_table)
                table_columns_lower = [col.lower() for col in table_columns]
                
                # Add primary key, datasetid, metadata, and date columns
                df_columns = list(df.columns)
                table_name = target_table.split('.')[-1]
                df[f"{table_name}id"] = [str(uuid.uuid4()) for _ in range(len(df))]  # Generate UUIDs
                df["datasetid"] = dataset_id
                if metadata_label and "metadata_label" in table_columns_lower:
                    df["metadata_label"] = metadata_label
                if event_date and "event_date" in table_columns_lower:
                    df["event_date"] = event_date
                
                # Filter DataFrame to match table columns (case-insensitive)
                matching_columns = []
                column_mapping = {}
                for col in df_columns + [f"{table_name}id", "datasetid", "metadata_label", "event_date"]:
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
                
                df = df[matching_columns].rename(columns=column_mapping)
                
                # Convert DataFrame to list of tuples
                records = [tuple(row) for row in df.to_numpy()]
                
                # Prepare insert query
                placeholders = ",".join(["%s"] * len(df.columns))
                insert_query = f"""
                    INSERT INTO {target_table} ({','.join(f'"{col}"' for col in df.columns)})
                    VALUES ({placeholders})
                """
                
                # Execute insert
                cur.executemany(insert_query, records)
                conn.commit()
                log_message(log_file, "DataLoad", f"Loaded {len(records)} rows to {target_table} with columns: {', '.join(df.columns)}",
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

    # Find matching files using regex pattern
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

    success = True
    dataset_ids = []
    for file_path in files:
        filename = os.path.basename(file_path)
        log_message(log_file, "Processing", f"Processing file: {filename}",
                    run_uuid=run_uuid, stepcounter=f"File_{filename}_0", user=user, script_start_time=script_start_time)

        # Extract dataset date and label from filename
        dataset_date_match = re.search(r'\d{8}', filename)
        dataset_date = datetime.strptime(dataset_date_match.group(0), '%Y%m%d').date() if dataset_date_match else datetime.now().date()
        label = parse_metadata(filename, config, config["metadata_label_source"], config["metadata_label_location"], config["delimiter"], log_file, run_uuid, user, script_start_time)
        if not label:
            label = config["config_name"]

        # Insert dataset record
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                dataset_id = insert_dataset(cur, config["config_name"], dataset_date, label, log_file, run_uuid, user, script_start_time)
                if not dataset_id:
                    log_message(log_file, "Error", "Failed to create dataset for file {filename}. Skipping.",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_1", user=user, script_start_time=script_start_time)
                    success = False
                    continue
                dataset_ids.append(dataset_id)
                conn.commit()

        # Convert XLS/XLSX to CSV if needed
        csv_path = file_path
        if config["file_type"] in ["XLS", "XLSX"]:
            csv_path = os.path.join(FILE_WATCHER_DIR, f"{timestamp}_{filename}.csv")
            try:
                xls_to_csv(file_path)
                if not os.path.exists(csv_path):
                    log_message(log_file, "Error", f"Failed to convert {filename} to CSV",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_2", user=user, script_start_time=script_start_time)
                    success = False
                    continue
                log_message(log_file, "Conversion", f"Converted {filename} to {csv_path}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_3", user=user, script_start_time=script_start_time)
            except Exception as e:
                log_message(log_file, "Error", f"Conversion error for {filename}: {str(e)}",
                            run_uuid=run_uuid, stepcounter=f"File_{filename}_4", user=user, script_start_time=script_start_time)
                success = False
                continue

        # Read CSV to get source columns and data lengths
        try:
            df = pd.read_csv(csv_path)
            log_message(log_file, "Processing", f"Read {len(df)} rows from {csv_path} with columns: {', '.join(df.columns)}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_5", user=user, script_start_time=script_start_time)
        except Exception as e:
            log_message(log_file, "Error", f"Failed to read CSV {csv_path}: {str(e)}",
                        run_uuid=run_uuid, stepcounter=f"File_{filename}_6", user=user, script_start_time=script_start_time)
            success = False
            if csv_path != file_path and os.path.exists(csv_path):
                os.remove(csv_path)
            continue

        # Get column lengths
        column_lengths = get_column_lengths(df)

        # Check if table exists and handle columns
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # Check if table exists
                if not table_exists(cur, config["target_table"]):
                    if config["importstrategyid"] == 1:
                        # Strategy 1: Create table with source columns plus metadata
                        columns = []
                        table_name = config["target_table"].split('.')[-1]
                        columns.append(f'"{table_name}id" VARCHAR(255) PRIMARY KEY')
                        columns.append('"datasetid" INT NOT NULL REFERENCES dba.tDataSet(DataSetID)')
                        for col in df.columns:
                            varchar_length = 1000 if column_lengths.get(col, 255) > 255 else 255
                            columns.append(f'"{col}" VARCHAR({varchar_length})')
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
                            log_message(log_file, "SchemaUpdate", f"Created table {config['target_table']} with columns: {table_name}id, datasetid, {', '.join(df.columns)}",
                                        run_uuid=run_uuid, stepcounter="SchemaCreate_0", user=user, script_start_time=script_start_time)
                        except psycopg2.Error as e:
                            log_message(log_file, "Error", f"Failed to create table {config['target_table']}: {str(e)}",
                                        run_uuid=run_uuid, stepcounter="SchemaCreate_1", user=user, script_start_time=script_start_time)
                            success = False
                            continue
                    else:
                        log_message(log_file, "Error", f"Table {config['target_table']} does not exist and importstrategyid {config['importstrategyid']} does not allow creation",
                                    run_uuid=run_uuid, stepcounter="SchemaCheck_0", user=user, script_start_time=script_start_time)
                        success = False
                        continue

                # Get table and source columns
                table_columns = get_table_columns(cur, config["target_table"])
                source_columns = list(df.columns)
                log_message(log_file, "SchemaCheck", f"Table columns: {', '.join(table_columns)}",
                            run_uuid=run_uuid, stepcounter="SchemaCheck_1", user=user, script_start_time=script_start_time)
                log_message(log_file, "SchemaCheck", f"Source columns: {', '.join(source_columns)}",
                            run_uuid=run_uuid, stepcounter="SchemaCheck_2", user=user, script_start_time=script_start_time)

                # Identify new and missing columns
                new_columns = [col for col in source_columns if col not in table_columns]
                missing_columns = [col for col in table_columns if col not in source_columns and col not in [f"{table_name}id", 'datasetid', 'metadata_label', 'event_date']]

                # Apply importstrategyid
                if config["importstrategyid"] == 1:  # Import and create new columns
                    if new_columns:
                        if not add_columns_to_table(cur, config["target_table"], new_columns, column_lengths, log_file, run_uuid, user, script_start_time):
                            success = False
                            continue
                        conn.commit()
                elif config["importstrategyid"] == 2:  # Import only (ignore new columns)
                    pass
                elif config["importstrategyid"] == 3:  # Import or fail if columns missing
                    if missing_columns:
                        log_message(log_file, "Error", f"Missing required columns in source file: {', '.join(missing_columns)}",
                                    run_uuid=run_uuid, stepcounter="SchemaCheck_3", user=user, script_start_time=script_start_time)
                        success = False
                        continue

                # Parse metadata
                metadata_label = parse_metadata(filename, config, config["metadata_label_source"],
                                               config["metadata_label_location"], config["delimiter"],
                                               log_file, run_uuid, user, script_start_time)
                event_date = parse_metadata(filename, config, config["DateConfig"],
                                            config["DateLocation"], config["delimiter"],
                                            log_file, run_uuid, user, script_start_time)

                # Load data to PostgreSQL
                if load_data_to_postgres(df, config["target_table"], dataset_id, metadata_label, event_date,
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
                        success = False
                else:
                    log_message(log_file, "Error", f"Failed to load data from {filename} to {config['target_table']}",
                                run_uuid=run_uuid, stepcounter=f"File_{filename}_8", user=user, script_start_time=script_start_time)
                    success = False

                # Clean up temporary CSV if created
                if csv_path != file_path and os.path.exists(csv_path):
                    os.remove(csv_path)

    # Update dataset status for all processed datasets
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            for dataset_id in dataset_ids:
                update_dataset_status(cur, dataset_id, success, log_file, run_uuid, user, script_start_time)
            conn.commit()

    log_message(log_file, "Finalization", f"Completed processing for config_id {config_id}",
                run_uuid=run_uuid, stepcounter="Finalization_0", user=user, script_start_time=script_start_time)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generic_import.py <config_id>")
        sys.exit(1)
    config_id = int(sys.argv[1])
    generic_import(config_id)