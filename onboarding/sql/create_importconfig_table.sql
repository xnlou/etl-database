-- Creating the tImportConfig table in the dba schema to manage flat file imports
CREATE TABLE IF NOT EXISTS dba."tImportConfig" (
    config_id SERIAL PRIMARY KEY,
    -- Unique identifier for each configuration
    config_name VARCHAR(100) NOT NULL UNIQUE,
    -- Descriptive name for the configuration (e.g., 'MeetMaxCSVImport')
    file_pattern VARCHAR(255) NOT NULL,
    -- Pattern to match files (e.g., '*.csv', '*MeetMax*.xls', regex: '\d{8}T\d{6}_MeetMax.*\.csv')
    source_directory VARCHAR(255) NOT NULL,
    -- Directory where input files are located (e.g., '/home/etl_user/client_etl_workflow/file_watcher')
    archive_directory VARCHAR(255) NOT NULL,
    -- Directory where files are moved after processing (e.g., '/home/etl_user/client_etl_workflow/archive')
    file_type VARCHAR(10) NOT NULL CHECK (file_type IN ('CSV', 'XLS', 'XLSX')),
    -- Type of file to process (CSV, XLS, XLSX)
    metadata_label_source VARCHAR(50) NOT NULL CHECK (metadata_label_source IN ('filename', 'file_content', 'static')),
    -- Source of metadata label (filename, specific column in file, or static user-defined value)
    metadata_label_location VARCHAR(255),
    -- Location details for metadata extraction
    -- For 'filename': regex pattern (e.g., '\d{8}T\d{6}_(.*)\.csv')
    -- For 'file_content': column name (e.g., 'EventName')
    -- For 'static': user-defined value (e.g., 'MeetMax2025')
    target_table VARCHAR(100) NOT NULL,
    -- Target database table for import (e.g., 'dba.meetmax_data')
    is_active BOOLEAN DEFAULT TRUE,
    -- Flag to enable/disable the configuration
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp when the configuration was created
    last_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp when the configuration was last modified
    CONSTRAINT valid_directories CHECK (
        source_directory != archive_directory
        AND source_directory ~ '^/.*[^/]$'
        AND archive_directory ~ '^/.*[^/]$'
    ),
    -- Ensure directories are valid absolute paths (no trailing slash) and distinct
    CONSTRAINT valid_metadata CHECK (
        (metadata_label_source = 'filename' AND metadata_label_location ~ '.*\..*')
        OR (metadata_label_source = 'file_content' AND metadata_label_location ~ '^[a-zA-Z0-9_]+$')
        OR (metadata_label_source = 'static' AND metadata_label_location IS NOT NULL AND metadata_label_location != '')
    )
    -- Ensure metadata_label_location is appropriate for the metadata_label_source
);

-- Creating an index on file_pattern for faster lookups
CREATE INDEX idx_tImportConfig_file_pattern ON dba."tImportConfig" (file_pattern);

-- Creating an index on is_active for filtering active configurations
CREATE INDEX idx_tImportConfig_is_active ON dba."tImportConfig" (is_active);

-- Adding a comment to describe the table
COMMENT ON TABLE dba."tImportConfig" IS 'Configuration table for importing flat files (CSV, XLS, XLSX) into the database, specifying file patterns, directories, and metadata extraction rules.';

-- Adding comments to columns for clarity
COMMENT ON COLUMN dba."tImportConfig".config_id IS 'Unique identifier for the configuration.';
COMMENT ON COLUMN dba."tImportConfig".config_name IS 'Descriptive name of the configuration.';
COMMENT ON COLUMN dba."tImportConfig".file_pattern IS 'Pattern to match files (glob or regex, e.g., "*.csv" or "\d{8}T\d{6}_.*\.csv").';
COMMENT ON COLUMN dba."tImportConfig".source_directory IS 'Absolute path to the directory containing input files.';
COMMENT ON COLUMN dba."tImportConfig".archive_directory IS 'Absolute path to the directory where files are archived after processing.';
COMMENT ON COLUMN dba."tImportConfig".file_type IS 'Type of file to process (CSV, XLS, XLSX).';
COMMENT ON COLUMN dba."tImportConfig".metadata_label_source IS 'Source of the metadata label (filename, file_content, or static user-defined value).';
COMMENT ON COLUMN dba."tImportConfig".metadata_label_location IS 'Location details for metadata extraction (regex for filename, column name for file_content, user-defined value for static).';
COMMENT ON COLUMN dba."tImportConfig".target_table IS 'Target database table for the imported data.';
COMMENT ON COLUMN dba."tImportConfig".is_active IS 'Flag indicating whether the configuration is active.';
COMMENT ON COLUMN dba."tImportConfig".created_at IS 'Timestamp when the configuration was created.';
COMMENT ON COLUMN dba."tImportConfig".last_modified_at IS 'Timestamp when the configuration was last modified.';

-- Inserting example configurations
INSERT INTO dba."tImportConfig" (
    config_name,
    file_pattern,
    source_directory,
    archive_directory,
    file_type,
    metadata_label_source,
    metadata_label_location,
    target_table,
    is_active
) VALUES
(
    'MeetMaxCSVImport',
    '\d{8}T\d{6}_MeetMax.*\.csv',
    '/home/etl_user/client_etl_workflow/file_watcher',
    '/home/etl_user/client_etl_workflow/archive',
    'CSV',
    'filename',
    '\d{8}T\d{6}_(.*)\.csv',
    'dba.meetmax_data',
    TRUE
),
(
    'MeetMaxXLSImport',
    '\d{8}T\d{6}_MeetMax.*\.xls',
    '/home/etl_user/client_etl_workflow/file_watcher',
    '/home/etl_user/client_etl_workflow/archive',
    'XLS',
    'file_content',
    'EventName',
    'dba.meetmax_data',
    TRUE
),
(
    'StaticLabelImport',
    'summary_.*\.csv',
    '/home/etl_user/client_etl_workflow/file_watcher',
    '/home/etl_user/client_etl_workflow/archive',
    'CSV',
    'static',
    'Summary2025',
    'dba.daily_summaries',
    TRUE
);

-- Creating a stored procedure for inserting a new tImportConfig row
CREATE OR REPLACE PROCEDURE dba.insert_tImportConfig(
    p_config_name VARCHAR,
    p_file_pattern VARCHAR,
    p_source_directory VARCHAR,
    p_archive_directory VARCHAR,
    p_file_type VARCHAR,
    p_metadata_label_source VARCHAR,
    p_metadata_label_location VARCHAR,
    p_target_table VARCHAR,
    p_is_active BOOLEAN
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dba."tImportConfig" (
        config_name,
        file_pattern,
        source_directory,
        archive_directory,
        file_type,
        metadata_label_source,
        metadata_label_location,
        target_table,
        is_active,
        created_at,
        last_modified_at
    ) VALUES (
        p_config_name,
        p_file_pattern,
        p_source_directory,
        p_archive_directory,
        p_file_type,
        p_metadata_label_source,
        p_metadata_label_location,
        p_target_table,
        p_is_active,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    );
    COMMIT;
END;
$$;

-- Creating a stored procedure for updating an existing tImportConfig row with partial updates
CREATE OR REPLACE PROCEDURE dba.update_tImportConfig(
    p_config_id INT,
    p_config_name VARCHAR DEFAULT NULL,
    p_file_pattern VARCHAR DEFAULT NULL,
    p_source_directory VARCHAR DEFAULT NULL,
    p_archive_directory VARCHAR DEFAULT NULL,
    p_file_type VARCHAR DEFAULT NULL,
    p_metadata_label_source VARCHAR DEFAULT NULL,
    p_metadata_label_location VARCHAR DEFAULT NULL,
    p_target_table VARCHAR DEFAULT NULL,
    p_is_active BOOLEAN DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE dba."tImportConfig"
    SET
        config_name = COALESCE(p_config_name, config_name),
        file_pattern = COALESCE(p_file_pattern, file_pattern),
        source_directory = COALESCE(p_source_directory, source_directory),
        archive_directory = COALESCE(p_archive_directory, archive_directory),
        file_type = COALESCE(p_file_type, file_type),
        metadata_label_source = COALESCE(p_metadata_label_source, metadata_label_source),
        metadata_label_location = COALESCE(p_metadata_label_location, metadata_label_location),
        target_table = COALESCE(p_target_table, target_table),
        is_active = COALESCE(p_is_active, is_active),
        last_modified_at = CURRENT_TIMESTAMP
    WHERE config_id = p_config_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No configuration found with config_id %', p_config_id;
    END IF;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE EXCEPTION 'Error updating configuration: %', SQLERRM;
END;
$$;

-- Example usage of stored procedures
-- Insert a new configuration
CALL dba.insert_tImportConfig(
    'NewConfig',
    '*.csv',
    '/home/etl_user/client_etl_workflow/file_watcher',
    '/home/etl_user/client_etl_workflow/archive',
    'CSV',
    'static',
    'CustomLabel2025',
    'dba.custom_data',
    TRUE
);

-- Update only the is_active field for an existing configuration
CALL dba.update_tImportConfig(
    p_config_id => 1,
    p_is_active => FALSE
);

-- Update multiple fields, leaving others unchanged
CALL dba.update_tImportConfig(
    p_config_id => 1,
    p_config_name => 'UpdatedMeetMaxCSVImport',
    p_file_pattern => '\d{8}T\d{6}_MeetMaxUpdated.*\.csv',
    p_is_active => TRUE
);

-- Restrict direct updates to the tImportConfig table
REVOKE UPDATE ON dba."tImportConfig" FROM PUBLIC;
GRANT EXECUTE ON PROCEDURE dba.insert_tImportConfig TO etl_user;
GRANT EXECUTE ON PROCEDURE dba.update_tImportConfig TO etl_user;