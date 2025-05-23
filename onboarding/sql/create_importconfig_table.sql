-- Create the importstrategy table in the dba schema
CREATE TABLE IF NOT EXISTS dba.timportstrategy (
    importstrategyid SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- Add comments to importstrategy table and columns
COMMENT ON TABLE dba.timportstrategy IS 'Table defining import strategies for timportconfig, specifying how to handle column mismatches during data import.';
COMMENT ON COLUMN dba.timportstrategy.importstrategyid IS 'Unique identifier for the import strategy.';
COMMENT ON COLUMN dba.timportstrategy.name IS 'Descriptive name of the import strategy (e.g., ''Import and create new columns if needed'').';

-- Insert predefined import strategies
INSERT INTO dba.timportstrategy (importstrategyid, name) VALUES
(1, 'Import and create new columns if needed'),
(2, 'Import only (ignores new columns)'),
(3, 'Import or fail if columns are missing from source file')
ON CONFLICT (importstrategyid) DO NOTHING;

-- Creating the timportconfig table in the dba schema to manage flat file imports
CREATE TABLE IF NOT EXISTS dba."timportconfig" (
    config_id SERIAL PRIMARY KEY,
    -- Unique identifier for each configuration
    config_name VARCHAR(100) NOT NULL UNIQUE,
    -- Descriptive name for the configuration (e.g., 'MeetMaxURLCheckImport')
    datasource VARCHAR(100) NOT NULL,
    -- Descriptive name of the data source (e.g., 'MeetMax')
    datasettype VARCHAR(100) NOT NULL,
    -- Descriptive name of the dataset type (e.g., 'MetaData')
    source_directory VARCHAR(255) NOT NULL,
    -- Directory where input files are located (e.g., '/home/yostfundsadmin/client_etl_workflow/file_watcher')
    archive_directory VARCHAR(255) NOT NULL,
    -- Directory where files are moved after processing (e.g., '/home/yostfundsadmin/client_etl_workflow/archive/import_MeetMaxURLCheckImport')
    file_pattern VARCHAR(255) NOT NULL,
    -- Pattern to match files (e.g., '*.csv', '*MeetMax*.xls', regex: '\d{8}T\d{6}_MeetMax.*\.csv')
    file_type VARCHAR(10) NOT NULL CHECK (file_type IN ('CSV', 'XLS', 'XLSX')),
    -- Type of file to process (CSV, XLS, XLSX)
    metadata_label_source VARCHAR(50) NOT NULL CHECK (metadata_label_source IN ('filename', 'file_content', 'static')),
    -- Source of metadata label (filename, specific column in file, or static user-defined value)
    metadata_label_location VARCHAR(255),
    -- Location details for metadata extraction
    -- For 'filename': position index (e.g., '0' for first part before delimiter)
    -- For 'file_content': column name (e.g., 'EventName')
    -- For 'static': user-defined value (e.g., 'MeetMaxURLCheck')
    dateconfig VARCHAR(50) NOT NULL CHECK (dateconfig IN ('filename', 'file_content', 'static')),
    -- Source of date metadata (filename, specific column in file, or static date value)
    datelocation VARCHAR(255),
    -- Location details for date extraction
    -- For 'filename': position index (e.g., '0' for first part before delimiter)
    -- For 'file_content': column name (e.g., 'EventDate')
    -- For 'static': fixed date format defined by dateformat
    dateformat VARCHAR(50),
    -- Format of the date (e.g., 'yyyyMMddTHHmmss' for '20250520T214109', 'yyyy-MM-dd' for '2025-05-16')
    delimiter VARCHAR(10),
    -- Delimiter used for parsing filenames (e.g., '_'), NULL if not applicable
    target_table VARCHAR(100) NOT NULL,
    -- Target database table for import (e.g., 'public.tmeetmaxurlcheck')
    importstrategyid INT NOT NULL DEFAULT 1,
    -- Foreign key to importstrategy table, defining how to handle column mismatches
    is_active BIT(1) DEFAULT '1',
    -- Flag to enable/disable the configuration (1 = active, 0 = inactive)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp when the configuration was created
    last_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp when the configuration was last modified
    CONSTRAINT fk_importstrategyid FOREIGN KEY (importstrategyid) REFERENCES dba.timportstrategy(importstrategyid),
    CONSTRAINT valid_directories CHECK (
        source_directory != archive_directory
        AND source_directory ~ '^/.*[^/]$'
        AND archive_directory ~ '^/.*[^/]$'
    ),
    -- Ensure directories are valid absolute paths (no trailing slash) and distinct
    CONSTRAINT valid_date CHECK (
        (dateconfig = 'filename' AND datelocation ~ '^[0-9]+$' AND delimiter IS NOT NULL AND dateformat IS NOT NULL)
        OR (dateconfig = 'file_content' AND datelocation ~ '^[a-zA-Z0-9_]+$' AND dateformat IS NOT NULL)
        OR (dateconfig = 'static' AND dateformat IS NOT NULL)
    )
    -- Ensure DateLocation and DateFormat are appropriate for the DateConfig
);

-- Adding a comment to describe the table
COMMENT ON TABLE dba."timportconfig" IS 'Configuration table for importing flat files (CSV, XLS, XLSX) into the database, specifying file patterns, directories, metadata, date extraction rules, delimiter, date format, data source/type descriptions, and import strategy.';

-- Adding comments to columns for clarity
COMMENT ON COLUMN dba."timportconfig".config_id IS 'Unique identifier for the configuration.';
COMMENT ON COLUMN dba."timportconfig".config_name IS 'Descriptive name of the configuration.';
COMMENT ON COLUMN dba."timportconfig".datasource IS 'Descriptive name of the data source (e.g., ''MeetMax'').';
COMMENT ON COLUMN dba."timportconfig".datasettype IS 'Descriptive name of the dataset type (e.g., ''MetaData'').';
COMMENT ON COLUMN dba."timportconfig".source_directory IS 'Absolute path to the directory containing input files.';
COMMENT ON COLUMN dba."timportconfig".archive_directory IS 'Absolute path to the directory where files are archived after processing.';
COMMENT ON COLUMN dba."timportconfig".file_pattern IS 'Pattern to match files (glob or regex, e.g., "*.csv" or "\d{8}T\d{6}_.*\.csv").';
COMMENT ON COLUMN dba."timportconfig".file_type IS 'Type of file to process (CSV, XLS, XLSX).';
COMMENT ON COLUMN dba."timportconfig".metadata_label_source IS 'Source of the metadata label (filename, file_content, or static user-defined value).';
COMMENT ON COLUMN dba."timportconfig".metadata_label_location IS 'Location details for metadata extraction (position index for filename, column name for file_content, user-defined value for static).';
COMMENT ON COLUMN dba."timportconfig".dateconfig IS 'Source of date metadata (filename, file_content, or static date value).';
COMMENT ON COLUMN dba."timportconfig".datelocation IS 'Location details for date extraction (position index for filename, column name for file_content, fixed date for static).';
COMMENT ON COLUMN dba."timportconfig".dateformat IS 'Format of the date (e.g., ''yyyyMMddTHHmmss'' for ''20250520T214109'', ''yyyy-MM-dd'' for ''2025-05-16'').';
COMMENT ON COLUMN dba."timportconfig".delimiter IS 'Delimiter used for parsing filenames (e.g., ''_'', NULL if not applicable).';
COMMENT ON COLUMN dba."timportconfig".target_table IS 'Target database table for the imported data.';
COMMENT ON COLUMN dba."timportconfig".importstrategyid IS 'Foreign key to importstrategy table, defining how to handle column mismatches (e.g., add new columns, ignore, or fail).';
COMMENT ON COLUMN dba."timportconfig".is_active IS 'Flag indicating whether the configuration is active (1 = active, 0 = inactive).';
COMMENT ON COLUMN dba."timportconfig".created_at IS 'Timestamp when the configuration was created.';
COMMENT ON COLUMN dba."timportconfig".last_modified_at IS 'Timestamp when the configuration was last modified.';

-- Creating a stored procedure for inserting a new timportconfig row
CREATE OR REPLACE PROCEDURE dba.pimportconfigi(
    p_config_name VARCHAR,
    p_datasource VARCHAR,
    p_datasettype VARCHAR,
    p_source_directory VARCHAR,
    p_archive_directory VARCHAR,
    p_file_pattern VARCHAR,
    p_file_type VARCHAR,
    p_metadata_label_source VARCHAR,
    p_metadata_label_location VARCHAR,
    p_dateconfig VARCHAR,
    p_datelocation VARCHAR,
    p_dateformat VARCHAR,
    p_delimiter VARCHAR,
    p_target_table VARCHAR,
    p_importstrategyid INT,
    p_is_active BIT(1)
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dba."timportconfig" (
        config_name,
        datasource,
        datasettype,
        source_directory,
        archive_directory,
        file_pattern,
        file_type,
        metadata_label_source,
        metadata_label_location,
        dateconfig,
        datelocation,
        dateformat,
        delimiter,
        target_table,
        importstrategyid,
        is_active,
        created_at,
        last_modified_at
    ) VALUES (
        p_config_name,
        p_datasource,
        p_datasettype,
        p_source_directory,
        p_archive_directory,
        p_file_pattern,
        p_file_type,
        p_metadata_label_source,
        p_metadata_label_location,
        p_dateconfig,
        p_datelocation,
        p_dateformat,
        p_delimiter,
        p_target_table,
        p_importstrategyid,
        p_is_active,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ) ON CONFLICT (config_name) DO NOTHING;
END;
$$;

-- Inserting configurations using the insert procedure
DO $$
BEGIN
    -- Insert MeetMaxURLCheckImport configuration
    IF NOT EXISTS (SELECT 1 FROM dba."timportconfig" WHERE config_name = 'MeetMaxURLCheckImport') THEN
        CALL dba.pimportconfigi(
            'MeetMaxURLCheckImport',
            'MeetMax',
            'MeetMaxURL',
            '/home/yostfundsadmin/client_etl_workflow/file_watcher',
            '/home/yostfundsadmin/client_etl_workflow/archive/import_MeetMaxURLCheckImport',
            '\d{8}T\d{6}_MeetMaxURLCheck\.csv',
            'CSV',
            'static',
            'MeetMaxURLCheck',
            'filename',
            '0',
            'yyyyMMddTHHmmss',
            '_',
            'public.tmeetmaxurlcheck',
            1,
            '1'::BIT(1)
        );
    ELSE
        -- Update existing configuration to match new settings
        CALL dba.pimportconfigu(
            1,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            'static',
            'MeetMaxURLCheck',
            'filename',
            '0',
            'yyyyMMddTHHmmss',
            '_',
            NULL,
            NULL,
            NULL
        );
    END IF;

    -- Insert MeetMax_Events_XLS_Import configuration
    IF NOT EXISTS (SELECT 1 FROM dba."timportconfig" WHERE config_name = 'MeetMax_Events_XLS_Import') THEN
        CALL dba.pimportconfigi(
            'MeetMax_Events_XLS_Import',
            'MeetMax',
            'MeetMaxEvents',
            '/home/yostfundsadmin/client_etl_workflow/file_watcher',
            '/home/yostfundsadmin/client_etl_workflow/archive/meetmaxevents',
            '^\d{8}T\d{6}_MeetMax_\d+\.xls$',
            'XLS',
            'filename',
            '2',
            'filename',
            '0',
            'yyyyMMddTHHmmss',
            '_',
            'public.tmeetmaxevent',
            1,
            '1'::BIT(1)
        );
    END IF;
END;
$$;

-- Creating a stored procedure for updating an existing timportconfig row with partial updates
CREATE OR REPLACE PROCEDURE dba.pimportconfigu(
    p_config_id INT,
    p_config_name VARCHAR DEFAULT NULL,
    p_datasource VARCHAR DEFAULT NULL,
    p_datasettype VARCHAR DEFAULT NULL,
    p_source_directory VARCHAR DEFAULT NULL,
    p_archive_directory VARCHAR DEFAULT NULL,
    p_file_pattern VARCHAR DEFAULT NULL,
    p_file_type VARCHAR DEFAULT NULL,
    p_metadata_label_source VARCHAR DEFAULT NULL,
    p_metadata_label_location VARCHAR DEFAULT NULL,
    p_dateconfig VARCHAR DEFAULT NULL,
    p_datelocation VARCHAR DEFAULT NULL,
    p_dateformat VARCHAR DEFAULT NULL,
    p_delimiter VARCHAR DEFAULT NULL,
    p_target_table VARCHAR DEFAULT NULL,
    p_importstrategyid INT DEFAULT NULL,
    p_is_active BIT(1) DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE dba."timportconfig"
    SET
        config_name = COALESCE(p_config_name, config_name),
        datasource = COALESCE(p_datasource, datasource),
        datasettype = COALESCE(p_datasettype, datasettype),
        source_directory = COALESCE(p_source_directory, source_directory),
        archive_directory = COALESCE(p_archive_directory, archive_directory),
        file_pattern = COALESCE(p_file_pattern, file_pattern),
        file_type = COALESCE(p_file_type, file_type),
        metadata_label_source = COALESCE(p_metadata_label_source, metadata_label_source),
        metadata_label_location = COALESCE(p_metadata_label_location, metadata_label_location),
        dateconfig = COALESCE(p_dateconfig, dateconfig),
        datelocation = COALESCE(p_datelocation, datelocation),
        dateformat = COALESCE(p_dateformat, dateformat),
        delimiter = COALESCE(p_delimiter, delimiter),
        target_table = COALESCE(p_target_table, target_table),
        importstrategyid = COALESCE(p_importstrategyid, importstrategyid),
        is_active = COALESCE(p_is_active, is_active),
        last_modified_at = CURRENT_TIMESTAMP
    WHERE config_id = p_config_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No configuration found with config_id %', p_config_id;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error updating configuration: %', SQLERRM;
END;
$$;

-- Conditionally grant permissions to etl_user
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.routine_privileges
        WHERE grantee = 'etl_user'
        AND routine_schema = 'dba'
        AND routine_name = 'pimportconfigi'
    ) THEN
        GRANT EXECUTE ON PROCEDURE dba.pimportconfigi TO etl_user;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.routine_privileges
        WHERE grantee = 'etl_user'
        AND routine_schema = 'dba'
        AND routine_name = 'pimportconfigu'
    ) THEN
        GRANT EXECUTE ON PROCEDURE dba.pimportconfigu TO etl_user;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.table_privileges
        WHERE grantee = 'public'
        AND table_schema = 'dba'
        AND table_name = 'timportconfig'
        AND privilege_type = 'UPDATE'
    ) THEN
        REVOKE UPDATE ON dba."timportconfig" FROM PUBLIC;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.table_privileges
        WHERE grantee = 'public'
        AND table_schema = 'dba'
        AND table_name = 'timportstrategy'
        AND privilege_type = 'UPDATE'
    ) THEN
        REVOKE UPDATE ON dba.timportstrategy FROM PUBLIC;
    END IF;
END;
$$;