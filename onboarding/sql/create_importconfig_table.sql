
-- Create the importstrategy table in the dba schema
CREATE TABLE IF NOT EXISTS dba.timportstrategy (
    importstrategyID SERIAL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL UNIQUE
);

-- Add comments to importstrategy table and columns
COMMENT ON TABLE dba.timportstrategy IS 'Table defining import strategies for timportconfig, specifying how to handle column mismatches during data import.';
COMMENT ON COLUMN dba.timportstrategy.importstrategyID IS 'Unique identifier for the import strategy.';
COMMENT ON COLUMN dba.timportstrategy.Name IS 'Descriptive name of the import strategy (e.g., ''Import and create new columns if needed'').';

-- Insert predefined import strategies
INSERT INTO dba.timportstrategy (importstrategyID, Name) VALUES
(1, 'Import and create new columns if needed'),
(2, 'Import only (ignores new columns)'),
(3, 'Import or fail if columns are missing from source file')
ON CONFLICT (importstrategyID) DO NOTHING;

-- Creating the timportconfig table in the dba schema to manage flat file imports
CREATE TABLE IF NOT EXISTS dba."timportconfig" (
    config_id SERIAL PRIMARY KEY,
    -- Unique identifier for each configuration
    config_name VARCHAR(100) NOT NULL UNIQUE,
    -- Descriptive name for the configuration (e.g., 'MeetMaxURLCheckImport')
    DataSource VARCHAR(100) NOT NULL,
    -- Descriptive name of the data source (e.g., 'MeetMax')
    DataSetType VARCHAR(100) NOT NULL,
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
    -- For 'filename': regex pattern (e.g., '_([^_]+)')
    -- For 'file_content': column name (e.g., 'EventName')
    -- For 'static': user-defined value (e.g., 'MeetMax2025')
    DateConfig VARCHAR(50) NOT NULL CHECK (DateConfig IN ('filename', 'file_content', 'static')),
    -- Source of date metadata (filename, specific column in file, or static date value)
    DateLocation VARCHAR(255),
    -- Location details for date extraction
    -- For 'filename': regex pattern (e.g., '\d{4}\d{2}\d{2}T\d{6}')
    -- For 'file_content': column name (e.g., 'EventDate')
    -- For 'static': fixed date value (e.g., '2025-05-16')
    delimiter VARCHAR(10),
    -- Delimiter used for parsing filenames (e.g., '_'), NULL if not applicable
    target_table VARCHAR(100) NOT NULL,
    -- Target database table for import (e.g., 'public.tmeetmaxurlcheck')
    importstrategyID INT NOT NULL DEFAULT 1,
    -- Foreign key to importstrategy table, defining how to handle column mismatches
    is_active BIT(1) DEFAULT '1',
    -- Flag to enable/disable the configuration (1 = active, 0 = inactive)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp when the configuration was created
    last_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp when the configuration was last modified
    CONSTRAINT fk_importstrategyID FOREIGN KEY (importstrategyID) REFERENCES dba.timportstrategy(importstrategyID),
    CONSTRAINT valid_directories CHECK (
        source_directory != archive_directory
        AND source_directory ~ '^/.*[^/]$'
        AND archive_directory ~ '^/.*[^/]$'
    ),
    -- Ensure directories are valid absolute paths (no trailing slash) and distinct
    CONSTRAINT valid_date CHECK (
        (DateConfig = 'filename' AND DateLocation ~ '.*\d.*' AND delimiter IS NOT NULL)
        OR (DateConfig = 'file_content' AND DateLocation ~ '^[a-zA-Z0-9_]+$')
        OR (DateConfig = 'static' AND DateLocation ~ '^\d{4}-\d{2}-\d{2}$')
    )
    -- Ensure DateLocation is appropriate for the DateConfig
);

-- Adding a comment to describe the table
COMMENT ON TABLE dba."timportconfig" IS 'Configuration table for importing flat files (CSV, XLS, XLSX) into the database, specifying file patterns, directories, metadata, date extraction rules, delimiter, data source/type descriptions, and import strategy.';

-- Adding comments to columns for clarity
COMMENT ON COLUMN dba."timportconfig".config_id IS 'Unique identifier for the configuration.';
COMMENT ON COLUMN dba."timportconfig".config_name IS 'Descriptive name of the configuration.';
COMMENT ON COLUMN dba."timportconfig".DataSource IS 'Descriptive name of the data source (e.g., ''MeetMax'').';
COMMENT ON COLUMN dba."timportconfig".DataSetType IS 'Descriptive name of the dataset type (e.g., ''MetaData'').';
COMMENT ON COLUMN dba."timportconfig".source_directory IS 'Absolute path to the directory containing input files.';
COMMENT ON COLUMN dba."timportconfig".archive_directory IS 'Absolute path to the directory where files are archived after processing.';
COMMENT ON COLUMN dba."timportconfig".file_pattern IS 'Pattern to match files (glob or regex, e.g., "*.csv" or "\d{8}T\d{6}_.*\.csv").';
COMMENT ON COLUMN dba."timportconfig".file_type IS 'Type of file to process (CSV, XLS, XLSX).';
COMMENT ON COLUMN dba."timportconfig".metadata_label_source IS 'Source of the metadata label (filename, file_content, or static user-defined value).';
COMMENT ON COLUMN dba."timportconfig".metadata_label_location IS 'Location details for metadata extraction (regex for filename, column name for file_content, user-defined value for static).';
COMMENT ON COLUMN dba."timportconfig".DateConfig IS 'Source of date metadata (filename, file_content, or static date value).';
COMMENT ON COLUMN dba."timportconfig".DateLocation IS 'Location details for date extraction (regex for filename, column name for file_content, fixed date for static).';
COMMENT ON COLUMN dba."timportconfig".delimiter IS 'Delimiter used for parsing filenames (e.g., ''_'', NULL if not applicable).';
COMMENT ON COLUMN dba."timportconfig".target_table IS 'Target database table for the imported data.';
COMMENT ON COLUMN dba."timportconfig".importstrategyID IS 'Foreign key to importstrategy table, defining how to handle column mismatches (e.g., add new columns, ignore, or fail).';
COMMENT ON COLUMN dba."timportconfig".is_active IS 'Flag indicating whether the configuration is active (1 = active, 0 = inactive).';
COMMENT ON COLUMN dba."timportconfig".created_at IS 'Timestamp when the configuration was created.';
COMMENT ON COLUMN dba."timportconfig".last_modified_at IS 'Timestamp when the configuration was last modified.';

-- Creating a stored procedure for inserting a new timportconfig row
CREATE OR REPLACE PROCEDURE dba.pimportconfigI(
    p_config_name VARCHAR,
    p_DataSource VARCHAR,
    p_DataSetType VARCHAR,
    p_source_directory VARCHAR,
    p_archive_directory VARCHAR,
    p_file_pattern VARCHAR,
    p_file_type VARCHAR,
    p_metadata_label_source VARCHAR,
    p_metadata_label_location VARCHAR,
    p_DateConfig VARCHAR,
    p_DateLocation VARCHAR,
    p_delimiter VARCHAR,
    p_target_table VARCHAR,
    p_ImportStrategyID INT,
    p_is_active BIT(1)
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dba."timportconfig" (
        config_name,
        DataSource,
        DataSetType,
        source_directory,
        archive_directory,
        file_pattern,
        file_type,
        metadata_label_source,
        metadata_label_location,
        DateConfig,
        DateLocation,
        delimiter,
        target_table,
        ImportStrategyID,
        is_active,
        created_at,
        last_modified_at
    ) VALUES (
        p_config_name,
        p_DataSource,
        p_DataSetType,
        p_source_directory,
        p_archive_directory,
        p_file_pattern,
        p_file_type,
        p_metadata_label_source,
        p_metadata_label_location,
        p_DateConfig,
        p_DateLocation,
        p_delimiter,
        p_target_table,
        p_ImportStrategyID,
        p_is_active,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ) ON CONFLICT (config_name) DO NOTHING;
END;
$$;

-- Inserting example configuration for MeetMaxURLCheckImport using the insert procedure
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM dba."timportconfig" WHERE config_name = 'MeetMaxURLCheckImport') THEN
        CALL dba.pimportconfigI(
            'MeetMaxURLCheckImport',
            'MeetMax',
            'MetaData',
            '/home/yostfundsadmin/client_etl_workflow/file_watcher',
            '/home/yostfundsadmin/client_etl_workflow/archive/import_MeetMaxURLCheckImport',
            '\d{8}T\d{6}_MeetMaxURLCheck\.csv',
            'CSV',
            'filename',
            '_([^_]+)',
            'filename',
            '\d{8}T\d{6}',
            '_',
            'public.tmeetmaxurlcheck',
            1, -- Import and create new columns if needed
            '1'::BIT(1)
        );
    ELSE
        RAISE NOTICE 'Configuration with config_name ''MeetMaxURLCheckImport'' already exists. Skipping insert.';
    END IF;
END;
$$;

-- Creating a stored procedure for updating an existing timportconfig row with partial updates
CREATE OR REPLACE PROCEDURE dba.pimportconfigU(
    p_config_id INT,
    p_config_name VARCHAR DEFAULT NULL,
    p_DataSource VARCHAR DEFAULT NULL,
    p_DataSetType VARCHAR DEFAULT NULL,
    p_source_directory VARCHAR DEFAULT NULL,
    p_archive_directory VARCHAR DEFAULT NULL,
    p_file_pattern VARCHAR DEFAULT NULL,
    p_file_type VARCHAR DEFAULT NULL,
    p_metadata_label_source VARCHAR DEFAULT NULL,
    p_metadata_label_location VARCHAR DEFAULT NULL,
    p_DateConfig VARCHAR DEFAULT NULL,
    p_DateLocation VARCHAR DEFAULT NULL,
    p_delimiter VARCHAR DEFAULT NULL,
    p_target_table VARCHAR DEFAULT NULL,
    p_ImportStrategyID INT DEFAULT NULL,
    p_is_active BIT(1) DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE dba."timportconfig"
    SET
        config_name = COALESCE(p_config_name, config_name),
        DataSource = COALESCE(p_DataSource, DataSource),
        DataSetType = COALESCE(p_DataSetType, DataSetType),
        source_directory = COALESCE(p_source_directory, source_directory),
        archive_directory = COALESCE(p_archive_directory, archive_directory),
        file_pattern = COALESCE(p_file_pattern, file_pattern),
        file_type = COALESCE(p_file_type, file_type),
        metadata_label_source = COALESCE(p_metadata_label_source, metadata_label_source),
        metadata_label_location = COALESCE(p_metadata_label_location, metadata_label_location),
        DateConfig = COALESCE(p_DateConfig, DateConfig),
        DateLocation = COALESCE(p_DateLocation, DateLocation),
        delimiter = COALESCE(p_delimiter, delimiter),
        target_table = COALESCE(p_target_table, target_table),
        ImportStrategyID = COALESCE(p_ImportStrategyID, ImportStrategyID),
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
        AND routine_name = 'pimportconfigI'
    ) THEN
        GRANT EXECUTE ON PROCEDURE dba.pimportconfigI TO etl_user;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.routine_privileges
        WHERE grantee = 'etl_user'
        AND routine_schema = 'dba'
        AND routine_name = 'pimportconfigU'
    ) THEN
        GRANT EXECUTE ON PROCEDURE dba.pimportconfigU TO etl_user;
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
        AND table_name = 'importstrategy'
        AND privilege_type = 'UPDATE'
    ) THEN
        REVOKE UPDATE ON dba.importstrategy FROM PUBLIC;
    END IF;
END;
$$;