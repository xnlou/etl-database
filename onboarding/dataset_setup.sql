-- Schema: dba
-- Database: Feeds
-- Description: Tables to track dataset metadata for ETL pipeline, including tDataSet and supporting lookup tables.
-- Naming Conventions: t for tables, f for functions, p for procedures, v for views.
-- Idempotency: Each object creation is guarded by an existence check to prevent errors on rerun.

-- Create schema dba if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dba') THEN
        CREATE SCHEMA dba;
        COMMENT ON SCHEMA dba IS 'Schema for ETL pipeline metadata and logging tables.';
    END IF;
END $$;

-- Create tDataSetType table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdatasettype') THEN
        CREATE TABLE dba.tDataSetType (
            DataSetTypeID SERIAL PRIMARY KEY,
            TypeName VARCHAR(50) NOT NULL UNIQUE,
            Description TEXT,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tDataSetType IS 'Stores dataset type definitions (e.g., MeetMax, ClientUpload).';
        COMMENT ON COLUMN dba.tDataSetType.DataSetTypeID IS 'Primary key for dataset type.';
        COMMENT ON COLUMN dba.tDataSetType.TypeName IS 'Unique name of the dataset type.';
        COMMENT ON COLUMN dba.tDataSetType.Description IS 'Optional description of the dataset type.';
        COMMENT ON COLUMN dba.tDataSetType.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tDataSetType.CreatedBy IS 'User who created the record.';
    END IF;
END $$;

-- Create tDataSource table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdatasource') THEN
        CREATE TABLE dba.tDataSource (
            DataSourceID SERIAL PRIMARY KEY,
            SourceName VARCHAR(50) NOT NULL UNIQUE,
            Description TEXT,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tDataSource IS 'Stores data source definitions (e.g., MeetMax Website, SFTP Upload).';
        COMMENT ON COLUMN dba.tDataSource.DataSourceID IS 'Primary key for data source.';
        COMMENT ON COLUMN dba.tDataSource.SourceName IS 'Unique name of the data source.';
        COMMENT ON COLUMN dba.tDataSource.Description IS 'Optional description of the data source.';
        COMMENT ON COLUMN dba.tDataSource.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tDataSource.CreatedBy IS 'User who created the record.';
    END IF;
END $$;

-- Create tDataStatus table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdatastatus') THEN
        CREATE TABLE dba.tDataStatus (
            DataStatusID SERIAL PRIMARY KEY,
            StatusName VARCHAR(50) NOT NULL UNIQUE,
            Description TEXT,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tDataStatus IS 'Stores status codes for datasets (e.g., Active, Inactive, Deleted).';
        COMMENT ON COLUMN dba.tDataStatus.DataStatusID IS 'Primary key for status.';
        COMMENT ON COLUMN dba.tDataStatus.StatusName IS 'Unique name of the status.';
        COMMENT ON COLUMN dba.tDataStatus.Description IS 'Optional description of the status.';
        COMMENT ON COLUMN dba.tDataStatus.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tDataStatus.CreatedBy IS 'User who created the record.';
    END IF;
END $$;

-- Create tDataSet table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdataset') THEN
        CREATE TABLE dba.tDataSet (
            DataSetID SERIAL PRIMARY KEY,
            DataSetDate DATE NOT NULL,
            Label VARCHAR(100) NOT NULL,
            DataSetTypeID INTEGER NOT NULL,
            DataSourceID INTEGER NOT NULL,
            DataStatusID INTEGER NOT NULL,
            EffFromDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            EffThruDate TIMESTAMP NOT NULL DEFAULT '9999-01-01',
            IsActive BOOLEAN NOT NULL DEFAULT TRUE,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER,
            CONSTRAINT fk_dataset_type FOREIGN KEY (DataSetTypeID) REFERENCES dba.tDataSetType (DataSetTypeID),
            CONSTRAINT fk_dataset_source FOREIGN KEY (DataSourceID) REFERENCES dba.tDataSource (DataSourceID),
            CONSTRAINT fk_dataset_status FOREIGN KEY (DataStatusID) REFERENCES dba.tDataStatus (DataStatusID),
            CONSTRAINT chk_eff_dates CHECK (EffFromDate <= EffThruDate)
        );

        COMMENT ON TABLE dba.tDataSet IS 'Tracks metadata for dataset loads in the ETL pipeline.';
        COMMENT ON COLUMN dba.tDataSet.DataSetID IS 'Primary key for the dataset.';
        COMMENT ON COLUMN dba.tDataSet.DataSetDate IS 'Date associated with the dataset (e.g., data reference date).';
        COMMENT ON COLUMN dba.tDataSet.Label IS 'Descriptive label for the dataset.';
        COMMENT ON COLUMN dba.tDataSet.DataSetTypeID IS 'Foreign key to tDataSetType, indicating dataset type.';
        COMMENT ON COLUMN dba.tDataSet.DataSourceID IS 'Foreign key to tDataSource, indicating data source.';
        COMMENT ON COLUMN dba.tDataSet.DataStatusID IS 'Foreign key to tDataStatus, indicating dataset status.';
        COMMENT ON COLUMN dba.tDataSet.EffFromDate IS 'Effective start date, defaults to creation time.';
        COMMENT ON COLUMN dba.tDataSet.EffThruDate IS 'Effective end date, defaults to 9999-01-01 for active records.';
        COMMENT ON COLUMN dba.tDataSet.IsActive IS 'Indicates if the dataset is active (TRUE) or inactive (FALSE).';
        COMMENT ON COLUMN dba.tDataSet.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tDataSet.CreatedBy IS 'User who created the record.';
    END IF;
END $$;

-- Create indexes for tDataSet if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tdataset_datasetdate') THEN
        CREATE INDEX idx_tdataset_datasetdate ON dba.tDataSet (DataSetDate);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tdataset_isactive') THEN
        CREATE INDEX idx_tdataset_isactive ON dba.tDataSet (IsActive);
    END IF;
END $$;

-- Create function fEnforceSingleActiveDataSet if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba') AND proname = 'fenforcesingleactivedataset') THEN
        CREATE FUNCTION dba.fEnforceSingleActiveDataSet()
        RETURNS TRIGGER AS $$
        BEGIN
            -- If inserting or updating to IsActive = TRUE, set other records with same Label, DataSetTypeID, and DataSetDate to IsActive = FALSE
            IF NEW.IsActive = TRUE THEN
                UPDATE dba.tDataSet
                SET IsActive = FALSE,
                    EffThruDate = CURRENT_TIMESTAMP,
                    DataStatusID = (SELECT DataStatusID FROM dba.tDataStatus WHERE StatusName = 'Inactive')
                WHERE Label = NEW.Label
                  AND DataSetTypeID = NEW.DataSetTypeID
                  AND DataSetDate = NEW.DataSetDate
                  AND DataSetID != NEW.DataSetID
                  AND IsActive = TRUE;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    END IF;
END $$;

-- Create trigger tTriggerEnforceSingleActiveDataSet if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ttriggerenforcesingleactivedataset' AND tgrelid = (SELECT oid FROM pg_class WHERE relname = 'tdataset' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba'))) THEN
        CREATE TRIGGER tTriggerEnforceSingleActiveDataSet
        AFTER INSERT OR UPDATE OF IsActive
        ON dba.tDataSet
        FOR EACH ROW
        WHEN (NEW.IsActive = TRUE)
        EXECUTE FUNCTION dba.fEnforceSingleActiveDataSet();
    END IF;
END $$;

-- Insert initial data for lookup tables only if they are empty
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM dba.tDataSetType) = 0 THEN
        INSERT INTO dba.tDataSetType (TypeName, Description) VALUES
            ('MeetMax', 'Datasets from MeetMax event scraping'),
            ('ClientUpload', 'Datasets uploaded by clients via SFTP');
    END IF;
END $$;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM dba.tDataSource) = 0 THEN
        INSERT INTO dba.tDataSource (SourceName, Description) VALUES
            ('MeetMaxWebsite', 'Data scraped from MeetMax website'),
            ('SFTPUpload', 'Data uploaded by clients via SFTP');
    END IF;
END $$;

DO $$
BEGIN
    IF (SELECT COUNT(*) FROM dba.tDataStatus) = 0 THEN
        INSERT INTO dba.tDataStatus (StatusName, Description) VALUES
            ('Active', 'Dataset is currently active and in use'),
            ('Inactive', 'Dataset is no longer active but retained for history'),
            ('Deleted', 'Dataset has been marked for deletion');
    END IF;
END $$;