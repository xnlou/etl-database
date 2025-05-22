-- dataset_setup.sql
-- Description: Sets up tables and functions in the dba schema to track dataset metadata for the ETL pipeline.
-- Naming Conventions: t for tables, f for functions, p for procedures, v for views.
-- Idempotency: Each object creation is guarded by an existence check to prevent errors on rerun.

-- Create dba schema if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 1: Creating dba schema';
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dba') THEN
        CREATE SCHEMA dba;
        RAISE NOTICE 'Line 4: dba schema created';
    END IF;
END $OUTER$;

-- Line 7: Create tDataSetType table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 7: Starting creation of tDataSetType table';
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
        RAISE NOTICE 'Line 21: tDataSetType table and comments created';
    END IF;
    RAISE NOTICE 'Line 23: Completed tDataSetType block';
END $OUTER$;

-- Grant permissions on tDataSetType
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 27: Granting permissions on tDataSetType';
    GRANT SELECT, INSERT ON dba.tDataSetType TO etl_user;
    GRANT ALL ON dba.tDataSetType TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tDataSetType_DataSetTypeID_seq TO etl_user;
    RAISE NOTICE 'Line 31: Permissions granted on tDataSetType';
END $OUTER$;

-- Line 34: Create tDataSource table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 34: Starting creation of tDataSource table';
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
        RAISE NOTICE 'Line 48: tDataSource table and comments created';
    END IF;
    RAISE NOTICE 'Line 50: Completed tDataSource block';
END $OUTER$;

-- Grant permissions on tDataSource
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 54: Granting permissions on tDataSource';
    GRANT SELECT, INSERT ON dba.tDataSource TO etl_user;
    GRANT ALL ON dba.tDataSource TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tDataSource_DataSourceID_seq TO etl_user;
    RAISE NOTICE 'Line 58: Permissions granted on tDataSource';
END $OUTER$;

-- Line 61: Create tDataStatus table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 61: Starting creation of tDataStatus table';
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
        RAISE NOTICE 'Line 75: tDataStatus table and comments created';
    END IF;
    RAISE NOTICE 'Line 77: Completed tDataStatus block';
END $OUTER$;

-- Grant permissions on tDataStatus
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 81: Granting permissions on tDataStatus';
    GRANT SELECT, INSERT ON dba.tDataStatus TO etl_user;
    GRANT ALL ON dba.tDataStatus TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tDataStatus_DataStatusID_seq TO etl_user;
    RAISE NOTICE 'Line 85: Permissions granted on tDataStatus';
END $OUTER$;

-- Line 88: Create tDataSet table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 88: Starting creation of tDataSet table';
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
        RAISE NOTICE 'Line 115: tDataSet table and comments created';
    END IF;
    RAISE NOTICE 'Line 117: Completed tDataSet block';
END $OUTER$;

-- Grant permissions on tDataSet
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 121: Granting permissions on tDataSet';
    GRANT SELECT, INSERT ON dba.tDataSet TO etl_user;
    GRANT ALL ON dba.tDataSet TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tDataSet_DataSetID_seq TO etl_user;
    RAISE NOTICE 'Line 125: Permissions granted on tDataSet';
END $OUTER$;

-- Line 128: Create indexes for tDataSet if they don't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 128: Starting creation of tDataSet indexes';
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tdataset_datasetdate') THEN
        CREATE INDEX idx_tdataset_datasetdate ON dba.tDataSet (DataSetDate);
        RAISE NOTICE 'Line 132: idx_tdataset_datasetdate index created';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tdataset_isactive') THEN
        CREATE INDEX idx_tdataset_isactive ON dba.tDataSet (IsActive);
        RAISE NOTICE 'Line 136: idx_tdataset_isactive index created';
    END IF;
    RAISE NOTICE 'Line 138: Completed tDataSet indexes block';
END $OUTER$;

-- Line 141: Create function fEnforceSingleActiveDataSet if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 141: Starting creation of fEnforceSingleActiveDataSet function';
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba')
        AND proname = 'fenforcesingleactivedataset'
    ) THEN
        CREATE FUNCTION dba.fEnforceSingleActiveDataSet()
        RETURNS TRIGGER AS $INNER$
        BEGIN
            RAISE NOTICE 'Line 147: Inside fEnforceSingleActiveDataSet function body';
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
                RAISE NOTICE 'Line 157: Completed UPDATE in fEnforceSingleActiveDataSet';
            END IF;
            RAISE NOTICE 'Line 159: Returning from fEnforceSingleActiveDataSet';
            RETURN NEW;
        END;
        $INNER$ LANGUAGE plpgsql;
        RAISE NOTICE 'Line 162: fEnforceSingleActiveDataSet function created';
    END IF;
    RAISE NOTICE 'Line 164: Completed fEnforceSingleActiveDataSet block';
END $OUTER$;

-- Grant permissions on fEnforceSingleActiveDataSet
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 168: Granting permissions on fEnforceSingleActiveDataSet';
    GRANT EXECUTE ON FUNCTION dba.fEnforceSingleActiveDataSet() TO etl_user;
    RAISE NOTICE 'Line 170: Permissions granted on fEnforceSingleActiveDataSet';
END $OUTER$;

-- Line 173: Create trigger tTriggerEnforceSingleActiveDataSet if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 173: Starting creation of tTriggerEnforceSingleActiveDataSet trigger';
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ttriggerenforcesingleactivedataset' AND tgrelid = (SELECT oid FROM pg_class WHERE relname = 'tdataset' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba'))) THEN
        CREATE TRIGGER tTriggerEnforceSingleActiveDataSet
        AFTER INSERT OR UPDATE OF IsActive
        ON dba.tDataSet
        FOR EACH ROW
        WHEN (NEW.IsActive = TRUE)
        EXECUTE FUNCTION dba.fEnforceSingleActiveDataSet();
        RAISE NOTICE 'Line 181: tTriggerEnforceSingleActiveDataSet trigger created';
    END IF;
    RAISE NOTICE 'Line 183: Completed tTriggerEnforceSingleActiveDataSet block';
END $OUTER$;

-- Line 186: Create function f_dataset_iu if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 186: Starting creation of f_dataset_iu function';
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba')
        AND proname = 'f_dataset_iu'
    ) THEN
        CREATE OR REPLACE FUNCTION dba.f_dataset_iu(
            p_datasetid INT,
            p_datasetdate DATE,
            p_datasettype VARCHAR,
            p_datasource VARCHAR,
            p_label VARCHAR,
            p_statusname VARCHAR,
            p_createduser VARCHAR
        ) RETURNS INT
        LANGUAGE plpgsql
        AS $INNER$
        DECLARE
            v_datasettypeid INT;
            v_datasourceid INT;
            v_datastatusid INT;
            v_efffromdate TIMESTAMP;
            v_effthrudate TIMESTAMP;
            v_datasetid INT;
        BEGIN
            -- Set effective dates
            v_efffromdate := CURRENT_TIMESTAMP;
            v_effthrudate := '9999-01-01';

            -- Resolve datasettypeid
            SELECT datasettypeid INTO v_datasettypeid 
            FROM dba.tdatasettype 
            WHERE typename = p_datasettype;
            IF v_datasettypeid IS NULL THEN
                RAISE EXCEPTION 'Dataset type % not found', p_datasettype;
            END IF;

            -- Resolve datasourceid
            SELECT datasourceid INTO v_datasourceid 
            FROM dba.tdatasource 
            WHERE sourcename = p_datasource;
            IF v_datasourceid IS NULL THEN
                RAISE EXCEPTION 'Data source % not found', p_datasource;
            END IF;

            -- Resolve datastatusid
            SELECT datastatusid INTO v_datastatusid 
            FROM dba.tdatastatus 
            WHERE statusname = p_statusname;
            IF v_datastatusid IS NULL THEN
                RAISE EXCEPTION 'Data status % not found', p_statusname;
            END IF;

            IF p_datasetid IS NULL THEN
                -- Insert new dataset with 'New' status
                INSERT INTO dba.tdataset (
                    datasetdate,
                    label,
                    datasettypeid,
                    datasourceid,
                    datastatusid,
                    isactive,
                    createddate,
                    createdby,
                    efffromdate,
                    effthrudate
                ) VALUES (
                    p_datasetdate,
                    p_label,
                    v_datasettypeid,
                    v_datasourceid,
                    v_datastatusid,
                    FALSE,  -- New datasets are not active by default
                    CURRENT_TIMESTAMP,
                    p_createduser,
                    v_efffromdate,
                    v_effthrudate
                ) RETURNING datasetid INTO v_datasetid;
            ELSE
                -- Update existing dataset
                UPDATE dba.tdataset
                SET datastatusid = v_datastatusid,
                    isactive = CASE WHEN p_statusname = 'Active' THEN TRUE ELSE isactive END
                WHERE datasetid = p_datasetid;

                IF p_statusname = 'Active' THEN
                    -- Deactivate other datasets with the same label, type, and date
                    UPDATE dba.tdataset
                    SET isactive = FALSE,
                        effthrudate = CURRENT_TIMESTAMP,
                        datastatusid = (SELECT datastatusid FROM dba.tdatastatus WHERE statusname = 'Inactive')
                    WHERE label = p_label
                      AND datasettypeid = v_datasettypeid
                      AND datasetdate = p_datasetdate
                      AND datasetid != p_datasetid
                      AND isactive = TRUE;
                END IF;
                v_datasetid := p_datasetid;
            END IF;

            RETURN v_datasetid;
        END;
        $INNER$;
        COMMENT ON FUNCTION dba.f_dataset_iu IS 'Inserts or updates a dataset in tDataSet, resolving type, source, and status IDs, and managing active status.';
        RAISE NOTICE 'Line 260: f_dataset_iu function created';
    END IF;
    RAISE NOTICE 'Line 262: Completed f_dataset_iu block';
END $OUTER$;

-- Grant permissions on f_dataset_iu
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 266: Granting permissions on f_dataset_iu';
    GRANT EXECUTE ON FUNCTION dba.f_dataset_iu(INT, DATE, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO etl_user;
    RAISE NOTICE 'Line 268: Permissions granted on f_dataset_iu';
END $OUTER$;

-- Line 271: Insert data into tDataStatus if the table is empty
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 271: Starting insert into tDataStatus';
    IF (SELECT COUNT(*) FROM dba.tDataStatus) = 0 THEN
        INSERT INTO dba.tDataStatus (StatusName, Description) VALUES
            ('Active', 'Dataset is currently active and in use'),
            ('Inactive', 'Dataset is no longer active but retained for history'),
            ('Deleted', 'Dataset has been marked for deletion'),
            ('New', 'Default status of every new dataset'),
            ('Failed', 'Status if something goes wrong')
            ;
        RAISE NOTICE 'Line 276: Inserted data into tDataStatus';
    END IF;
    RAISE NOTICE 'Line 278: Completed tDataStatus insert block';
END $OUTER$;