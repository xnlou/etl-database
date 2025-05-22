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

-- Line 7: Create tdatasettype table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 7: Starting creation of tdatasettype table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdatasettype') THEN
        CREATE TABLE dba.tdatasettype (
            datasettypeid SERIAL PRIMARY KEY,
            typename VARCHAR(50) NOT NULL UNIQUE,
            description TEXT,
            createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            createdby VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tdatasettype IS 'Stores dataset type definitions (e.g., MeetMax, ClientUpload).';
        COMMENT ON COLUMN dba.tdatasettype.datasettypeid IS 'Primary key for dataset type.';
        COMMENT ON COLUMN dba.tdatasettype.typename IS 'Unique name of the dataset type.';
        COMMENT ON COLUMN dba.tdatasettype.description IS 'Optional description of the dataset type.';
        COMMENT ON COLUMN dba.tdatasettype.createddate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tdatasettype.createdby IS 'User who created the record.';
        RAISE NOTICE 'Line 21: tdatasettype table and comments created';
    END IF;
    RAISE NOTICE 'Line 23: Completed tdatasettype block';
END $OUTER$;

-- Grant permissions on tdatasettype
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 27: Granting permissions on tdatasettype';
    GRANT SELECT, INSERT ON dba.tdatasettype TO etl_user;
    GRANT ALL ON dba.tdatasettype TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tdatasettype_datasettypeid_seq TO etl_user;
    RAISE NOTICE 'Line 31: Permissions granted on tdatasettype';
END $OUTER$;

-- Line 34: Create tdatasource table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 34: Starting creation of tdatasource table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdatasource') THEN
        CREATE TABLE dba.tdatasource (
            datasourceid SERIAL PRIMARY KEY,
            sourcename VARCHAR(50) NOT NULL UNIQUE,
            description TEXT,
            createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            createdby VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tdatasource IS 'Stores data source definitions (e.g., MeetMax Website, SFTP Upload).';
        COMMENT ON COLUMN dba.tdatasource.datasourceid IS 'Primary key for data source.';
        COMMENT ON COLUMN dba.tdatasource.sourcename IS 'Unique name of the data source.';
        COMMENT ON COLUMN dba.tdatasource.description IS 'Optional description of the data source.';
        COMMENT ON COLUMN dba.tdatasource.createddate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tdatasource.createdby IS 'User who created the record.';
        RAISE NOTICE 'Line 48: tdatasource table and comments created';
    END IF;
    RAISE NOTICE 'Line 50: Completed tdatasource block';
END $OUTER$;

-- Grant permissions on tdatasource
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 54: Granting permissions on tdatasource';
    GRANT SELECT, INSERT ON dba.tdatasource TO etl_user;
    GRANT ALL ON dba.tdatasource TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tdatasource_datasourceid_seq TO etl_user;
    RAISE NOTICE 'Line 58: Permissions granted on tdatasource';
END $OUTER$;

-- Line 61: Create tdatastatus table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 61: Starting creation of tdatastatus table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdatastatus') THEN
        CREATE TABLE dba.tdatastatus (
            datastatusid SERIAL PRIMARY KEY,
            statusname VARCHAR(50) NOT NULL UNIQUE,
            description TEXT,
            createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            createdby VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tdatastatus IS 'Stores status codes for datasets (e.g., Active, Inactive, Deleted).';
        COMMENT ON COLUMN dba.tdatastatus.datastatusid IS 'Primary key for status.';
        COMMENT ON COLUMN dba.tdatastatus.statusname IS 'Unique name of the status.';
        COMMENT ON COLUMN dba.tdatastatus.description IS 'Optional description of the status.';
        COMMENT ON COLUMN dba.tdatastatus.createddate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tdatastatus.createdby IS 'User who created the record.';
        RAISE NOTICE 'Line 75: tdatastatus table and comments created';
    END IF;
    RAISE NOTICE 'Line 77: Completed tdatastatus block';
END $OUTER$;

-- Grant permissions on tdatastatus
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 81: Granting permissions on tdatastatus';
    GRANT SELECT, INSERT ON dba.tdatastatus TO etl_user;
    GRANT ALL ON dba.tdatastatus TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tdatastatus_datastatusid_seq TO etl_user;
    RAISE NOTICE 'Line 85: Permissions granted on tdatastatus';
END $OUTER$;

-- Line 88: Create tdataset table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 88: Starting creation of tdataset table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tdataset') THEN
        CREATE TABLE dba.tdataset (
            datasetid SERIAL PRIMARY KEY,
            datasetdate DATE NOT NULL,
            label VARCHAR(100) NOT NULL,
            datasettypeid INTEGER NOT NULL,
            datasourceid INTEGER NOT NULL,
            datastatusid INTEGER NOT NULL,
            efffromdate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            effthrudate TIMESTAMP NOT NULL DEFAULT '9999-01-01',
            isactive BOOLEAN NOT NULL DEFAULT TRUE,
            createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            createdby VARCHAR(50) NOT NULL DEFAULT CURRENT_USER,
            CONSTRAINT fk_dataset_type FOREIGN KEY (datasettypeid) REFERENCES dba.tdatasettype (datasettypeid),
            CONSTRAINT fk_dataset_source FOREIGN KEY (datasourceid) REFERENCES dba.tdatasource (datasourceid),
            CONSTRAINT fk_dataset_status FOREIGN KEY (datastatusid) REFERENCES dba.tdatastatus (datastatusid),
            CONSTRAINT chk_eff_dates CHECK (efffromdate <= effthrudate)
        );

        COMMENT ON TABLE dba.tdataset IS 'Tracks metadata for dataset loads in the ETL pipeline.';
        COMMENT ON COLUMN dba.tdataset.datasetid IS 'Primary key for the dataset.';
        COMMENT ON COLUMN dba.tdataset.datasetdate IS 'Date associated with the dataset (e.g., data reference date).';
        COMMENT ON COLUMN dba.tdataset.label IS 'Descriptive label for the dataset.';
        COMMENT ON COLUMN dba.tdataset.datasettypeid IS 'Foreign key to tdatasettype, indicating dataset type.';
        COMMENT ON COLUMN dba.tdataset.datasourceid IS 'Foreign key to tdatasource, indicating data source.';
        COMMENT ON COLUMN dba.tdataset.datastatusid IS 'Foreign key to tdatastatus, indicating dataset status.';
        COMMENT ON COLUMN dba.tdataset.efffromdate IS 'Effective start date, defaults to creation time.';
        COMMENT ON COLUMN dba.tdataset.effthrudate IS 'Effective end date, defaults to 9999-01-01 for active records.';
        COMMENT ON COLUMN dba.tdataset.isactive IS 'Indicates if the dataset is active (TRUE) or inactive (FALSE).';
        COMMENT ON COLUMN dba.tdataset.createddate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.tdataset.createdby IS 'User who created the record.';
        RAISE NOTICE 'Line 115: tdataset table and comments created';
    END IF;
    RAISE NOTICE 'Line 117: Completed tdataset block';
END $OUTER$;

-- Grant permissions on tdataset
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 121: Granting permissions on tdataset';
    GRANT SELECT, INSERT ON dba.tdataset TO etl_user;
    GRANT ALL ON dba.tdataset TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tdataset_datasetid_seq TO etl_user;
    RAISE NOTICE 'Line 125: Permissions granted on tdataset';
END $OUTER$;

-- Line 128: Create indexes for tdataset if they don't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 128: Starting creation of tdataset indexes';
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tdataset_datasetdate') THEN
        CREATE INDEX idx_tdataset_datasetdate ON dba.tdataset (datasetdate);
        RAISE NOTICE 'Line 132: idx_tdataset_datasetdate index created';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tdataset_isactive') THEN
        CREATE INDEX idx_tdataset_isactive ON dba.tdataset (isactive);
        RAISE NOTICE 'Line 136: idx_tdataset_isactive index created';
    END IF;
    RAISE NOTICE 'Line 138: Completed tdataset indexes block';
END $OUTER$;

-- Line 141: Create function fenforcesingleactivedataset if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 141: Starting creation of fenforcesingleactivedataset function';
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba')
        AND proname = 'fenforcesingleactivedataset'
    ) THEN
        CREATE FUNCTION dba.fenforcesingleactivedataset()
        RETURNS TRIGGER AS $INNER$
        BEGIN
            RAISE NOTICE 'Line 147: Inside fenforcesingleactivedataset function body';
            IF NEW.isactive = TRUE THEN
                UPDATE dba.tdataset
                SET isactive = FALSE,
                    effthrudate = CURRENT_TIMESTAMP,
                    datastatusid = (SELECT datastatusid FROM dba.tdatastatus WHERE statusname = 'Inactive')
                WHERE label = NEW.label
                  AND datasettypeid = NEW.datasettypeid
                  AND datasetdate = NEW.datasetdate
                  AND datasetid != NEW.datasetid
                  AND isactive = TRUE;
                RAISE NOTICE 'Line 157: Completed UPDATE in fenforcesingleactivedataset';
            END IF;
            RAISE NOTICE 'Line 159: Returning from fenforcesingleactivedataset';
            RETURN NEW;
        END;
        $INNER$ LANGUAGE plpgsql;
        RAISE NOTICE 'Line 162: fenforcesingleactivedataset function created';
    END IF;
    RAISE NOTICE 'Line 164: Completed fenforcesingleactivedataset block';
END $OUTER$;

-- Grant permissions on fenforcesingleactivedataset
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 168: Granting permissions on fenforcesingleactivedataset';
    GRANT EXECUTE ON FUNCTION dba.fenforcesingleactivedataset() TO etl_user;
    RAISE NOTICE 'Line 170: Permissions granted on fenforcesingleactivedataset';
END $OUTER$;

-- Line 173: Create trigger ttriggerenforcesingleactivedataset if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 173: Starting creation of ttriggerenforcesingleactivedataset trigger';
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ttriggerenforcesingleactivedataset' AND tgrelid = (SELECT oid FROM pg_class WHERE relname = 'tdataset' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba'))) THEN
        CREATE TRIGGER ttriggerenforcesingleactivedataset
        AFTER INSERT OR UPDATE OF isactive
        ON dba.tdataset
        FOR EACH ROW
        WHEN (NEW.isactive = TRUE)
        EXECUTE FUNCTION dba.fenforcesingleactivedataset();
        RAISE NOTICE 'Line 181: ttriggerenforcesingleactivedataset trigger created';
    END IF;
    RAISE NOTICE 'Line 183: Completed ttriggerenforcesingleactivedataset block';
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
        COMMENT ON FUNCTION dba.f_dataset_iu IS 'Inserts or updates a dataset in tdataset, resolving type, source, and status IDs, and managing active status.';
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

-- Line 271: Insert data into tdatastatus if the table is empty
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 271: Starting insert into tdatastatus';
    IF (SELECT COUNT(*) FROM dba.tdatastatus) = 0 THEN
        INSERT INTO dba.tdatastatus (statusname, description) VALUES
            ('Active', 'Dataset is currently active and in use'),
            ('Inactive', 'Dataset is no longer active but retained for history'),
            ('Deleted', 'Dataset has been marked for deletion'),
            ('New', 'Default status of every new dataset'),
            ('Failed', 'Status if something goes wrong'),
            ('Empty', 'Dataset has no data'),
            ;
        RAISE NOTICE 'Line 276: Inserted data into tdatastatus';
    END IF;
    RAISE NOTICE 'Line 278: Completed tdatastatus insert block';
END $OUTER$;