-- Create dba.treportmanager table to store email report configurations
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating dba.treportmanager table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'treportmanager') THEN
        CREATE TABLE dba.treportmanager (
            reportID SERIAL PRIMARY KEY,
            reportname VARCHAR(255) NOT NULL,
            reportdescription TEXT,
            frequency VARCHAR(50) NOT NULL,
            Subjectheader VARCHAR(255) NOT NULL,
            toheader TEXT NOT NULL,
            hasattachment BOOLEAN DEFAULT FALSE,
            attachmentqueries JSONB,
            emailbodytemplate TEXT,
            emailbodyqueries JSONB,
            datastatusid INT REFERENCES dba.tdatastatus(datastatusid), -- Renamed to datastatusid
            createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            createduser VARCHAR(100)
        );

        -- Add comments for clarity
        COMMENT ON TABLE dba.treportmanager IS 'Stores configurations for automated email reports.';
        COMMENT ON COLUMN dba.treportmanager.reportID IS 'Primary key for the report configuration.';
        COMMENT ON COLUMN dba.treportmanager.reportname IS 'Name of the report.';
        COMMENT ON COLUMN dba.treportmanager.reportdescription IS 'Description of the report.';
        COMMENT ON COLUMN dba.treportmanager.frequency IS 'Cron expression defining the report schedule (e.g., "0 8 * * *").';
        COMMENT ON COLUMN dba.treportmanager.Subjectheader IS 'Email subject header.';
        COMMENT ON COLUMN dba.treportmanager.toheader IS 'Comma-separated list of recipient email addresses.';
        COMMENT ON COLUMN dba.treportmanager.hasattachment IS 'Flag indicating if the report includes attachments.';
        COMMENT ON COLUMN dba.treportmanager.attachmentqueries IS 'JSONB array of attachment queries (e.g., [{"name": "file.csv", "query": "SELECT * FROM table"}]).';
        COMMENT ON COLUMN dba.treportmanager.emailbodytemplate IS 'HTML template for the email body with placeholders (e.g., "Here is your report: {{grid1}}").';
        COMMENT ON COLUMN dba.treportmanager.emailbodyqueries IS 'JSONB mapping of placeholders to SQL queries (e.g., {"grid1": "SELECT * FROM table"}).';
        COMMENT ON COLUMN dba.treportmanager.datastatusid IS 'Foreign key to dba.tdatastatus, indicating report status (e.g., active, inactive).';
        COMMENT ON COLUMN dba.treportmanager.createddate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN dba.treportmanager.createduser IS 'User who created the record.';

        RAISE NOTICE 'Created dba.treportmanager table and comments';
    END IF;
END $OUTER$;

-- Grant permissions
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting permissions on dba.treportmanager';
    GRANT SELECT, INSERT, UPDATE ON dba.treportmanager TO etl_user;
    GRANT ALL ON dba.treportmanager TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.treportmanager_reportid_seq TO etl_user;
    RAISE NOTICE 'Permissions granted on dba.treportmanager';
END $OUTER$;


DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating function f_insert_treportmanager';
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'dba')
        AND proname = 'f_insert_treportmanager'
    ) THEN
        CREATE OR REPLACE FUNCTION dba.f_insert_treportmanager(
            p_reportname VARCHAR,
            p_reportdescription TEXT,
            p_frequency VARCHAR,
            p_subjectheader VARCHAR,
            p_toheader TEXT,
            p_hasattachment BOOLEAN,
            p_attachmentqueries JSONB,
            p_emailbodytemplate TEXT,
            p_emailbodyqueries JSONB,
            p_datastatus int,
            p_createduser VARCHAR
        ) RETURNS INT
        LANGUAGE plpgsql
        AS $INNER$
        DECLARE
            v_datastatusid INT;
            v_reportid INT;
        BEGIN
            -- Resolve datastatusid
            SELECT datastatusid INTO v_datastatusid 
            FROM dba.tdatastatus 
            WHERE datastatusid = p_datastatus;
            IF v_datastatusid IS NULL THEN
                RAISE EXCEPTION 'Data status % not found', p_datastatus;
            END IF;

            -- Insert new report configuration
            INSERT INTO dba.treportmanager (
                reportname,
                reportdescription,
                frequency,
                Subjectheader,
                toheader,
                hasattachment,
                attachmentqueries,
                emailbodytemplate,
                emailbodyqueries,
                datastatusid, -- Updated to datastatusid
                createddate,
                createduser
            ) VALUES (
                p_reportname,
                p_reportdescription,
                p_frequency,
                p_subjectheader,
                p_toheader,
                p_hasattachment,
                p_attachmentqueries,
                p_emailbodytemplate,
                p_emailbodyqueries,
                v_datastatusid,
                CURRENT_TIMESTAMP,
                p_createduser
            ) RETURNING reportID INTO v_reportid;

            RETURN v_reportid;
        END;
        $INNER$;

        COMMENT ON FUNCTION dba.f_insert_treportmanager IS 'Inserts a new report configuration into dba.treportmanager and returns the reportID.';
        RAISE NOTICE 'Created function f_insert_treportmanager';
    END IF;
END $OUTER$;

-- Grant permissions on the function
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting permissions on f_insert_treportmanager';
    GRANT EXECUTE ON FUNCTION dba.f_insert_treportmanager(VARCHAR, TEXT, VARCHAR, VARCHAR, TEXT, BOOLEAN, JSONB, TEXT, JSONB, int, VARCHAR) TO etl_user;
    RAISE NOTICE 'Permissions granted on f_insert_treportmanager';
END $OUTER$;


--update
-- Drop the function if it exists to avoid conflicts
DROP FUNCTION IF EXISTS dba.f_update_treportmanager(INT, VARCHAR, TEXT, VARCHAR, VARCHAR, TEXT, BOOLEAN, JSONB, TEXT, JSONB, INT);

-- Create or replace the function
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating function f_update_treportmanager';
    CREATE OR REPLACE FUNCTION dba.f_update_treportmanager(
        p_reportid INT,
        p_reportname VARCHAR,
        p_reportdescription TEXT,
        p_frequency VARCHAR,
        p_subjectheader VARCHAR,
        p_toheader TEXT,
        p_hasattachment BOOLEAN,
        p_attachmentqueries JSONB,
        p_emailbodytemplate TEXT,
        p_emailbodyqueries JSONB,
        p_datastatusid INT
    ) RETURNS VOID
    LANGUAGE plpgsql
    AS $INNER$
    BEGIN
        -- Validate datastatusid exists in dba.tdatastatus
        IF NOT EXISTS (
            SELECT 1
            FROM dba.tdatastatus
            WHERE datastatusid = p_datastatusid
        ) THEN
            RAISE EXCEPTION 'Data status ID % not found in dba.tdatastatus', p_datastatusid;
        END IF;

        -- Update the report configuration
        UPDATE dba.treportmanager
        SET reportname = p_reportname,
            reportdescription = p_reportdescription,
            frequency = p_frequency,
            Subjectheader = p_subjectheader,
            toheader = p_toheader,
            hasattachment = p_hasattachment,
            attachmentqueries = p_attachmentqueries,
            emailbodytemplate = p_emailbodytemplate,
            emailbodyqueries = p_emailbodyqueries,
            datastatusid = p_datastatusid
        WHERE reportID = p_reportid;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Report with ID % not found', p_reportid;
        END IF;
    END;
    $INNER$;

    COMMENT ON FUNCTION dba.f_update_treportmanager IS 'Updates an existing report configuration in dba.treportmanager.';
    RAISE NOTICE 'Created function f_update_treportmanager';
END $OUTER$;

-- Grant permissions on the function
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting permissions on f_update_treportmanager';
    GRANT EXECUTE ON FUNCTION dba.f_update_treportmanager(INT, VARCHAR, TEXT, VARCHAR, VARCHAR, TEXT, BOOLEAN, JSONB, TEXT, JSONB, INT) TO etl_user;
    RAISE NOTICE 'Permissions granted on f_update_treportmanager';
END $OUTER$;