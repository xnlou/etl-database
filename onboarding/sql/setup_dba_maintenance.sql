-- Create schema dba if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dba') THEN
        CREATE SCHEMA dba;
        COMMENT ON SCHEMA dba IS 'Schema for ETL pipeline maintenance and logging tables.';
    END IF;
END $$;

-- Create tddllogs table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = 'dba' AND tablename = 'tddllogs'
    ) THEN
        CREATE TABLE dba.tddllogs (
              logid SERIAL PRIMARY KEY
            , eventtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            , eventtype TEXT NOT NULL
            , schemaname TEXT NULL
            , objectname TEXT NULL
            , objecttype TEXT NOT NULL
            , sqlstatement TEXT NOT NULL
            , username VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tddllogs IS 'Logs DDL changes in the database.';
        COMMENT ON COLUMN dba.tddllogs.logid IS 'Primary key for the log entry.';
        COMMENT ON COLUMN dba.tddllogs.eventtime IS 'Timestamp of the DDL event.';
        COMMENT ON COLUMN dba.tddllogs.eventtype IS 'Type of DDL event (e.g., CREATE, ALTER, DROP).';
        COMMENT ON COLUMN dba.tddllogs.schemaname IS 'Schema of the affected object.';
        COMMENT ON COLUMN dba.tddllogs.objectname IS 'Identifier of the affected object.';
        COMMENT ON COLUMN dba.tddllogs.objecttype IS 'Type of the affected object (e.g., TABLE, FUNCTION).';
        COMMENT ON COLUMN dba.tddllogs.sqlstatement IS 'SQL statement tag that triggered the event.';
        COMMENT ON COLUMN dba.tddllogs.username IS 'User who performed the DDL operation.';

        GRANT ALL ON TABLE dba.tddllogs TO yostfundsadmin;
    END IF;
END $$;

-- Create tlogentry table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = 'dba' AND tablename = 'tlogentry'
    ) THEN
        CREATE TABLE dba.tlogentry (
              logid SERIAL PRIMARY KEY
            , timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            , run_uuid VARCHAR(36) NOT NULL
            , processtype VARCHAR(50) NOT NULL
            , stepcounter VARCHAR(50)
            , username VARCHAR(50)
            , stepruntime FLOAT
            , totalruntime FLOAT
            , message TEXT NOT NULL
        );

        COMMENT ON TABLE dba.tlogentry IS 'Stores log entries for ETL processes.';
        COMMENT ON COLUMN dba.tlogentry.logid IS 'Primary key for the log entry.';
        COMMENT ON COLUMN dba.tlogentry.timestamp IS 'Timestamp of the log entry.';
        COMMENT ON COLUMN dba.tlogentry.run_uuid IS 'Unique identifier for the ETL run.';
        COMMENT ON COLUMN dba.tlogentry.processtype IS 'Type of process (e.g., EventProcessing, FinalSave).';
        COMMENT ON COLUMN dba.tlogentry.stepcounter IS 'Step identifier within the process.';
        COMMENT ON COLUMN dba.tlogentry.username IS 'User who executed the process.';
        COMMENT ON COLUMN dba.tlogentry.stepruntime IS 'Runtime of the step in seconds.';
        COMMENT ON COLUMN dba.tlogentry.totalruntime IS 'Total runtime of the script in seconds.';
        COMMENT ON COLUMN dba.tlogentry.message IS 'Log message.';

        GRANT ALL ON TABLE dba.tlogentry TO yostfundsadmin;

        CREATE INDEX idx_tlogentry_timestamp ON dba.tlogentry (timestamp);
        CREATE INDEX idx_tlogentry_run_uuid ON dba.tlogentry (run_uuid);
    END IF;
END $$;

-- Create or replace function to log DDL changes
CREATE OR REPLACE FUNCTION dba.flogddlchanges()
RETURNS EVENT_TRIGGER AS $$
DECLARE
    r RECORD;
    changetime TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    FOR r IN (SELECT * FROM pg_event_trigger_ddl_commands())
    LOOP
        INSERT INTO dba.tddllogs (
              eventtime
            , eventtype
            , schemaname
            , objectname
            , objecttype
            , sqlstatement
        )
        VALUES (
              changetime
            , r.command_tag
            , COALESCE(r.schema_name, 'dba') -- Handle NULL schema_name
            , r.object_identity
            , r.object_type
            , r.command_tag
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create event trigger if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_event_trigger
        WHERE evtname = 'logddl'
    ) THEN
        CREATE EVENT TRIGGER logddl
        ON ddl_command_end
        EXECUTE FUNCTION dba.flogddlchanges();
    END IF;
END $$;

-- Grant permissions to yostfundsadmin
DO $$
BEGIN
    EXECUTE 'GRANT ALL ON SCHEMA dba TO yostfundsadmin';
END $$;