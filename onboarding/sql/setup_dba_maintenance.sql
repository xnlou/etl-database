-- Create schema dba if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dba') THEN
        CREATE SCHEMA dba;
        COMMENT ON SCHEMA dba IS 'Schema for ETL pipeline maintenance and logging tables.';
    END IF;
END $$;

-- Create tDDLLogs table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = 'dba' AND tablename = 'tddllogs'
    ) THEN
        CREATE TABLE dba.tDDLLogs (
              logId SERIAL PRIMARY KEY
            , eventTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            , eventType TEXT NOT NULL
            , schemaName TEXT NULL
            , objectName TEXT NULL
            , objectType TEXT NOT NULL
            , sqlStatement TEXT NOT NULL
            , userName VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE dba.tDDLLogs IS 'Logs DDL changes in the database.';
        COMMENT ON COLUMN dba.tDDLLogs.logId IS 'Primary key for the log entry.';
        COMMENT ON COLUMN dba.tDDLLogs.eventTime IS 'Timestamp of the DDL event.';
        COMMENT ON COLUMN dba.tDDLLogs.eventType IS 'Type of DDL event (e.g., CREATE, ALTER, DROP).';
        COMMENT ON COLUMN dba.tDDLLogs.schemaName IS 'Schema of the affected object.';
        COMMENT ON COLUMN dba.tDDLLogs.objectName IS 'Identifier of the affected object.';
        COMMENT ON COLUMN dba.tDDLLogs.objectType IS 'Type of the affected object (e.g., TABLE, FUNCTION).';
        COMMENT ON COLUMN dba.tDDLLogs.sqlStatement IS 'SQL statement tag that triggered the event.';
        COMMENT ON COLUMN dba.tDDLLogs.userName IS 'User who performed the DDL operation.';

        GRANT ALL ON TABLE dba.tDDLLogs TO yostfundsadmin;
    END IF;
END $$;

-- Create tLogEntry table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = 'dba' AND tablename = 'tlogentry'
    ) THEN
        CREATE TABLE dba.tLogEntry (
              logId SERIAL PRIMARY KEY
            , timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            , run_uuid VARCHAR(36) NOT NULL
            , process_type VARCHAR(50) NOT NULL
            , stepcounter VARCHAR(50)
            , user_name VARCHAR(50)
            , step_runtime FLOAT
            , total_runtime FLOAT
            , message TEXT NOT NULL
        );

        COMMENT ON TABLE dba.tLogEntry IS 'Stores log entries for ETL processes.';
        COMMENT ON COLUMN dba.tLogEntry.logId IS 'Primary key for the log entry.';
        COMMENT ON COLUMN dba.tLogEntry.timestamp IS 'Timestamp of the log entry.';
        COMMENT ON COLUMN dba.tLogEntry.run_uuid IS 'Unique identifier for the ETL run.';
        COMMENT ON COLUMN dba.tLogEntry.process_type IS 'Type of process (e.g., EventProcessing, FinalSave).';
        COMMENT ON COLUMN dba.tLogEntry.stepcounter IS 'Step identifier within the process.';
        COMMENT ON COLUMN dba.tLogEntry.user_name IS 'User who executed the process.';
        COMMENT ON COLUMN dba.tLogEntry.step_runtime IS 'Runtime of the step in seconds.';
        COMMENT ON COLUMN dba.tLogEntry.total_runtime IS 'Total runtime of the script in seconds.';
        COMMENT ON COLUMN dba.tLogEntry.message IS 'Log message.';

        GRANT ALL ON TABLE dba.tLogEntry TO yostfundsadmin;

        CREATE INDEX idx_tlogentry_timestamp ON dba.tLogEntry (timestamp);
        CREATE INDEX idx_tlogentry_run_uuid ON dba.tLogEntry (run_uuid);
    END IF;
END $$;

-- Create or replace function to log DDL changes
CREATE OR REPLACE FUNCTION dba.fLogDDLChanges()
RETURNS EVENT_TRIGGER AS $$
DECLARE
    r RECORD;
    changeTime TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    FOR r IN (SELECT * FROM pg_event_trigger_ddl_commands())
    LOOP
        INSERT INTO dba.tDDLLogs (
              eventTime
            , eventType
            , schemaName
            , objectName
            , objectType
            , sqlStatement
        )
        VALUES (
              changeTime
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
        EXECUTE FUNCTION dba.fLogDDLChanges();
    END IF;
END $$;

-- Grant permissions to yostfundsadmin
DO $$
BEGIN
    EXECUTE 'GRANT ALL ON SCHEMA dba TO yostfundsadmin';
END $$;