-- Create the Feeds database
CREATE DATABASE Feeds
    WITH 
    OWNER = yostfundsadmin
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

-- Connect to the Feeds database
\c Feeds

-- Create the dba schema
CREATE SCHEMA IF NOT EXISTS dba
    AUTHORIZATION yostfundsadmin;

-- Grant permissions to etl_user
GRANT USAGE ON SCHEMA dba TO etl_user;
GRANT ALL ON SCHEMA dba TO yostfundsadmin;

-- Create tDdlLogs table to track DDL changes
CREATE TABLE dba.tDdlLogs (
    ,logId SERIAL PRIMARY KEY
    ,changeTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,objectType VARCHAR(50)
    ,objectName VARCHAR(100)
    ,operation VARCHAR(50)
    ,userName VARCHAR(50)
    ,queryText TEXT
    ,schemaName VARCHAR(100)
);

-- Grant permissions on tDdlLogs
GRANT SELECT, INSERT ON dba.tDdlLogs TO etl_user;
GRANT ALL ON dba.tDdlLogs TO yostfundsadmin;
GRANT USAGE, SELECT ON SEQUENCE dba.tDdlLogs_logId_seq TO etl_user;

-- Create function to log DDL changes
CREATE OR REPLACE FUNCTION dba.fLogDdlChanges()
RETURNS EVENT_TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT * FROM pg_event_trigger_ddl_commands())
    LOOP
        INSERT INTO dba.tDdlLogs (
            ,changeTime
            ,objectType
            ,objectName
            ,operation
            ,userName
            ,queryText
            ,schemaName
        )
        VALUES (
            ,CURRENT_TIMESTAMP
            ,r.object_type
            ,r.object_identity
            ,tg_tag
            ,CURRENT_USER
            ,current_query()
            ,r.schema_name
        );
    END LOOP;
END;
$$;

-- Grant execute permission on the function
GRANT EXECUTE ON FUNCTION dba.fLogDdlChanges() TO etl_user;

-- Create event trigger for DDL operations
CREATE EVENT TRIGGER logDdl
ON ddl_command_end
EXECUTE FUNCTION dba.fLogDdlChanges();

-- Create tLogEntry table to capture process logs
CREATE TABLE dba.tLogEntry (
    ,logId SERIAL PRIMARY KEY
    ,runUuid UUID NOT NULL
    ,timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,processType VARCHAR(50)
    ,stepcounter VARCHAR(50)
    ,userName VARCHAR(50)
    ,stepRuntime FLOAT
    ,totalRuntime FLOAT
    ,message TEXT
);

-- Grant permissions on tLogEntry
GRANT SELECT, INSERT ON dba.tLogEntry TO etl_user;
GRANT ALL ON dba.tLogEntry TO yostfundsadmin;
GRANT USAGE, SELECT ON SEQUENCE dba.tLogEntry_logId_seq TO etl_user;

-- Create indexes for faster queries
CREATE INDEX idx_tLogEntry_runUuid ON dba.tLogEntry(runUuid);
CREATE INDEX idx_tLogEntry_timestamp ON dba.tLogEntry(timestamp);

-- Grant CONNECT privilege on database
GRANT CONNECT ON DATABASE Feeds TO etl_user;