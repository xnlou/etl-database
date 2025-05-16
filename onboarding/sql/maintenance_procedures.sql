-- maintenance_procedures.sql
-- Description: Defines maintenance procedures and tables for ETL pipeline

-- Create schema if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating dba schema';
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dba') THEN
        CREATE SCHEMA dba;
        RAISE NOTICE 'dba schema created';
    END IF;
END $OUTER$;

-- Create logging table if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating tMaintenanceLog table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tmaintenancelog') THEN
        CREATE TABLE dba.tMaintenanceLog (
            logId SERIAL PRIMARY KEY,
            maintenanceTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            operation VARCHAR(50) NOT NULL,
            tableName VARCHAR(100),
            userName VARCHAR(50),
            durationSeconds FLOAT,
            details TEXT
        );
        RAISE NOTICE 'tMaintenanceLog table created';
    END IF;
END $OUTER$;

-- Grant permissions
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting permissions on tMaintenanceLog';
    GRANT SELECT, INSERT ON dba.tMaintenanceLog TO etl_user;
    GRANT ALL ON dba.tMaintenanceLog TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tMaintenanceLog_logId_seq TO etl_user;
    RAISE NOTICE 'Permissions granted on tMaintenanceLog';
END $OUTER$;

-- Create index for faster queries
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating index idx_tMaintenanceLog_maintenanceTime';
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tmaintenancelog_maintenancetime') THEN
        CREATE INDEX idx_tMaintenanceLog_maintenanceTime ON dba.tMaintenanceLog(maintenanceTime);
        RAISE NOTICE 'Index idx_tMaintenanceLog_maintenanceTime created';
    END IF;
END $OUTER$;

-- Create procedure for VACUUM ANALYZE
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating pRunMaintenanceVacuumAnalyze procedure';
    CREATE OR REPLACE PROCEDURE dba.pRunMaintenanceVacuumAnalyze()
    LANGUAGE plpgsql
    AS $INNER$
    DECLARE
        startTime TIMESTAMP;
        endTime TIMESTAMP;
        tableRec RECORD;
    BEGIN
        startTime := CURRENT_TIMESTAMP;
        -- Run VACUUM ANALYZE on all tables
        FOR tableRec IN (
            SELECT format('%I.%I', nspname, relname) AS tableName
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ) LOOP
            EXECUTE 'VACUUM ANALYZE ' || quote_ident(tableRec.tableName);
        END LOOP;
        endTime := CURRENT_TIMESTAMP;

        -- Log the operation
        INSERT INTO dba.tMaintenanceLog (
            maintenanceTime,
            operation,
            tableName,
            userName,
            durationSeconds,
            details
        )
        VALUES (
            startTime,
            'VACUUM ANALYZE',
            NULL,
            CURRENT_USER,
            EXTRACT(EPOCH FROM (endTime - startTime)),
            'Database-wide VACUUM ANALYZE'
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO dba.tMaintenanceLog (
                maintenanceTime,
                operation,
                tableName,
                userName,
                durationSeconds,
                details
            )
            VALUES (
                startTime,
                'VACUUM ANALYZE',
                NULL,
                CURRENT_USER,
                NULL,
                'Error: ' || SQLERRM
            );
            COMMIT;
            RAISE NOTICE 'Maintenance failed: %', SQLERRM;
    END;
    $INNER$;
    RAISE NOTICE 'pRunMaintenanceVacuumAnalyze procedure created';
END $OUTER$;

-- Grant execute permission
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting execute permission on pRunMaintenanceVacuumAnalyze';
    GRANT EXECUTE ON PROCEDURE dba.pRunMaintenanceVacuumAnalyze() TO etl_user;
    RAISE NOTICE 'Execute permission granted on pRunMaintenanceVacuumAnalyze';
END $OUTER$;