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
    RAISE NOTICE 'Creating tmaintenancelog table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'tmaintenancelog') THEN
        CREATE TABLE dba.tmaintenancelog (
            logid SERIAL PRIMARY KEY,
            maintenancetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            operation VARCHAR(50) NOT NULL,
            tablename VARCHAR(100),
            username VARCHAR(50),
            durationseconds FLOAT,
            details TEXT
        );
        RAISE NOTICE 'tmaintenancelog table created';
    END IF;
END $OUTER$;

-- Grant permissions
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting permissions on tmaintenancelog';
    GRANT SELECT, INSERT ON dba.tmaintenancelog TO etl_user;
    GRANT ALL ON dba.tmaintenancelog TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tmaintenancelog_logid_seq TO etl_user;
    RAISE NOTICE 'Permissions granted on tmaintenancelog';
END $OUTER$;

-- Create index for faster queries
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating index idx_tmaintenancelog_maintenancetime';
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_tmaintenancelog_maintenancetime') THEN
        CREATE INDEX idx_tmaintenancelog_maintenancetime ON dba.tmaintenancelog(maintenancetime);
        RAISE NOTICE 'Index idx_tmaintenancelog_maintenancetime created';
    END IF;
END $OUTER$;

-- Create procedure for VACUUM ANALYZE
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating prunmaintenancevacuumanalyze procedure';
    CREATE OR REPLACE PROCEDURE dba.prunmaintenancevacuumanalyze()
    LANGUAGE plpgsql
    AS $INNER$
    DECLARE
        starttime TIMESTAMP;
        endtime TIMESTAMP;
        tablerec RECORD;
    BEGIN
        starttime := CURRENT_TIMESTAMP;
        -- Run VACUUM ANALYZE on all tables
        FOR tablerec IN (
            SELECT format('%I.%I', nspname, relname) AS tablename
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ) LOOP
            EXECUTE 'VACUUM ANALYZE ' || quote_ident(tablerec.tablename);
        END LOOP;
        endtime := CURRENT_TIMESTAMP;

        -- Log the operation
        INSERT INTO dba.tmaintenancelog (
            maintenancetime,
            operation,
            tablename,
            username,
            durationseconds,
            details
        )
        VALUES (
            starttime,
            'VACUUM ANALYZE',
            NULL,
            CURRENT_USER,
            EXTRACT(EPOCH FROM (endtime - starttime)),
            'Database-wide VACUUM ANALYZE'
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO dba.tmaintenancelog (
                maintenancetime,
                operation,
                tablename,
                username,
                durationseconds,
                details
            )
            VALUES (
                starttime,
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
    RAISE NOTICE 'prunmaintenancevacuumanalyze procedure created';
END $OUTER$;

-- Grant execute permission
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting execute permission on prunmaintenancevacuumanalyze';
    GRANT EXECUTE ON PROCEDURE dba.prunmaintenancevacuumanalyze() TO etl_user;
    RAISE NOTICE 'Execute permission granted on prunmaintenancevacuumanalyze';
END $OUTER$;