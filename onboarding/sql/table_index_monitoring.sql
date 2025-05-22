-- table_index_monitoring.sql
-- Description: Defines table and procedure to monitor table and index usage for ETL pipeline

-- Ensure dba schema exists
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating dba schema';
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dba') THEN
        CREATE SCHEMA dba;
        RAISE NOTICE 'dba schema created';
    END IF;
END $OUTER$;

-- Create table to log table and index usage
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating ttableindexstats table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'ttableindexstats') THEN
        CREATE TABLE dba.ttableindexstats (
            statid SERIAL PRIMARY KEY,
            snapshottime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            schemaname VARCHAR(100),
            tablename VARCHAR(100),
            indexname VARCHAR(100),
            rowcount BIGINT,
            seqscans BIGINT,
            idxscans BIGINT,
            inserts BIGINT,
            updates BIGINT,
            deletes BIGINT,
            totalsize BIGINT
        );
        RAISE NOTICE 'ttableindexstats table created';
    END IF;
END $OUTER$;

-- Grant permissions
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting permissions on ttableindexstats';
    GRANT SELECT, INSERT ON dba.ttableindexstats TO etl_user;
    GRANT ALL ON dba.ttableindexstats TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.ttableindexstats_statid_seq TO etl_user;
    RAISE NOTICE 'Permissions granted on ttableindexstats';
END $OUTER$;

-- Create index for faster queries
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating index idx_ttableindexstats_snapshottime';
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_ttableindexstats_snapshottime') THEN
        CREATE INDEX idx_ttableindexstats_snapshottime ON dba.ttableindexstats(snapshottime);
        RAISE NOTICE 'Index idx_ttableindexstats_snapshottime created';
    END IF;
END $OUTER$;

-- Create procedure to capture table and index stats
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating pcapturetableindexstats procedure';
    CREATE OR REPLACE PROCEDURE dba.pcapturetableindexstats()
    LANGUAGE plpgsql
    AS $INNER$
    BEGIN
        -- Capture table stats
        INSERT INTO dba.ttableindexstats (
            snapshottime,
            schemaname,
            tablename,
            indexname,
            rowcount,
            seqscans,
            idxscans,
            inserts,
            updates,
            deletes,
            totalsize
        )
        SELECT 
            CURRENT_TIMESTAMP,
            schemaname,
            relname,
            NULL AS indexname,
            n_live_tup,
            seq_scan,
            idx_scan,
            n_tup_ins,
            n_tup_upd,
            n_tup_del,
            pg_total_relation_size(relid)
        FROM pg_stat_user_tables;
        
        -- Capture index stats
        INSERT INTO dba.ttableindexstats (
            snapshottime,
            schemaname,
            tablename,
            indexname,
            rowcount,
            seqscans,
            idxscans,
            inserts,
            updates,
            deletes,
            totalsize
        )
        SELECT 
            CURRENT_TIMESTAMP,
            s.schemaname,
            s.relname AS tablename,
            s.indexrelname AS indexname,
            NULL AS rowcount,
            NULL AS seqscans,
            s.idx_scan AS idxscans,
            NULL AS inserts,
            NULL AS updates,
            NULL AS deletes,
            pg_total_relation_size(s.indexrelid) AS totalsize
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        WHERE NOT i.indisprimary;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO dba.tlogentry (
                runuuid,
                timestamp,
                processtype,
                stepcounter,
                username,
                stepruntime,
                totalruntime,
                message
            )
            VALUES (
                gen_random_uuid(),
                CURRENT_TIMESTAMP,
                'StatsCapture',
                'error',
                CURRENT_USER,
                NULL,
                NULL,
                'Error capturing table/index stats: ' || SQLERRM
            );
            COMMIT;
            RAISE NOTICE 'Stats capture failed: %', SQLERRM;
    END;
    $INNER$;
    RAISE NOTICE 'pcapturetableindexstats procedure created';
END $OUTER$;

-- Grant execute permission
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting execute permission on pcapturetableindexstats';
    GRANT EXECUTE ON PROCEDURE dba.pcapturetableindexstats() TO etl_user;
    RAISE NOTICE 'Execute permission granted on pcapturetableindexstats';
END $OUTER$;