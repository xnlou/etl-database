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
    RAISE NOTICE 'Creating tTableIndexStats table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dba' AND tablename = 'ttableindexstats') THEN
        CREATE TABLE dba.tTableIndexStats (
            statId SERIAL PRIMARY KEY,
            snapshotTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            schemaName VARCHAR(100),
            tableName VARCHAR(100),
            indexName VARCHAR(100),
            rowCount BIGINT,
            seqScans BIGINT,
            idxScans BIGINT,
            inserts BIGINT,
            updates BIGINT,
            deletes BIGINT,
            totalSize BIGINT
        );
        RAISE NOTICE 'tTableIndexStats table created';
    END IF;
END $OUTER$;

-- Grant permissions
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting permissions on tTableIndexStats';
    GRANT SELECT, INSERT ON dba.tTableIndexStats TO etl_user;
    GRANT ALL ON dba.tTableIndexStats TO yostfundsadmin;
    GRANT USAGE, SELECT ON SEQUENCE dba.tTableIndexStats_statId_seq TO etl_user;
    RAISE NOTICE 'Permissions granted on tTableIndexStats';
END $OUTER$;

-- Create index for faster queries
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating index idx_tTableIndexStats_snapshotTime';
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dba' AND indexname = 'idx_ttableindexstats_snapshottime') THEN
        CREATE INDEX idx_tTableIndexStats_snapshotTime ON dba.tTableIndexStats(snapshotTime);
        RAISE NOTICE 'Index idx_tTableIndexStats_snapshotTime created';
    END IF;
END $OUTER$;

-- Create procedure to capture table and index stats
DO $OUTER$
BEGIN
    RAISE NOTICE 'Creating pCaptureTableIndexStats procedure';
    CREATE OR REPLACE PROCEDURE dba.pCaptureTableIndexStats()
    LANGUAGE plpgsql
    AS $INNER$
    BEGIN
        -- Capture table stats
        INSERT INTO dba.tTableIndexStats (
            snapshotTime,
            schemaName,
            tableName,
            indexName,
            rowCount,
            seqScans,
            idxScans,
            inserts,
            updates,
            deletes,
            totalSize
        )
        SELECT 
            CURRENT_TIMESTAMP,
            schemaname,
            relname,
            NULL AS indexName,
            n_live_tup,
            seq_scan,
            idx_scan,
            n_tup_ins,
            n_tup_upd,
            n_tup_del,
            pg_total_relation_size(relid)
        FROM pg_stat_user_tables;
        
        -- Capture index stats
        INSERT INTO dba.tTableIndexStats (
            snapshotTime,
            schemaName,
            tableName,
            indexName,
            rowCount,
            seqScans,
            idxScans,
            inserts,
            updates,
            deletes,
            totalSize
        )
        SELECT 
            CURRENT_TIMESTAMP,
            s.schemaname,
            s.relname AS tableName,
            s.indexrelname AS indexName,
            NULL AS rowCount,
            NULL AS seqScans,
            s.idx_scan AS idxScans,
            NULL AS inserts,
            NULL AS updates,
            NULL AS deletes,
            pg_total_relation_size(s.indexrelid) AS totalSize
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        WHERE NOT i.indisprimary;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO dba.tLogEntry (
                runUuid,
                timestamp,
                processType,
                stepcounter,
                userName,
                stepRuntime,
                totalRuntime,
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
    RAISE NOTICE 'pCaptureTableIndexStats procedure created';
END $OUTER$;

-- Grant execute permission
DO $OUTER$
BEGIN
    RAISE NOTICE 'Granting execute permission on pCaptureTableIndexStats';
    GRANT EXECUTE ON PROCEDURE dba.pCaptureTableIndexStats() TO etl_user;
    RAISE NOTICE 'Execute permission granted on pCaptureTableIndexStats';
END $OUTER$;