-- Create procedure to purge old logs
CREATE OR REPLACE PROCEDURE dba.ppurgeoldlogs(thresholddays INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    starttime TIMESTAMP := CURRENT_TIMESTAMP;
    deletedrows INTEGER;
BEGIN
    -- Purge tddllogs
    DELETE FROM dba.tddllogs
    WHERE changetime < CURRENT_DATE - INTERVAL '1 day' * thresholddays;
    GET DIAGNOSTICS deletedrows = ROW_COUNT;
    INSERT INTO dba.tmaintenancelog (
        maintenancetime
        ,operation
        ,tablename
        ,username
        ,durationseconds
        ,details
    )
    VALUES (
        starttime
        ,'PURGE'
        ,'tDDLLogs'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - starttime))
        ,format('Deleted %s rows older than %s days', deletedrows, thresholddays)
    );
    
    -- Purge tlogentry
    DELETE FROM dba.tlogentry
    WHERE timestamp < CURRENT_DATE - INTERVAL '1 day' * thresholddays;
    GET DIAGNOSTICS deletedrows = ROW_COUNT;
    INSERT INTO dba.tmaintenancelog (
        maintenancetime
        ,operation
        ,tablename
        ,username
        ,durationseconds
        ,details
    )
    VALUES (
        starttime
        ,'PURGE'
        ,'tLogEntry'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - starttime))
        ,format('Deleted %s rows older than %s days', deletedrows, thresholddays)
    );
    
    -- Purge tmaintenancelog
    DELETE FROM dba.tmaintenancelog
    WHERE maintenancetime < CURRENT_DATE - INTERVAL '1 day' * thresholddays;
    GET DIAGNOSTICS deletedrows = ROW_COUNT;
    INSERT INTO dba.tmaintenancelog (
        maintenancetime
        ,operation
        ,tablename
        ,username
        ,durationseconds
        ,details
    )
    VALUES (
        starttime
        ,'PURGE'
        ,'tMaintenanceLog'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - starttime))
        ,format('Deleted %s rows older than %s days', deletedrows, thresholddays)
    );
    
    -- Purge ttableindexstats
    DELETE FROM dba.ttableindexstats
    WHERE snapshottime < CURRENT_DATE - INTERVAL '1 day' * thresholddays;
    GET DIAGNOSTICS deletedrows = ROW_COUNT;
    INSERT INTO dba.tmaintenancelog (
        maintenancetime
        ,operation
        ,tablename
        ,username
        ,durationseconds
        ,details
    )
    VALUES (
        starttime
        ,'PURGE'
        ,'tTableIndexStats'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - starttime))
        ,format('Deleted %s rows older than %s days', deletedrows, thresholddays)
    );
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO dba.tmaintenancelog (
            maintenancetime
            ,operation
            ,tablename
            ,username
            ,durationseconds
            ,details
        )
        VALUES (
            starttime
            ,'PURGE'
            ,NULL
            ,CURRENT_USER
            ,NULL
            ,'Error purging logs: ' || SQLERRM
        );
        COMMIT;
        RAISE NOTICE 'Log purge failed: %', SQLERRM;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON PROCEDURE dba.ppurgeoldlogs(INTEGER) TO etl_user;