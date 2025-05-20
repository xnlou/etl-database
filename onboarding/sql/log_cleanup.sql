-- Create procedure to purge old logs
CREATE OR REPLACE PROCEDURE dba.pPurgeOldLogs(thresholdDays INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    startTime TIMESTAMP := CURRENT_TIMESTAMP;
    deletedRows INTEGER;
BEGIN
    -- Purge tDDLLogs
    DELETE FROM dba.tDDLLogs
    WHERE changeTime < CURRENT_DATE - INTERVAL '1 day' * thresholdDays;
    GET DIAGNOSTICS deletedRows = ROW_COUNT;
    INSERT INTO dba.tMaintenanceLog (
        maintenanceTime
        ,operation
        ,tableName
        ,userName
        ,durationSeconds
        ,details
    )
    VALUES (
        startTime
        ,'PURGE'
        ,'tDDLLogs'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - startTime))
        ,format('Deleted %s rows older than %s days', deletedRows, thresholdDays)
    );
    
    -- Purge tLogEntry
    DELETE FROM dba.tLogEntry
    WHERE timestamp < CURRENT_DATE - INTERVAL '1 day' * thresholdDays;
    GET DIAGNOSTICS deletedRows = ROW_COUNT;
    INSERT INTO dba.tMaintenanceLog (
        maintenanceTime
        ,operation
        ,tableName
        ,userName
        ,durationSeconds
        ,details
    )
    VALUES (
        startTime
        ,'PURGE'
        ,'tLogEntry'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - startTime))
        ,format('Deleted %s rows older than %s days', deletedRows, thresholdDays)
    );
    
    -- Purge tMaintenanceLog
    DELETE FROM dba.tMaintenanceLog
    WHERE maintenanceTime < CURRENT_DATE - INTERVAL '1 day' * thresholdDays;
    GET DIAGNOSTICS deletedRows = ROW_COUNT;
    INSERT INTO dba.tMaintenanceLog (
        maintenanceTime
        ,operation
        ,tableName
        ,userName
        ,durationSeconds
        ,details
    )
    VALUES (
        startTime
        ,'PURGE'
        ,'tMaintenanceLog'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - startTime))
        ,format('Deleted %s rows older than %s days', deletedRows, thresholdDays)
    );
    
    -- Purge tTableIndexStats
    DELETE FROM dba.tTableIndexStats
    WHERE snapshotTime < CURRENT_DATE - INTERVAL '1 day' * thresholdDays;
    GET DIAGNOSTICS deletedRows = ROW_COUNT;
    INSERT INTO dba.tMaintenanceLog (
        maintenanceTime
        ,operation
        ,tableName
        ,userName
        ,durationSeconds
        ,details
    )
    VALUES (
        startTime
        ,'PURGE'
        ,'tTableIndexStats'
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - startTime))
        ,format('Deleted %s rows older than %s days', deletedRows, thresholdDays)
    );
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO dba.tMaintenanceLog (
            maintenanceTime
            ,operation
            ,tableName
            ,userName
            ,durationSeconds
            ,details
        )
        VALUES (
            startTime
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
GRANT EXECUTE ON PROCEDURE dba.pPurgeOldLogs(INTEGER) TO etl_user;