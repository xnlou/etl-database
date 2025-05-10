-- Create table to log maintenance activities
CREATE TABLE dba.tMaintenanceLog (
    ,logId SERIAL PRIMARY KEY
    ,maintenanceTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,operation VARCHAR(50) NOT NULL
    ,tableName VARCHAR(100)
    ,userName VARCHAR(50)
    ,durationSeconds FLOAT
    ,details TEXT
);

-- Grant permissions
GRANT SELECT, INSERT ON dba.tMaintenanceLog TO etl_user;
GRANT ALL ON dba.tMaintenanceLog TO yostfundsadmin;
GRANT USAGE, SELECT ON SEQUENCE dba.tMaintenanceLog_logId_seq TO etl_user;

-- Create index for faster queries
CREATE INDEX idx_tMaintenanceLog_maintenanceTime ON dba.tMaintenanceLog(maintenanceTime);

-- Create procedure for VACUUM ANALYZE
CREATE OR REPLACE PROCEDURE dba.pRunMaintenanceVacuumAnalyze()
LANGUAGE plpgsql
AS $$
DECLARE
    startTime TIMESTAMP;
    endTime TIMESTAMP;
BEGIN
    startTime := CURRENT_TIMESTAMP;
    -- Run VACUUM ANALYZE on all tables
    PERFORM vacuum_analyze FROM (
        SELECT format('%I.%I', nspname, relname) AS tableName
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ) t;
    endTime := CURRENT_TIMESTAMP;
    
    -- Log the operation
    INSERT INTO dba.tMaintenanceLog (
        ,maintenanceTime
        ,operation
        ,tableName
        ,userName
        ,durationSeconds
        ,details
    )
    VALUES (
        ,startTime
        ,'VACUUM ANALYZE'
        ,NULL
        ,CURRENT_USER
        ,EXTRACT(EPOCH FROM (endTime - startTime))
        ,'Database-wide VACUUM ANALYZE'
    );
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO dba.tMaintenanceLog (
            ,maintenanceTime
            ,operation
            ,tableName
            ,userName
            ,durationSeconds
            ,details
        )
        VALUES (
            ,startTime
            ,'VACUUM ANALYZE'
            ,NULL
            ,CURRENT_USER
            ,NULL
            ,'Error: ' || SQLERRM
        );
        COMMIT;
        RAISE NOTICE 'Maintenance failed: %', SQLERRM;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON PROCEDURE dba.pRunMaintenanceVacuumAnalyze() TO etl_user;