-- Create procedure to monitor long-running queries
CREATE OR REPLACE PROCEDURE dba.pMonitorLongRunningQueries(thresholdMinutes INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    runUuid UUID := gen_random_uuid();
BEGIN
    FOR r IN (
        SELECT
            pid
            ,usename
            ,application_name
            ,state
            ,query
            ,EXTRACT(EPOCH FROM (now() - query_start)) / 60 AS runningMinutes
        FROM pg_stat_activity
        WHERE state = 'active'
          AND query_start IS NOT NULL
          AND now() - query_start > (thresholdMinutes || ' minutes')::INTERVAL
          AND usename != 'postgres'
    )
    LOOP
        INSERT INTO dba.tLogEntry (
            runUuid
            ,timestamp
            ,processType
            ,stepcounter
            ,userName
            ,stepRuntime
            ,totalRuntime
            ,message
        )
        VALUES (
            runUuid
            ,CURRENT_TIMESTAMP
            ,'QueryMonitor'
            ,'pid_' || r.pid
            ,r.usename
            ,r.runningMinutes * 60
            ,r.runningMinutes * 60
            ,format('Long-running query: PID=%s, App=%s, Query=%s', 
                   r.pid, r.application_name, r.query)
        );
    END LOOP;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO dba.tLogEntry (
            runUuid
            ,timestamp
            ,processType
            ,stepcounter
            ,userName
            ,stepRuntime
            ,totalRuntime
            ,message
        )
        VALUES (
            runUuid
            ,CURRENT_TIMESTAMP
            ,'QueryMonitor'
            ,'error'
            ,CURRENT_USER
            ,NULL
            ,NULL
            ,'Error monitoring queries: ' || SQLERRM
        );
        COMMIT;
        RAISE NOTICE 'Query monitoring failed: %', SQLERRM;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON PROCEDURE dba.pMonitorLongRunningQueries(INTEGER) TO etl_user;