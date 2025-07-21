-- Create procedure to monitor long-running queries
CREATE OR REPLACE PROCEDURE dba.pmonitorlongrunningqueries(thresholdminutes INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    run_uuid UUID := gen_random_uuid();
BEGIN
    FOR r IN (
        SELECT
            pid
            , usename
            , application_name
            , state
            , query
            , EXTRACT(EPOCH FROM (NOW() - query_start)) / 60 AS runningminutes
        FROM pg_stat_activity
        WHERE state = 'active'
          AND query_start IS NOT NULL
          AND NOW() - query_start > (thresholdminutes || ' minutes')::INTERVAL
          AND usename != 'postgres'
    )
    LOOP
        INSERT INTO dba.tlogentry (
            run_uuid
            , timestamp
            , processtype
            , stepcounter
            , username
            , stepruntime
            , totalruntime
            , message
        )
        VALUES (
            run_uuid
            , CURRENT_TIMESTAMP
            , 'QueryMonitor'
            , 'pid_' || r.pid
            , r.usename
            , r.runningminutes * 60
            , r.runningminutes * 60
            , format('Long-running query: PID=%s, App=%s, Query=%s', 
                     r.pid, r.application_name, r.query)
        );
    END LOOP;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO dba.tlogentry (
            run_uuid
            , timestamp
            , processtype
            , stepcounter
            , username
            , stepruntime
            , totalruntime
            , message
        )
        VALUES (
            run_uuid
            , CURRENT_TIMESTAMP
            , 'QueryMonitor'
            , 'error'
            , CURRENT_USER
            , NULL
            , NULL
            , 'Error monitoring queries: ' || SQLERRM
        );
        COMMIT;
        RAISE NOTICE 'Query monitoring failed: %', SQLERRM;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON PROCEDURE dba.pmonitorlongrunningqueries(INTEGER) TO etl_user;