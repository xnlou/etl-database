CREATE OR REPLACE FUNCTION dba.f_insert_tscheduler(
    p_taskname VARCHAR,
    p_taskdescription TEXT,
    p_frequency VARCHAR,
    p_scriptpath VARCHAR,
    p_scriptargs TEXT,
    p_datastatusid INT,
    p_createduser VARCHAR
) RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_schedulerid INT;
BEGIN
    -- Validate datastatusid exists in dba.tdatastatus
    IF NOT EXISTS (
        SELECT 1
        FROM dba.tdatastatus
        WHERE datastatusid = p_datastatusid
    ) THEN
        RAISE EXCEPTION 'Data status ID % not found in dba.tdatastatus', p_datastatusid;
    END IF;

    -- Insert new schedule
    INSERT INTO dba.tscheduler (
        taskname,
        taskdescription,
        frequency,
        scriptpath,
        scriptargs,
        datastatusid,
        createddate,
        createduser
    ) VALUES (
        p_taskname,
        p_taskdescription,
        p_frequency,
        p_scriptpath,
        p_scriptargs,
        p_datastatusid,
        CURRENT_TIMESTAMP,
        p_createduser
    ) RETURNING schedulerID INTO v_schedulerid;

    RETURN v_schedulerid;
END;
$$;

-- Grant permissions on the function
GRANT EXECUTE ON FUNCTION dba.f_insert_tscheduler(VARCHAR, TEXT, VARCHAR, VARCHAR, TEXT, INT, VARCHAR) TO etl_user;

CREATE OR REPLACE FUNCTION dba.f_update_tscheduler(
    p_schedulerid INT,
    p_taskname VARCHAR,
    p_taskdescription TEXT,
    p_frequency VARCHAR,
    p_scriptpath VARCHAR,
    p_scriptargs TEXT,
    p_datastatusid INT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Validate datastatusid exists in dba.tdatastatus
    IF NOT EXISTS (
        SELECT 1
        FROM dba.tdatastatus
        WHERE datastatusid = p_datastatusid
    ) THEN
        RAISE EXCEPTION 'Data status ID % not found in dba.tdatastatus', p_datastatusid;
    END IF;

    -- Update the schedule
    UPDATE dba.tscheduler
    SET taskname = p_taskname,
        taskdescription = p_taskdescription,
        frequency = p_frequency,
        scriptpath = p_scriptpath,
        scriptargs = p_scriptargs,
        datastatusid = p_datastatusid
    WHERE schedulerID = p_schedulerid;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Schedule with ID % not found', p_schedulerid;
    END IF;
END;
$$;

-- Grant permissions on the function
GRANT EXECUTE ON FUNCTION dba.f_update_tscheduler(INT, VARCHAR, TEXT, VARCHAR, VARCHAR, TEXT, INT) TO etl_user;