CREATE TABLE dba.tscheduler (
    schedulerID SERIAL PRIMARY KEY,
    taskname VARCHAR(255) NOT NULL,
    taskdescription TEXT,
    frequency VARCHAR(50) NOT NULL, -- Cron expression (e.g., "0 0 * * *")
    scriptpath VARCHAR(255) NOT NULL, -- Path to the script to execute (e.g., "/path/to/download_script.py")
    scriptargs TEXT, -- Optional arguments for the script (e.g., "arg1 arg2")
    datastatusid INT REFERENCES dba.tdatastatus(datastatusid),
    createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    createduser VARCHAR(100)
    
);

-- Add comments for clarity
COMMENT ON TABLE dba.tscheduler IS 'Stores scheduling information for ETL tasks.';
COMMENT ON COLUMN dba.tscheduler.schedulerID IS 'Primary key for the schedule.';
COMMENT ON COLUMN dba.tscheduler.taskname IS 'Unique name of the task (e.g., "daily_download").';
COMMENT ON COLUMN dba.tscheduler.taskdescription IS 'Description of the task.';
COMMENT ON COLUMN dba.tscheduler.frequency IS 'Cron expression defining the task schedule (e.g., "0 0 * * *").';
COMMENT ON COLUMN dba.tscheduler.scriptpath IS 'Path to the script to execute (e.g., "/path/to/script.py").';
COMMENT ON COLUMN dba.tscheduler.scriptargs IS 'Optional arguments for the script (e.g., "arg1 arg2").';
COMMENT ON COLUMN dba.tscheduler.datastatusid IS 'Foreign key to dba.tdatastatus, indicating task status (e.g., active, inactive).';
COMMENT ON COLUMN dba.tscheduler.createddate IS 'Timestamp when the record was created.';
COMMENT ON COLUMN dba.tscheduler.createduser IS 'User who created the record.';
COMMENT ON COLUMN dba.tscheduler.uk_taskname IS 'Ensures task names are unique.';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE ON dba.tscheduler TO etl_user;
GRANT ALL ON dba.tscheduler TO yostfundsadmin;
GRANT USAGE, SELECT ON SEQUENCE dba.tscheduler_schedulerid_seq TO etl_user;