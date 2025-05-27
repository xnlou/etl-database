CREATE OR REPLACE FUNCTION dba.f_get_event_changes(p_fulldate DATE DEFAULT NULL)
RETURNS TABLE (
    eventid VARCHAR,
    company_name TEXT,
    ticker TEXT,
    mindate DATE,
    maxdate DATE,
    url TEXT,
    scenario TEXT
)
LANGUAGE SQL
AS $$
WITH DateRange AS (
    SELECT 
        c1.fulldate AS periodenddate,
        (SELECT MAX(c2.fulldate)
         FROM dba.tcalendardays c2
         WHERE c2.fulldate <= (c1.fulldate - INTERVAL '45 days')
         AND c2.isbusday = TRUE
         AND c2.isholiday = FALSE
        ) AS periodstartdate
    FROM dba.tcalendardays c1
    WHERE c1.fulldate = COALESCE(p_fulldate, CURRENT_DATE)
),
EventsDataSets AS (
    SELECT 
        t.datasetid,
        t.datasetdate,
        t.label AS eventid
    FROM dba.tdataset t
    CROSS JOIN DateRange d
    WHERE t.datasettypeid = 3
    AND t.isactive = TRUE
    AND t.datasetdate BETWEEN d.periodstartdate AND d.periodenddate
),
MaxURLCheckDate AS (
    SELECT 
        MAX(t.datasetdate) AS maxdatasetdate
    FROM dba.tdataset t
    WHERE t.isactive = TRUE
    AND t.datasettypeid = 2
),
LatestURLCheckDataset AS (
    SELECT 
        eventid,
        m.url,
        m.ifexists,
        m.invalideventid,
        m.isdownloadable,
        m.downloadlink,
        m.statuscode,
        m.title,
        mu.maxdatasetdate
    FROM public.tmeetmaxurlcheck m
    JOIN dba.tdataset t ON t.datasetid = m.datasetid
    CROSS JOIN MaxURLCheckDate mu
    WHERE t.datasetdate = mu.maxdatasetdate
    AND t.isactive = TRUE
),
EventsData AS (
    SELECT
        ed.eventid,
        UPPER(COALESCE(
            company_name,
            "company/organization",
            company_description,
            "company_description_(bio)",
            organization_description,
            description
        )) AS company_name,
        UPPER(COALESCE(ticker, company_ticker)) AS ticker,
        MIN(ed.datasetdate) AS mindate,
        MAX(ed.datasetdate) AS maxdate
    FROM EventsDataSets ed
    JOIN public.tmeetmaxevent t ON t.datasetid = ed.datasetid
    GROUP BY
        ed.eventid,
        UPPER(COALESCE(
            company_name,
            "company/organization",
            company_description,
            "company_description_(bio)",
            organization_description,
            description
        )),
        UPPER(COALESCE(ticker, company_ticker))
)
SELECT 
    e.eventid,
    e.company_name,
    e.ticker,
    e.mindate,
    e.maxdate,
    cd.url,
    x.scenario
FROM EventsData e
CROSS JOIN MaxURLCheckDate m
JOIN LatestURLCheckDataset cd ON cd.eventid = e.eventid
LEFT JOIN LATERAL (
    SELECT CASE
        WHEN m.maxdatasetdate > e.maxdate THEN 'removed'
        WHEN e.mindate = e.maxdate AND e.maxdate = COALESCE(p_fulldate, CURRENT_DATE) THEN 'added'
        WHEN e.mindate = e.maxdate AND e.maxdate < COALESCE(p_fulldate, CURRENT_DATE) THEN 'removed'
        WHEN e.maxdate < COALESCE(p_fulldate, CURRENT_DATE) THEN 'removed'
        WHEN e.mindate <> e.maxdate AND e.maxdate = COALESCE(p_fulldate, CURRENT_DATE) THEN 'normal'
    END AS scenario
) x ON TRUE
WHERE 1=1
AND x.scenario <> 'normal'
ORDER BY x.scenario DESC, ticker;
$$;

-- Grant permissions on the function
GRANT EXECUTE ON FUNCTION dba.f_get_event_changes(DATE) TO etl_user;