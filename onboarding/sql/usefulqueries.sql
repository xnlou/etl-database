SELECT
	t.label as eventID	
    ,COALESCE(
        company_name,
        "company/organization",
        company_description,
        "company_description_(bio)",
        organization_description,
        description
    ) AS company_name
    , COALESCE(ticker, company_ticker) AS ticker
FROM public.tmeetmaxevent m
join dba.tdataset t on t.datasetid = m.datasetid


WITH TickerData AS (
    SELECT 
        COALESCE(ticker, company_ticker) AS ticker,
        t.datasetdate
    FROM public.tmeetmaxevent m
    JOIN dba.tdataset t ON t.datasetid = m.datasetid
    WHERE 1 = 1
      --and t.label = '119179'
      AND t.datasetdate IN ('2025-05-21', '2025-05-23')
)
SELECT 
    ticker,
    'Added' AS status,
    '2025-05-23' AS datasetdate
FROM (
    SELECT ticker
    FROM TickerData
    WHERE datasetdate = '2025-05-23'
    EXCEPT
    SELECT ticker
    FROM TickerData
    WHERE datasetdate = '2025-05-21'
) AS added
UNION
SELECT 
    ticker,
    'Removed' AS status,
    '2025-05-21' AS datasetdate
FROM (
    SELECT ticker
    FROM TickerData
    WHERE datasetdate = '2025-05-21'
    EXCEPT
    SELECT ticker
    FROM TickerData
    WHERE datasetdate = '2025-05-23'
) AS removed
ORDER BY status, ticker, datasetdate;

--reporting



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
    WHERE c1.fulldate = CURRENT_DATE
),
EventsDataSets AS (
    SELECT 
        t.datasetid,
        t.datasetdate,
        t.label as eventid
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
    WHERE t.isactive = true
    and t.datasettypeid = 2
),
LatestURLCheckDataset AS (
    SELECT 
    	 eventid
        ,m.url
        ,m.ifexists
        ,m.invalideventid
        ,m.isdownloadable
        ,m.downloadlink
        ,m.statuscode
        ,m.title
        ,mu.maxdatasetdate
    FROM public.tmeetmaxurlcheck m
    JOIN dba.tdataset t ON t.datasetid = m.datasetid
    CROSS JOIN MaxURLCheckDate mu
    WHERE t.datasetdate = mu.maxdatasetdate
    AND t.isactive = TRUE
),
EventsData as (
select
	ed.eventid
	,UPPER(COALESCE(
        company_name,
        "company/organization",
        company_description,
        "company_description_(bio)",
        organization_description,
        description
    )) AS company_name
    , UPPER(COALESCE(ticker, company_ticker)) AS ticker
    ,min(ed.datasetdate) as mindate
    ,max(ed.datasetdate) as maxdate
from EventsDataSets ed
join public.tmeetmaxevent t on t.datasetid  = ed.datasetid
group by
	ed.eventid
		,UPPER(COALESCE(
        company_name,
        "company/organization",
        company_description,
        "company_description_(bio)",
        organization_description,
        description
    ))
    , UPPER(COALESCE(ticker, company_ticker)) 
)
    SELECT 
	    e.*
	    ,cd.url
	    ,x.scenario
	FROM EventsData e
	CROSS JOIN MaxURLCheckDate m
	join LatestURLCheckDataset cd on cd.eventid = e.eventid
	LEFT JOIN lateral(
		select case
			when m.maxdatasetdate > e.maxdate then 'removed'
			when e.mindate = e.maxdate and e.maxdate = CURRENT_DATE then 'added'
			when e.mindate = e.maxdate and e.maxdate < CURRENT_DATE then 'removed'
			when e.maxdate < CURRENT_DATE then 'removed'
			when e.mindate <> e.maxdate and e.maxdate = CURRENT_DATE then 'normal'
		end as scenario
	)x	on true
	WHERE 1=1
	and x.scenario <> 'normal'
	order by x.scenario desc,ticker


select
	t2.*
	,t.*
from public.tmeetmaxevent t 
join dba.tdataset t2 on t2.datasetid  =t.datasetid
where t.datasetid in (374
,1254
,1105
,1789)
and company_name = 'DINNER - Samir Shah, Former ICLR Executive'
and isactive = true
order by datasetdate



