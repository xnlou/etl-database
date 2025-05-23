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
