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