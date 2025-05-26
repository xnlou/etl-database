import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import os
import psycopg2
import pandas as pd

# Define database connection parameters
DB_PARAMS = {
    "dbname": "feeds",
    "user": "yostfundsadmin",
    "password": os.getenv("DB_PASSWORD", "etlserver2025!"),  # Default password; override with environment variable
    "host": "localhost",
    "port": "5432"
}

def send_test_email(recipient_email):
    """Send a test email with an HTML table of SQL query results using Gmail's SMTP server."""
    try:
        # Retrieve Gmail credentials from environment variables
        sender_email = os.getenv("ETL_EMAIL")
        password = os.getenv("ETL_EMAIL_PASSWORD")
        
        # Verify credentials are set
        if not sender_email or not password:
            raise ValueError("ETL_EMAIL or ETL_EMAIL_PASSWORD environment variables not set")
        
        # Connect to PostgreSQL database and fetch query results
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    
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
;
                """)
                rows = cur.fetchall()
                columns = ['PeriodEndDate', 'PeriodStartDate']
                df = pd.DataFrame(rows, columns=columns)
        
        # Email configuration
        subject = f"Test Email with SQL Data - {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
        
        # Generate HTML table from DataFrame
        html_table = df.to_html(index=False, border=1, classes='table table-striped', justify='center')
        
        # Email body with HTML table
        body = f"""
        <html>
        <body>
            <h2>Test Email with SQL Data</h2>
            <p>This is a test email sent from your ETL pipeline using Gmail's SMTP server.</p>
            <p>Below is the date range from dba.tcalendardays:</p>
            {html_table}
            <p>Sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </body>
        </html>
        """
        
        # Create MIMEText object for HTML email
        msg = MIMEText(body, 'html')
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        
        # Send email using Gmail SMTP (SSL/TLS)
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        
        print(f"Test email with SQL data sent successfully to {recipient_email}")
    except psycopg2.Error as e:
        print(f"Database error: {str(e)}")
    except Exception as e:
        print(f"Failed to send test email: {str(e)}")

# Test email details
recipient_email = "xnlouey@gmail.com"  # Your recipient's email

# Send the test email
send_test_email(recipient_email)