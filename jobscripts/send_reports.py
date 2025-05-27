import sys
sys.path.append('/home/yostfundsadmin/client_etl_workflow')  # Add repository root to sys.path
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import os
import io
import sys
import logging
import csv
import traceback
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from systemscripts.db_config import SQLALCHEMY_DATABASE_URL  # Import centralized DB config
import grp

# Generate log file name with timestamp suffix (yyyyMMddThhmmss)
log_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
log_file = f"/home/yostfundsadmin/client_etl_workflow/logs/send_reports.log_{log_timestamp}"

# Set permissions for log file
try:
    open(log_file, 'a').close()  # Create or touch the file
    os.chmod(log_file, 0o660)
    try:
        group_id = grp.getgrnam('etl_group').gr_gid
        os.chown(log_file, os.getuid(), group_id)
    except KeyError:
        print(f"Warning: Group 'etl_group' not found; skipping chown for {log_file}")
except Exception as e:
    print(f"Failed to set permissions for log file {log_file}: {e}")

# Configure logging
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create SQLAlchemy engine
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Gmail credentials from environment variables
ETL_EMAIL = os.getenv("ETL_EMAIL")
ETL_EMAIL_PASSWORD = os.getenv("ETL_EMAIL_PASSWORD")

# Log the environment variables for debugging
logging.info(f"ETL_EMAIL: {ETL_EMAIL}")
logging.info(f"ETL_EMAIL_PASSWORD: {ETL_EMAIL_PASSWORD if ETL_EMAIL_PASSWORD else 'Not set'}")

def send_email(recipients, subject, body, attachments=None):
    """Send an email via Gmail SMTP with optional attachments."""
    msg = MIMEMultipart()
    msg['From'] = ETL_EMAIL
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject

    # Attach the HTML body
    msg.attach(MIMEText(body, 'html'))

    # Add attachments
    if attachments:
        for filename, content in attachments:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(content.encode('utf-8'))
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={filename}')
            msg.attach(part)

    # Send email using Gmail SMTP (SSL/TLS)
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.set_debuglevel(1)  # Enable debug output
            logging.info(f"Attempting to login with ETL_EMAIL: {ETL_EMAIL}")
            server.login(ETL_EMAIL, ETL_EMAIL_PASSWORD)
            server.sendmail(ETL_EMAIL, recipients, msg.as_string())
    except Exception as e:
        logging.error(f"SMTP error: {str(e)}\n{traceback.format_exc()}")
        raise

def process_reports(report_id=None):
    """Fetch reports from dba.treportmanager and send emails."""
    try:
        # Connect to PostgreSQL database using SQLAlchemy engine
        with engine.connect() as conn:
            # Fetch reports: either a specific report or all active reports
            if report_id:
                query = text("SELECT * FROM dba.treportmanager WHERE reportID = :report_id")
                reports = pd.read_sql(query, conn, params={"report_id": report_id}).to_dict('records')
            else:
                query = text("SELECT * FROM dba.treportmanager WHERE datastatusid = 1")
                reports = pd.read_sql(query, conn).to_dict('records')

            for report in reports:
                report_id = report['reportid']
                reportname = report['reportname']
                toheader = report['toheader']
                hasattachment = report['hasattachment']
                attachmentqueries = report['attachmentqueries']
                emailbodytemplate = report['emailbodytemplate']
                emailbodyqueries = report['emailbodyqueries']
                subject = report['subjectheader']
                
                # Parse recipients
                recipients = toheader.split(",")
                
                # Build email body
                body = emailbodytemplate if emailbodytemplate else "<h2>No Template Provided</h2>"
                if emailbodyqueries:  # Check if emailbodyqueries is not None
                    for placeholder, query in emailbodyqueries.items():
                        try:
                            # Wrap the query in text() for SQLAlchemy
                            sql_query = text(query)
                            df = pd.read_sql(sql_query, conn)
                            html_grid = df.to_html(index=False, border=1, classes="table table-striped", justify="center")
                            body = body.replace("{{" + placeholder + "}}", html_grid)
                        except Exception as e:
                            error_msg = f"<p>Error generating grid {placeholder}: {str(e)}</p>"
                            body = body.replace("{{" + placeholder + "}}", error_msg)
                            logging.error(f"Error generating grid {placeholder} for report {reportname} (ID: {report_id}): {str(e)}")
                
                # Generate attachments
                attachments = []
                if hasattachment and attachmentqueries:  # Check if attachmentqueries is not None
                    for att in attachmentqueries:
                        try:
                            # Wrap the query in text() for SQLAlchemy
                            sql_query = text(att['query'])
                            df = pd.read_sql(sql_query, conn)
                            csv_buffer = io.StringIO()
                            df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
                            csv_content = csv_buffer.getvalue()
                            csv_buffer.close()
                            attachments.append((att['name'], csv_content))
                        except Exception as e:
                            error_msg = f"<p>Error generating attachment {att['name']}: {str(e)}</p>"
                            body += error_msg
                            logging.error(f"Error generating attachment {att['name']} for report {reportname} (ID: {report_id}): {str(e)}")
                
                # Send email
                try:
                    send_email(recipients, subject, body, attachments)
                    logging.info(f"Sent report {reportname} (ID: {report_id}) to {toheader}")
                except Exception as e:
                    logging.error(f"Failed to send report {reportname} (ID: {report_id}): {str(e)}\n{traceback.format_exc()}")
                    raise

    except Exception as e:
        logging.error(f"Error processing reports: {str(e)}\n{traceback.format_exc()}")
        raise

# Run the script
if __name__ == "__main__":
    # Check if a reportID is provided as a command-line argument
    report_id = None

    if len(sys.argv) > 1:
        try:
            report_id = int(sys.argv[1])
        except ValueError:
            print(f"Invalid reportID: {sys.argv[1]}. Expected an integer.")
            sys.exit(1)

    process_reports(report_id=report_id)