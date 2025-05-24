import smtplib
from email.mime.text import MIMEText
from datetime import datetime

def send_test_email(sender_email, password, recipient_email):
    """Send a test email using Gmail's SMTP server."""
    try:
        # Email configuration
        subject = f"Test Email - {datetime.now().strftime('%Y%m%d %H:%M:%S')}"
        body = """
        <html>
        <body>
            <h2>Test Email</h2>
            <p>This is a test email sent from your ETL pipeline using Gmail's SMTP server.</p>
            <p>Sent at: {}</p>
        </body>
        </html>
        """.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # Create MIMEText object for HTML email
        msg = MIMEText(body, 'html')
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        
        # Send email using Gmail SMTP (SSL/TLS)
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        
        print(f"Test email sent successfully to {recipient_email}")
    except Exception as e:
        print(f"Failed to send test email: {str(e)}")

# Test email details
sender_email = "yostfundsdata@gmail.com"  # Your Gmail address
password = "cvkc qabw jkqr oiex"  # Replace with your app-specific password (e.g., "abcdefghijklmnop")
recipient_email = "xnlouey@gmail.com"  # Replace with the recipient's email

# Send the test email
send_test_email(sender_email, password, recipient_email)