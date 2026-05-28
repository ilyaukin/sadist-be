import smtplib
import os
from email.mime.text import MIMEText
from app import logger

def send_email(to_email, subject, body):
    smtp_host = os.environ.get('SMTP_HOST')
    smtp_port = os.environ.get('SMTP_PORT', 587)
    smtp_user = os.environ.get('SMTP_USER')
    smtp_password = os.environ.get('SMTP_PASSWORD')
    from_email = os.environ.get('FROM_EMAIL', smtp_user)

    if not smtp_host:
        logger.error("SMTP host is required")
        return False

    if not smtp_user:
        logger.error("SMTP user is required")
        return False

    if not smtp_password:
        logger.error("SMTP password is required")
        return False

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    try:
        with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        return True
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        return False
