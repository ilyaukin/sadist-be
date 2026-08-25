import os
import smtplib
from email.utils import formataddr
from email.mime.text import MIMEText


def send_email(to_email, subject, body):
    smtp_host = os.environ.get('SMTP_HOST')
    smtp_port = os.environ.get('SMTP_PORT', 587)
    smtp_user = os.environ.get('SMTP_USER')
    smtp_password = os.environ.get('SMTP_PASSWORD')
    from_email = os.environ.get('FROM_EMAIL', smtp_user)
    from_name = os.environ.get('FROM_NAME')

    if not smtp_host:
        raise EnvironmentError("SMTP_HOST env is required")

    if not smtp_user:
        raise EnvironmentError("SMTP_USER env is required")

    if not smtp_password:
        raise EnvironmentError("SMTP_PASSWORD env is required")

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = formataddr((from_name, from_email)) if from_name else from_email
    msg['To'] = to_email

    with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
    return True
