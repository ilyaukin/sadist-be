from app.email_helper import send_email
from scheduler.task_interface import Task


@Task.sub('send_email_notification')
class SendEmailNotificationTask(Task):
    """Send one queued email notification."""

    def validate(self, payload: dict):
        """Validate email notification payload."""
        if not payload.get('toEmail'):
            raise ValueError('toEmail is required')
        if not payload.get('subject'):
            raise ValueError('subject is required')
        if not payload.get('message'):
            raise ValueError('message is required')

    def execute(self, payload: dict):
        """Send email notification."""
        send_email(payload['toEmail'], payload['subject'], payload['message'])