from app.telegram_helper import send_telegram_message
from scheduler.task_interface import Task


@Task.sub('send_telegram_notification')
class SendTelegramNotificationTask(Task):
    """Send one queued Telegram notification."""

    def validate(self, payload: dict):
        """Validate Telegram notification payload."""
        if payload.get('chatId') is None:
            raise ValueError('chatId is required')
        if not payload.get('message'):
            raise ValueError('message is required')

    def execute(self, payload: dict):
        """Send Telegram notification."""
        send_telegram_message(payload['chatId'], payload['message'])