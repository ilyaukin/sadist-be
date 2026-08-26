import json
import os
import urllib.parse
import urllib.request

BOT_COMMANDS = [
    {'command': 'pause', 'description': 'Pause notifications'},
    {'command': 'resume', 'description': 'Resume notifications'},
]


def normalize_telegram_username(username):
    """Normalize Telegram username for storing and matching."""
    if not username:
        return None
    return str(username).lstrip('@').lower()


def send_telegram_message(chat_id, text):
    """Send Telegram message and raise on configuration or API errors."""
    token = _bot_token()
    response = _telegram_request(token, 'sendMessage', {
        'chat_id': chat_id,
        'text': text,
    })
    if not response.get('ok'):
        raise RuntimeError('Telegram sendMessage failed: %s' % response)
    return response


def set_webhook(url, secret):
    """Set Telegram webhook URL and secret token."""
    token = _bot_token()
    response = _telegram_request(token, 'setWebhook', {
        'url': url,
        'secret_token': secret,
    })
    if not response.get('ok'):
        raise RuntimeError('Telegram setWebhook failed: %s' % response)
    return response


def delete_webhook():
    """Delete Telegram webhook configuration."""
    token = _bot_token()
    response = _telegram_request(token, 'deleteWebhook', {})
    if not response.get('ok'):
        raise RuntimeError('Telegram deleteWebhook failed: %s' % response)
    return response


def get_webhook_info():
    """Return Telegram webhook configuration information."""
    token = _bot_token()
    response = _telegram_request(token, 'getWebhookInfo', {})
    if not response.get('ok'):
        raise RuntimeError('Telegram getWebhookInfo failed: %s' % response)
    return response


def set_bot_commands(commands=None):
    """Set Telegram bot command menu."""
    token = _bot_token()
    response = _telegram_request(token, 'setMyCommands', {
        'commands': json.dumps(commands or BOT_COMMANDS),
    })
    if not response.get('ok'):
        raise RuntimeError('Telegram setMyCommands failed: %s' % response)
    return response


def delete_bot_commands():
    """Delete Telegram bot command menu."""
    token = _bot_token()
    response = _telegram_request(token, 'deleteMyCommands', {})
    if not response.get('ok'):
        raise RuntimeError('Telegram deleteMyCommands failed: %s' % response)
    return response


def _bot_token():
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not token:
        raise EnvironmentError('TELEGRAM_BOT_TOKEN env is required')
    return token


def _telegram_request(token, method, payload):
    data = urllib.parse.urlencode(payload).encode()
    request = urllib.request.Request(
        'https://api.telegram.org/bot%s/%s' % (token, method),
        data=data,
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode())