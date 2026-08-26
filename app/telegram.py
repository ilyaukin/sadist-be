import os

from flask import request
from mongomoron import insert_one, update

from app import app, logger
from app.telegram_helper import normalize_telegram_username
from app.url_helper import get_base_url
from db import conn, tg_chat
from scheduler.task_interface import create_task, EXECUTION_TYPE_SINGLE


@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    """Receive Telegram webhook updates and persist supported chat data."""
    _validate_webhook_secret()
    update_data = request.get_json() or {}
    message = update_data.get('message') or {}
    chat = message.get('chat') or {}
    chat_id = chat.get('id')
    username = normalize_telegram_username(chat.get('username'))
    if not chat_id:
        return {'success': True}
    if not username:
        logger.info('Telegram webhook update has no chat username: %s', update_data)
        return {'success': True}

    status = _status_from_command(message.get('text'))
    _save_chat(chat_id, username, status, update_data)
    create_task('match_telegram_chats', EXECUTION_TYPE_SINGLE, {
        'chatId': chat_id,
        'telegramUsername': username,
        'baseUrl': get_base_url('http://localhost'),
    })
    return {'success': True}


def _validate_webhook_secret():
    expected_secret = os.environ.get('TELEGRAM_WEBHOOK_SECRET')
    if not expected_secret:
        raise EnvironmentError('TELEGRAM_WEBHOOK_SECRET env is required')
    actual_secret = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
    if actual_secret != expected_secret:
        raise PermissionError('Invalid Telegram webhook secret')


def _status_from_command(text):
    if not text:
        return None
    command = text.split()[0].split('@')[0]
    if command == '/pause':
        return 'paused'
    if command == '/resume':
        return 'active'
    return None


def _save_chat(chat_id, username, status, update_data):
    changes = {
        'telegramUsername': username,
        'lastUpdate': update_data,
    }
    if status:
        changes['status'] = status
    result = conn.execute(update(tg_chat).filter(tg_chat._id == chat_id).set(changes))
    if result.matched_count:
        return
    changes['status'] = status or 'active'
    document = {'_id': chat_id, **changes}
    conn.execute(insert_one(tg_chat, document))