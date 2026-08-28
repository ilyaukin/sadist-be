from bson import ObjectId
from mongomoron import query

from app.telegram_helper import send_telegram_message
from db import app_user, conn, tg_chat
from scheduler.task_interface import Task

MATCHED_MESSAGE = 'Welcome, {username}! Your telegram has been added. Explore more fresh data at {base_url}!'
UNMATCHED_MESSAGE = 'Welcome, {username}! Add your telegram at {base_url} to get notifications about new data.'
STATUS_MESSAGE = '{username}, your notifications are {status}'


@Task.sub('match_telegram_chats')
class MatchTelegramChatsTask(Task):
    """Match Telegram chats with app users by normalized username."""

    def validate(self, payload: dict):
        """Validate optional chat selector payload."""
        if payload.get('chatId') is not None and payload.get('chatId') == '':
            raise ValueError('chatId must not be empty')
        if payload.get('userId') is not None and payload.get('userId') == '':
            raise ValueError('userId must not be empty')
        if payload.get('telegramUsername') is not None and payload.get('telegramUsername') == '':
            raise ValueError('telegramUsername must not be empty')
        if payload.get('status') is not None and payload.get('status') == '':
            raise ValueError('status must not be empty')
        if not payload.get('baseUrl'):
            raise ValueError('baseUrl is required')

    def execute(self, payload: dict):
        """Match known Telegram chats to app users and send welcome messages."""
        chat_id = payload.get('chatId')
        user = _get_user(payload.get('userId'), payload.get('telegramUsername'))
        chats = _get_chats(chat_id, payload.get('telegramUsername'), user)
        for chat in chats:
            _match_chat(chat, user, payload['baseUrl'], payload.get('status'), chat_id is not None)


def _get_chats(chat_id=None, telegram_username=None, user=None):
    if chat_id is None:
        if telegram_username:
            return conn.execute(query(tg_chat).filter(tg_chat.telegramUsername == telegram_username))
        if user:
            telegram_username = (user.get('settings') or {}).get('telegram')
            if telegram_username:
                return conn.execute(query(tg_chat).filter(tg_chat.telegramUsername == telegram_username))
        return conn.execute(query(tg_chat))
    return conn.execute(query(tg_chat).filter(tg_chat._id == chat_id))


def _get_user(user_id=None, telegram_username=None):
    if user_id:
        return next(iter(conn.execute(query(app_user).filter(app_user._id == _object_id(user_id)))), None)
    if telegram_username:
        return next(iter(conn.execute(query(app_user).filter(app_user.settings.telegram == telegram_username))), None)
    return None


def _object_id(value):
    if isinstance(value, ObjectId):
        return value
    return ObjectId(value)


def _match_chat(chat: dict, user: dict, base_url: str, status: str, notify_unmatched: bool):
    username = chat.get('telegramUsername')
    if not username:
        return
    user = user or next(iter(conn.execute(query(app_user).filter(app_user.settings.telegram == username))), None)
    if user:
        is_new_binding = _bind_chat_to_user(user, chat)
        if is_new_binding:
            message = MATCHED_MESSAGE.format(username=username, base_url=base_url)
        else:
            status = status or chat.get('status') or 'active'
            message = STATUS_MESSAGE.format(username=username, status=status)
    elif notify_unmatched:
        message = UNMATCHED_MESSAGE.format(username=username, base_url=base_url)
    else:
        return
    send_telegram_message(chat['_id'], message)


def _bind_chat_to_user(user: dict, chat: dict):
    # TODO: implement `$addToSet` support in mongomoron.
    result = conn.db()[app_user._name].update_one(
        {'_id': user['_id'], 'extra.telegramChatIds': {'$ne': chat['_id']}},
        {
            '$addToSet': {'extra.telegramChatIds': chat['_id']},
            '$set': {'extra.telegramUsername': chat.get('telegramUsername')},
        },
    )
    if result.modified_count:
        return True
    # TODO: implement `$set` support for nested fields in mongomoron update builders if needed.
    conn.db()[app_user._name].update_one(
        {'_id': user['_id']},
        {'$set': {'extra.telegramUsername': chat.get('telegramUsername')}},
    )
    return False