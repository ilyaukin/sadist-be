from bson import ObjectId
from flask import session
from mongomoron import insert_one

from app.telegram import telegram_webhook
from app.user import update_settings
from db import app_user, conn, task_active, tg_chat
from scheduler.tasks.match_telegram_chats import MatchTelegramChatsTask


def test_update_settings_normalizes_telegram_and_creates_matching_task(client):
    conn.db()[task_active._name].delete_many({'taskType': 'match_telegram_chats'})
    user_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'type': 'local',
        'settings': {'notificationChannels': ['email']},
    }))

    with client.application.test_request_context('/user/settings', method='PATCH', json={
        'telegram': '@TestUser',
    }):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        response = update_settings()

    user = conn.db()[app_user._name].find_one({'_id': user_id})
    assert user['settings'] == {
        'notificationChannels': ['email'],
        'telegram': 'testuser',
    }
    assert response['user']['settings'] == user['settings']
    task = conn.db()[task_active._name].find_one({'taskType': 'match_telegram_chats'})
    assert task['payload'] == {
        'userId': str(user_id),
        'telegramUsername': 'testuser',
        'baseUrl': 'http://localhost',
    }


def test_update_settings_does_not_create_matching_task_for_same_telegram(client):
    conn.db()[task_active._name].delete_many({'taskType': 'match_telegram_chats'})
    user_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'type': 'local',
        'settings': {
            'telegram': 'testuser',
            'notificationChannels': ['email'],
        },
    }))

    with client.application.test_request_context('/user/settings', method='PATCH', json={
        'telegram': '@TestUser',
        'notificationChannels': ['email', 'telegram'],
    }):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        update_settings()

    assert conn.db()[task_active._name].find_one({'taskType': 'match_telegram_chats'}) is None
    user = conn.db()[app_user._name].find_one({'_id': user_id})
    assert user['settings'] == {
        'telegram': 'testuser',
        'notificationChannels': ['email', 'telegram'],
    }


def test_telegram_webhook_saves_chat_and_creates_matching_task(client, monkeypatch):
    monkeypatch.setenv('TELEGRAM_WEBHOOK_SECRET', 'secret')
    conn.db()[tg_chat._name].delete_many({})
    conn.db()[task_active._name].delete_many({'taskType': 'match_telegram_chats'})

    with client.application.test_request_context(
            '/telegram/webhook',
            method='POST',
            json={
                'message': {
                    'text': '/start',
                    'chat': {'id': 123, 'username': '@TestUser'},
                },
            },
            headers={'X-Telegram-Bot-Api-Secret-Token': 'secret'}):
        response = telegram_webhook()

    assert response == {'success': True}
    chat = conn.db()[tg_chat._name].find_one({'_id': 123})
    assert chat['telegramUsername'] == 'testuser'
    assert chat['status'] == 'active'
    task = conn.db()[task_active._name].find_one({'taskType': 'match_telegram_chats'})
    assert task['payload'] == {'chatId': 123, 'telegramUsername': 'testuser', 'baseUrl': 'http://localhost'}


def test_telegram_webhook_pause_and_resume(client, monkeypatch):
    monkeypatch.setenv('TELEGRAM_WEBHOOK_SECRET', 'secret')
    conn.db()[tg_chat._name].delete_many({})

    for command, expected_status in [('/pause', 'paused'), ('/resume', 'active')]:
        with client.application.test_request_context(
                '/telegram/webhook',
                method='POST',
                json={
                    'message': {
                        'text': command,
                        'chat': {'id': 123, 'username': 'TestUser'},
                    },
                },
                headers={'X-Telegram-Bot-Api-Secret-Token': 'secret'}):
            telegram_webhook()
        assert conn.db()[tg_chat._name].find_one({'_id': 123})['status'] == expected_status


def test_telegram_webhook_keeps_paused_status_until_resume(client, monkeypatch):
    monkeypatch.setenv('TELEGRAM_WEBHOOK_SECRET', 'secret')
    conn.db()[tg_chat._name].delete_many({})
    conn.execute(insert_one(tg_chat, {
        '_id': 123,
        'telegramUsername': 'testuser',
        'status': 'paused',
    }))

    with client.application.test_request_context(
            '/telegram/webhook',
            method='POST',
            json={
                'message': {
                    'text': '/start',
                    'chat': {'id': 123, 'username': 'TestUser'},
                },
            },
            headers={'X-Telegram-Bot-Api-Secret-Token': 'secret'}):
        telegram_webhook()

    assert conn.db()[tg_chat._name].find_one({'_id': 123})['status'] == 'paused'


def test_match_telegram_chats_binds_user_and_sends_matched_welcome(monkeypatch):
    messages = []
    conn.db()[tg_chat._name].delete_many({})
    user_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'settings': {'telegram': 'matcheduser'},
        'extra': {'telegramChatIds': [1]},
    }))
    conn.execute(insert_one(tg_chat, {
        '_id': 2,
        'telegramUsername': 'matcheduser',
        'status': 'active',
    }))
    monkeypatch.setenv('BASE_URL', 'http://sadist.test')
    monkeypatch.setattr('scheduler.tasks.match_telegram_chats.send_telegram_message',
                        lambda *args: messages.append(args))

    MatchTelegramChatsTask().execute({'chatId': 2, 'baseUrl': 'http://sadist.test'})

    user = conn.db()[app_user._name].find_one({'_id': user_id})
    assert user['extra']['telegramChatIds'] == [1, 2]
    assert user['extra']['telegramUsername'] == 'matcheduser'
    assert messages == [(2, 'Welcome, matcheduser! Your telegram has been added. Explore more fresh data at http://sadist.test!')]


def test_match_telegram_chats_sends_unmatched_welcome(monkeypatch):
    messages = []
    conn.db()[tg_chat._name].delete_many({})
    conn.execute(insert_one(tg_chat, {
        '_id': 3,
        'telegramUsername': 'unknown',
        'status': 'active',
    }))
    monkeypatch.setattr('scheduler.tasks.match_telegram_chats.send_telegram_message',
                        lambda *args: messages.append(args))

    MatchTelegramChatsTask().execute({'chatId': 3, 'baseUrl': 'http://sadist.test'})

    assert messages == [(3, 'Welcome, unknown! Add your telegram at http://sadist.test to get notifications about new data.')]


def test_match_telegram_chats_uses_user_id_to_find_username_chats(monkeypatch):
    messages = []
    conn.db()[tg_chat._name].delete_many({})
    user_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'settings': {'telegram': 'directuser'},
    }))
    conn.execute(insert_one(tg_chat, {
        '_id': 5,
        'telegramUsername': 'directuser',
        'status': 'active',
    }))
    monkeypatch.setattr('scheduler.tasks.match_telegram_chats.send_telegram_message',
                        lambda *args: messages.append(args))

    MatchTelegramChatsTask().execute({'userId': str(user_id), 'baseUrl': 'http://sadist.test'})

    user = conn.db()[app_user._name].find_one({'_id': user_id})
    assert user['extra']['telegramChatIds'] == [5]
    assert messages == [(5, 'Welcome, directuser! Your telegram has been added. Explore more fresh data at http://sadist.test!')]


def test_match_telegram_chats_uses_telegram_username_to_find_user(monkeypatch):
    messages = []
    conn.db()[tg_chat._name].delete_many({})
    user_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'settings': {'telegram': 'directchat'},
    }))
    conn.execute(insert_one(tg_chat, {
        '_id': 6,
        'telegramUsername': 'directchat',
        'status': 'active',
    }))
    monkeypatch.setattr('scheduler.tasks.match_telegram_chats.send_telegram_message',
                        lambda *args: messages.append(args))

    MatchTelegramChatsTask().execute({'telegramUsername': 'directchat', 'baseUrl': 'http://sadist.test'})

    user = conn.db()[app_user._name].find_one({'_id': user_id})
    assert user['extra']['telegramChatIds'] == [6]
    assert messages == [(6, 'Welcome, directchat! Your telegram has been added. Explore more fresh data at http://sadist.test!')]


def test_match_telegram_chats_skips_unmatched_welcome_without_chat_id(monkeypatch):
    messages = []
    conn.db()[tg_chat._name].delete_many({})
    conn.execute(insert_one(tg_chat, {
        '_id': 4,
        'telegramUsername': 'unknown',
        'status': 'active',
    }))
    monkeypatch.setattr('scheduler.tasks.match_telegram_chats.send_telegram_message',
                        lambda *args: messages.append(args))

    MatchTelegramChatsTask().execute({'baseUrl': 'http://sadist.test'})

    assert messages == []


def test_match_telegram_chats_requires_base_url():
    try:
        MatchTelegramChatsTask().validate({'chatId': 1})
        assert False
    except ValueError as e:
        assert str(e) == 'baseUrl is required'