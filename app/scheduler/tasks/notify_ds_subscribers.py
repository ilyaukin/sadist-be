import logging
from string import Formatter

import pymongo
from bson import ObjectId
from mongomoron import query

from db import app_user, conn, ds, ds_list, ds_subscription, tg_chat
from scheduler.task_interface import Task, create_task, EXECUTION_TYPE_SINGLE

logger = logging.getLogger(__name__)


@Task.sub('notify_ds_subscribers')
class NotifyDsSubscribersTask(Task):
    """Notify users about subscription-matching DS updates."""

    def validate(self, payload: dict):
        """Validate notification task payload before task creation."""
        if not payload.get('dsId'):
            raise ValueError('dsId is required')
        if not payload.get('dsName'):
            raise ValueError('dsName is required')
        if not payload.get('baseUrl'):
            raise ValueError('baseUrl is required')

    def execute(self, payload: dict):
        """Check DS subscriptions and send notifications for matching updates."""
        ds_id = payload['dsId']
        ds_name = payload['dsName']
        old_ds = _get_previous_ds(ds_id, ds_name)
        if not old_ds:
            return

        subscriptions = conn.execute(
            query(ds_subscription).filter(ds_subscription.dsName == ds_name))
        for subscription in subscriptions:
            new_documents = _find_new_documents(
                str(old_ds['_id']),
                ds_id,
                subscription.get('query') or {},
                subscription.get('fields') or [],
            )
            if new_documents:
                message = _format_message(subscription, new_documents, ds_id,
                                          payload['baseUrl'])
                _send_subscription_message(subscription, message)


def _get_previous_ds(ds_id: str, ds_name: str):
    return next(iter(conn.execute(
        query(ds_list)
        .filter(ds_list.name == ds_name)
        .filter(ds_list.status == 'old')
        .filter(ds_list._id != ObjectId(ds_id))
        .sort((ds_list._createdAt, pymongo.DESCENDING))
    )), None)


def _find_new_documents(old_ds_id: str, new_ds_id: str,
                        subscription_query: dict, fields: list[str]) -> list[
    dict]:
    _validate_query(subscription_query)
    if not fields:
        raise ValueError('fields must be a non-empty list')

    old_keys = set(_document_key(document, fields) for document in
                   _find_ds_documents(old_ds_id, subscription_query))
    return [
        document
        for document in _find_ds_documents(new_ds_id, subscription_query)
        if _document_key(document, fields) not in old_keys
    ]


def _format_message(subscription: dict, new_documents: list[dict], ds_id: str,
                    base_url: str) -> str:
    format_context = dict(new_documents[0])
    format_context['$#'] = len(new_documents)
    format_context['$url'] = '%s/?id=%s' % (base_url.rstrip('/'), ds_id)
    return _format_with_dollar_keys(subscription['message'], format_context)


def _send_subscription_message(subscription: dict, message: str):
    subject = 'DS %s has updates' % subscription['dsName']
    for user in _get_subscribed_users(subscription.get('userIds') or []):
        channels = _get_notification_channels(user)
        for channel in channels:
            if channel == 'email':
                _create_email_notification(user, subject, message)
            if channel == 'telegram':
                _create_telegram_notifications(user, message)


def _create_email_notification(user: dict, subject: str, message: str):
    email = user.get('extra', {}).get('email')
    logger.info(f"Queueing email notification\nTo: {email}\nMessage: {message}")
    if email:
        create_task('send_email_notification', EXECUTION_TYPE_SINGLE, {
            'toEmail': email,
            'subject': subject,
            'message': message,
        })


def _create_telegram_notifications(user: dict, message: str):
    chat_ids = user.get('extra', {}).get('telegramChatIds') or []
    for chat in _get_active_chats(chat_ids):
        create_task('send_telegram_notification', EXECUTION_TYPE_SINGLE, {
            'chatId': chat['_id'],
            'message': message,
        })


def _get_notification_channels(user: dict):
    settings = user.get('settings') or {}
    if 'notificationChannels' not in settings:
        return ['email']
    return settings['notificationChannels']


def _find_ds_documents(ds_id: str, subscription_query: dict):
    q = query(ds[ds_id])
    q.query_filer_document.update(subscription_query)
    return conn.execute(q)


def _get_active_chats(chat_ids: list):
    if not chat_ids:
        return []
    # TODO: implement `in` query helper usage for Telegram chat ID lists in mongomoron.
    return conn.db()[tg_chat._name].find({
        '_id': {'$in': chat_ids},
        'status': 'active',
    })


def _get_subscribed_users(user_ids: list[str]):
    object_ids = []
    for user_id in user_ids:
        try:
            object_ids.append(ObjectId(user_id))
        except Exception:
            pass
    if not object_ids:
        return []
    # TODO: implement `in` query helper usage for ObjectId lists in mongomoron.
    return conn.db()[app_user._name].find({'_id': {'$in': object_ids}})


def _document_key(document: dict, fields: list[str]) -> tuple:
    return tuple(document[field] for field in fields)


def _validate_query(subscription_query: dict):
    for key, value in subscription_query.items():
        if key.startswith('$') or isinstance(value, dict):
            raise ValueError('Only simple equality query is supported')


def _format_with_dollar_keys(template: str, format_context: dict) -> str:
    result = []
    for literal_text, field_name, format_spec, conversion in Formatter().parse(
            template):
        result.append(literal_text)
        if field_name is None:
            continue
        value = format_context[field_name]
        if conversion:
            value = Formatter().convert_field(value, conversion)
        result.append(Formatter().format_field(value, format_spec))
    return ''.join(result)
