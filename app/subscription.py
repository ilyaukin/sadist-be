from bson import ObjectId
from flask import request, session
from mongomoron import delete, insert_one, query

from app import app
from db import conn, ds_subscription


@app.route('/subscribe', methods=['POST'])
def create_subscription():
    """Create a DS subscription and subscribe the current user to it."""
    user_id = _current_user_id()
    payload = request.get_json()
    _validate_subscription_payload(payload)
    item = {
        'dsName': payload['dsName'],
        'query': payload.get('query') or {},
        'fields': payload['fields'],
        'message': payload['message'],
        'userIds': [user_id],
    }
    item['_id'] = conn.execute(insert_one(ds_subscription, item)).inserted_id
    return {'item': _subscription_response(item, user_id), 'success': True}


@app.route('/subscribe/<subscription_id>', methods=['POST'])
def subscribe(subscription_id):
    """Subscribe the current user to an existing DS subscription."""
    user_id = _current_user_id()
    # TODO: implement `$addToSet` support in mongomoron.
    conn.db()[ds_subscription._name].update_one(
        {'_id': ObjectId(subscription_id)},
        {'$addToSet': {'userIds': user_id}},
    )
    return {'success': True}


@app.route('/unsubscribe', methods=['POST'])
def unsubscribe_all():
    """Unsubscribe the current user from all DS subscriptions."""
    user_id = _current_user_id()
    # TODO: implement `$pull` support in mongomoron.
    conn.db()[ds_subscription._name].update_many(
        {'userIds': user_id},
        {'$pull': {'userIds': user_id}},
    )
    _delete_empty_subscriptions()
    return {'success': True}


@app.route('/unsubscribe/<subscription_id>', methods=['POST'])
def unsubscribe(subscription_id):
    """Unsubscribe the current user from one DS subscription."""
    user_id = _current_user_id()
    # TODO: implement `$pull` support in mongomoron.
    conn.db()[ds_subscription._name].update_one(
        {'_id': ObjectId(subscription_id)},
        {'$pull': {'userIds': user_id}},
    )
    _delete_empty_subscriptions()
    return {'success': True}


@app.route('/ds/subscriptions')
def list_ds_subscriptions():
    """List subscriptions for a DS name and mark current user's subscription state."""
    user_id = _optional_user_id()
    ds_name = request.args['ds_name']
    items = conn.execute(query(ds_subscription).filter(ds_subscription.dsName == ds_name))
    return {'list': [_subscription_response(item, user_id) for item in items], 'success': True}


@app.route('/subscriptions')
def list_my_subscriptions():
    """List all DS subscriptions of the current user."""
    user_id = _current_user_id()
    items = conn.execute(query(ds_subscription).filter(ds_subscription.userIds == user_id))
    return {'list': [_subscription_response(item) for item in items], 'success': True}


def _validate_subscription_payload(payload: dict):
    if not payload:
        raise ValueError('payload is required')
    if not payload.get('dsName'):
        raise ValueError('dsName is required')
    if not isinstance(payload.get('query') or {}, dict):
        raise ValueError('query must be an object')
    for key, value in (payload.get('query') or {}).items():
        if key.startswith('$') or isinstance(value, dict):
            raise ValueError('Only simple equality query is supported')
    if not isinstance(payload.get('fields'), list) or not payload['fields']:
        raise ValueError('fields must be a non-empty list')
    if not all(isinstance(field, str) and field for field in payload['fields']):
        raise ValueError('fields must contain field names')
    if not payload.get('message'):
        raise ValueError('message is required')


def _subscription_response(item: dict, user_id=None):
    response = {
        '_id': str(item['_id']),
        'dsName': item['dsName'],
        'query': item.get('query') or {},
        'fields': item.get('fields') or [],
        'message': item.get('message'),
    }
    if user_id is not None:
        response['subscribed'] = user_id in item.get('userIds', [])
    return response


def _current_user_id():
    if 'user' not in session:
        raise Exception('Authorization is required')
    return str(session['user']['_id'])


def _optional_user_id():
    if 'user' not in session:
        return None
    return str(session['user']['_id'])


def _delete_empty_subscriptions():
    q = delete(ds_subscription)
    q.filter(ds_subscription.userIds == [])
    conn.execute(q)