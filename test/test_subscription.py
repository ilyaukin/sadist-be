from bson import ObjectId
from flask import session
from mongomoron import insert_one

from db import app_user, conn, ds, ds_list, ds_subscription
from app.subscription import create_subscription, list_ds_subscriptions, list_my_subscriptions, subscribe, send_test_subscription, unsubscribe, unsubscribe_all


def test_create_and_list_subscription(client):
    conn.db()[ds_subscription._name].delete_many({})
    user_id = ObjectId()
    with client.application.test_request_context('/subscribe', method='POST', json={
        'dsName': 'prices.csv',
        'query': {'category': 'books'},
        'fields': ['sku', 'shop'],
        'message': 'Found {$#} new prices, including {sku}: {$url}',
    }):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        response = create_subscription()

    assert response['success'] is True
    item = response['item']
    assert item['_id']
    assert item['dsName'] == 'prices.csv'
    assert item['subscribed'] is True

    with client.application.test_request_context('/ds/subscriptions?ds_name=prices.csv'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        list_response = list_ds_subscriptions()
    assert list_response['list'] == [item]

    with client.application.test_request_context('/subscriptions'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        my_response = list_my_subscriptions()
    assert my_response['list'] == [{
        '_id': item['_id'],
        'dsName': 'prices.csv',
        'query': {'category': 'books'},
        'fields': ['sku', 'shop'],
        'message': 'Found {$#} new prices, including {sku}: {$url}',
    }]


def test_subscribe_and_unsubscribe_existing_subscription(client):
    conn.db()[ds_subscription._name].delete_many({})
    subscription_id = ObjectId()
    user_id = ObjectId()
    conn.execute(insert_one(ds_subscription, {
        '_id': subscription_id,
        'dsName': 'prices.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{sku}',
        'userIds': [],
    }))
    with client.application.test_request_context('/subscribe/%s' % subscription_id, method='POST'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        assert subscribe(str(subscription_id)) == {'success': True}

    with client.application.test_request_context('/ds/subscriptions?ds_name=prices.csv'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        assert list_ds_subscriptions()['list'][0]['subscribed'] is True

    with client.application.test_request_context('/unsubscribe/%s' % subscription_id, method='POST'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        assert unsubscribe(str(subscription_id)) == {'success': True}
    assert conn.db()[ds_subscription._name].count_documents({}) == 0


def test_unsubscribe_all(client):
    conn.db()[ds_subscription._name].delete_many({})
    user_id = ObjectId()
    conn.execute(insert_one(ds_subscription, {
        'dsName': 'a.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{sku}',
        'userIds': [str(user_id)],
    }))
    conn.execute(insert_one(ds_subscription, {
        'dsName': 'b.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{sku}',
        'userIds': [str(user_id), str(ObjectId())],
    }))
    with client.application.test_request_context('/unsubscribe', method='POST'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        assert unsubscribe_all() == {'success': True}

    subscriptions = list(conn.db()[ds_subscription._name].find())
    assert len(subscriptions) == 1
    assert subscriptions[0]['dsName'] == 'b.csv'
    assert str(user_id) not in subscriptions[0]['userIds']


def test_create_subscription_rejects_operator_query(client):
    conn.db()[ds_subscription._name].delete_many({})
    with client.application.test_request_context('/subscribe', method='POST', json={
        'dsName': 'prices.csv',
        'query': {'price': {'$gt': 10}},
        'fields': ['sku'],
        'message': '{sku}',
    }):
        session['user'] = {'_id': str(ObjectId()), 'type': 'local'}
        try:
            create_subscription()
        except ValueError as e:
            assert str(e) == 'Only simple equality query is supported'
        else:
            assert False


def test_test_subscription_sends_message_to_current_user(client, monkeypatch):
    emails = []
    user_id = ObjectId()
    ds_id = ObjectId()
    subscription_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'extra': {'email': 'user@example.com'},
    }))
    conn.execute(insert_one(ds_list, {
        '_id': ds_id,
        'name': 'test-prices.csv',
        'status': 'active',
    }))
    conn.db()[ds[ds_id]._name].insert_one({
        '_id': 1,
        'sku': 'book-1',
    })
    conn.execute(insert_one(ds_subscription, {
        '_id': subscription_id,
        'dsName': 'test-prices.csv',
        'query': {},
        'fields': ['sku'],
        'message': 'Found {$#} test price for {sku}: {$url}',
        'userIds': [],
    }))
    monkeypatch.setattr('scheduler.tasks.notify_ds_subscribers.send_email',
                        lambda *args: emails.append(args))

    with client.application.test_request_context('/subscriptions/%s/test' % subscription_id, method='POST'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        assert send_test_subscription(str(subscription_id)) == {'success': True}

    assert emails == [(
        'user@example.com',
        'DS test-prices.csv has updates',
        'Found 1 test price for book-1: http://localhost/?id=%s' % ds_id,
    )]


def test_test_subscription_returns_404_for_empty_ds(client):
    user_id = ObjectId()
    ds_id = ObjectId()
    subscription_id = ObjectId()
    conn.execute(insert_one(ds_list, {
        '_id': ds_id,
        'name': 'empty.csv',
        'status': 'active',
    }))
    conn.execute(insert_one(ds_subscription, {
        '_id': subscription_id,
        'dsName': 'empty.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{sku}',
        'userIds': [],
    }))

    with client.application.test_request_context('/subscriptions/%s/test' % subscription_id, method='POST'):
        session['user'] = {'_id': str(user_id), 'type': 'local'}
        assert send_test_subscription(str(subscription_id)) == ({'error': 'DS is missing or empty: empty.csv'}, 404)
    conn.db()[ds_subscription._name].delete_many({})