from bson import ObjectId
from mongomoron import insert_one, query

from app import root
from db import app_user, conn, ds, ds_classification, ds_list, ds_subscription, task_active
from scheduler.task_interface import EXECUTION_TYPE_SINGLE
from scheduler.tasks.cleanup_old_ds import CleanupOldDsTask
from scheduler.tasks.classify_ds import ClassifyDsTask
from scheduler.tasks.detailize_cols import DetailizeColsTask
from scheduler.tasks.notify_ds_subscribers import NotifyDsSubscribersTask


class DummyDetailizer:
    labels = ['number']


def test_process_ds_creates_classification_task():
    ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': ds_id, 'name': 'test.csv'}))

    root._process_ds(ds_id)

    task = _get_active_task('classify_ds')
    assert task['taskType'] == 'classify_ds'
    assert task['executionType'] == EXECUTION_TYPE_SINGLE
    assert task['payload'] == {
        'dsId': str(ds_id),
        'classifier': 'SequenceClassifier',
    }


def test_process_ds_creates_cleanup_old_ds_task():
    ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': ds_id, 'name': 'test.csv'}))

    root._process_ds(ds_id)

    task = _get_active_task('cleanup_old_ds')
    assert task['executionType'] == EXECUTION_TYPE_SINGLE
    assert task['payload'] == {
        'name': 'test.csv',
        'keep': 1,
    }


def test_process_ds_creates_notify_ds_subscribers_task(client):
    ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': ds_id, 'name': 'test.csv'}))

    with client.application.test_request_context(base_url='http://localhost/'):
        root._process_ds(ds_id)

    task = _get_active_task('notify_ds_subscribers', {'payload.dsId': str(ds_id)})
    assert task['executionType'] == EXECUTION_TYPE_SINGLE
    assert task['payload'] == {
        'dsId': str(ds_id),
        'dsName': 'test.csv',
        'baseUrl': 'http://localhost',
    }


def test_cleanup_old_ds_keeps_latest_old_records():
    old_ids = [ObjectId(), ObjectId(), ObjectId()]
    active_id = ObjectId()
    for ds_id in old_ids:
        conn.execute(insert_one(ds_list, {
            '_id': ds_id,
            'name': 'test.csv',
            'status': 'old',
        }))
        _create_collection(ds[ds_id]._name)
        _create_collection(ds_classification[ds_id]._name)
    conn.execute(insert_one(ds_list, {
        '_id': active_id,
        'name': 'test.csv',
        'status': 'active',
    }))
    _create_collection(ds[active_id]._name)
    _create_collection(ds_classification[active_id]._name)

    CleanupOldDsTask().execute({'name': 'test.csv', 'keep': 1})

    remaining_old_records = list(conn.execute(
        query(ds_list)
            .filter(ds_list.name == 'test.csv')
            .filter(ds_list.status == 'old')
    ))
    assert len(remaining_old_records) == 1
    assert _collection_exists(ds[remaining_old_records[0]['_id']]._name)
    assert _collection_exists(ds_classification[remaining_old_records[0]['_id']]._name)
    assert _get_ds_list_record(active_id)
    assert _collection_exists(ds[active_id]._name)
    assert _collection_exists(ds_classification[active_id]._name)


def test_classify_ds_executes_classifier_and_creates_detailization_task(monkeypatch):
    created_tasks = []
    classified = []
    ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': ds_id}))

    monkeypatch.setattr('scheduler.tasks.classify_ds.classify_cells',
                        lambda *args: classified.append(args))
    monkeypatch.setattr('scheduler.tasks.classify_ds.create_task',
                        lambda **kwargs: created_tasks.append(kwargs))

    ClassifyDsTask().execute({'dsId': str(ds_id),
                              'classifier': 'SequenceClassifier'})

    assert classified[0][0] == str(ds_id)
    assert created_tasks == [{
        'task_type': 'detailize_cols',
        'execution_type': EXECUTION_TYPE_SINGLE,
        'payload': {'dsId': str(ds_id)},
        'timeout': 3600,
    }]


def test_classify_ds_updates_status_on_error(monkeypatch):
    ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': ds_id}))

    def raise_error(*args):
        raise RuntimeError('classification failed')

    monkeypatch.setattr('scheduler.tasks.classify_ds.classify_cells', raise_error)

    try:
        ClassifyDsTask().execute({'dsId': str(ds_id),
                                  'classifier': 'SequenceClassifier'})
    except RuntimeError:
        pass

    ds_record = _get_ds_list_record(ds_id)
    assert ds_record['classification'] == {
        'status': 'failed',
        'error': 'classification failed',
    }


def test_detailize_cols_executes_each_selected_detailizer(monkeypatch):
    calls = []
    ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': ds_id}))

    monkeypatch.setattr('scheduler.tasks.detailize_cols._iter_detailization_tasks',
                        lambda ds_id: [('amount', [], DummyDetailizer())])
    monkeypatch.setattr('scheduler.tasks.detailize_cols.get_details_for_cells',
                        lambda *args: calls.append(args))

    DetailizeColsTask().execute({'dsId': str(ds_id)})

    assert calls[0][0:2] == (str(ds_id), 'amount')
    assert _get_ds_list_record(ds_id)['detailization']['amount']['status'] == 'finished'


def test_detailize_cols_updates_status_on_error(monkeypatch):
    ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': ds_id}))

    def raise_error(*args):
        raise RuntimeError('detailization failed')

    monkeypatch.setattr('scheduler.tasks.detailize_cols._iter_detailization_tasks',
                        lambda ds_id: [('amount', [], DummyDetailizer())])
    monkeypatch.setattr('scheduler.tasks.detailize_cols.get_details_for_cells',
                        raise_error)

    try:
        DetailizeColsTask().execute({'dsId': str(ds_id)})
    except RuntimeError:
        pass

    detailization = _get_ds_list_record(ds_id)['detailization']['amount']
    assert detailization == {
        'status': 'failed',
        'error': 'detailization failed',
    }


def test_notify_ds_subscribers_sends_one_message_for_new_documents(monkeypatch):
    emails = []
    user_id = ObjectId()
    old_ds_id = ObjectId()
    new_ds_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'extra': {'email': 'user@example.com'},
    }))
    conn.execute(insert_one(ds_list, {
        '_id': old_ds_id,
        'name': 'prices.csv',
        'status': 'old',
    }))
    conn.execute(insert_one(ds_list, {
        '_id': new_ds_id,
        'name': 'prices.csv',
        'status': 'active',
    }))
    conn.execute(insert_one(ds_subscription, {
        'dsName': 'prices.csv',
        'query': {'category': 'books'},
        'fields': ['sku'],
        'message': 'Found {$#} new price for {sku}: {$url}',
        'userIds': [str(user_id)],
    }))
    conn.db()[ds[old_ds_id]._name].insert_many([
        {'_id': 1, 'category': 'books', 'sku': 'old'},
    ])
    conn.db()[ds[new_ds_id]._name].insert_many([
        {'_id': 1, 'category': 'books', 'sku': 'old'},
        {'_id': 2, 'category': 'books', 'sku': 'new-1'},
        {'_id': 3, 'category': 'books', 'sku': 'new-2'},
        {'_id': 4, 'category': 'games', 'sku': 'ignored'},
    ])
    monkeypatch.setattr('scheduler.tasks.notify_ds_subscribers.send_email',
                        lambda *args: emails.append(args))

    NotifyDsSubscribersTask().execute({
        'dsId': str(new_ds_id),
        'dsName': 'prices.csv',
        'baseUrl': 'http://sadist.test',
    })

    assert emails == [(
        'user@example.com',
        'DS prices.csv has updates',
        'Found 2 new price for new-1: http://sadist.test/?id=%s' % new_ds_id,
    )]


def test_notify_ds_subscribers_skips_user_with_empty_channels(monkeypatch):
    emails = []
    user_id = ObjectId()
    old_ds_id = ObjectId()
    new_ds_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'extra': {'email': 'user@example.com'},
        'settings': {'notificationChannels': []},
    }))
    conn.execute(insert_one(ds_list, {'_id': old_ds_id, 'name': 'prices.csv', 'status': 'old'}))
    conn.execute(insert_one(ds_list, {'_id': new_ds_id, 'name': 'prices.csv', 'status': 'active'}))
    conn.execute(insert_one(ds_subscription, {
        'dsName': 'prices.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{sku}',
        'userIds': [str(user_id)],
    }))
    conn.db()[ds[old_ds_id]._name].insert_one({'_id': 1, 'sku': 'old'})
    conn.db()[ds[new_ds_id]._name].insert_one({'_id': 2, 'sku': 'new'})
    monkeypatch.setattr('scheduler.tasks.notify_ds_subscribers.send_email',
                        lambda *args: emails.append(args))

    NotifyDsSubscribersTask().execute({
        'dsId': str(new_ds_id),
        'dsName': 'prices.csv',
        'baseUrl': 'http://sadist.test',
    })

    assert emails == []


def test_notify_ds_subscribers_uses_default_email_channel(monkeypatch):
    emails = []
    user_id = ObjectId()
    old_ds_id = ObjectId()
    new_ds_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'extra': {'email': 'user@example.com'},
        'settings': {},
    }))
    conn.execute(insert_one(ds_list, {'_id': old_ds_id, 'name': 'prices.csv', 'status': 'old'}))
    conn.execute(insert_one(ds_list, {'_id': new_ds_id, 'name': 'prices.csv', 'status': 'active'}))
    conn.execute(insert_one(ds_subscription, {
        'dsName': 'prices.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{sku}',
        'userIds': [str(user_id)],
    }))
    conn.db()[ds[old_ds_id]._name].insert_one({'_id': 1, 'sku': 'old'})
    conn.db()[ds[new_ds_id]._name].insert_one({'_id': 2, 'sku': 'new'})
    monkeypatch.setattr('scheduler.tasks.notify_ds_subscribers.send_email',
                        lambda *args: emails.append(args))

    NotifyDsSubscribersTask().execute({
        'dsId': str(new_ds_id),
        'dsName': 'prices.csv',
        'baseUrl': 'http://sadist.test',
    })

    assert emails == [('user@example.com', 'DS prices.csv has updates', 'new')]


def test_notify_ds_subscribers_raises_key_error_for_missing_message_field():
    user_id = ObjectId()
    old_ds_id = ObjectId()
    new_ds_id = ObjectId()
    conn.execute(insert_one(app_user, {
        '_id': user_id,
        'extra': {'email': 'user@example.com'},
    }))
    conn.execute(insert_one(ds_list, {'_id': old_ds_id, 'name': 'prices.csv', 'status': 'old'}))
    conn.execute(insert_one(ds_list, {'_id': new_ds_id, 'name': 'prices.csv', 'status': 'active'}))
    conn.execute(insert_one(ds_subscription, {
        'dsName': 'prices.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{missing}',
        'userIds': [str(user_id)],
    }))
    conn.db()[ds[old_ds_id]._name].insert_one({'_id': 1, 'sku': 'old'})
    conn.db()[ds[new_ds_id]._name].insert_one({'_id': 2, 'sku': 'new'})

    try:
        NotifyDsSubscribersTask().execute({
            'dsId': str(new_ds_id),
            'dsName': 'prices.csv',
            'baseUrl': 'http://sadist.test',
        })
    except KeyError as e:
        assert e.args == ('missing',)
    else:
        assert False


def test_notify_ds_subscribers_skips_without_previous_old_ds(monkeypatch):
    emails = []
    new_ds_id = ObjectId()
    conn.execute(insert_one(ds_list, {'_id': new_ds_id, 'name': 'no-old.csv', 'status': 'active'}))
    conn.execute(insert_one(ds_subscription, {
        'dsName': 'no-old.csv',
        'query': {},
        'fields': ['sku'],
        'message': '{sku}',
        'userIds': [str(ObjectId())],
    }))
    conn.db()[ds[new_ds_id]._name].insert_one({'_id': 1, 'sku': 'new'})
    monkeypatch.setattr('scheduler.tasks.notify_ds_subscribers.send_email',
                        lambda *args: emails.append(args))

    NotifyDsSubscribersTask().execute({
        'dsId': str(new_ds_id),
        'dsName': 'no-old.csv',
        'baseUrl': 'http://sadist.test',
    })

    assert emails == []


def _get_active_task(task_type: str, filter_=None):
    return conn.db()[task_active._name].find_one({'taskType': task_type, **(filter_ or {})})


def _get_ds_list_record(ds_id):
    return next(iter(conn.execute(query(ds_list).filter(ds_list._id == ds_id))))


def _collection_exists(name: str):
    return name in conn.db().list_collection_names()


def _create_collection(name: str):
    conn.db().create_collection(name)