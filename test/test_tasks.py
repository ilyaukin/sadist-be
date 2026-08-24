from bson import ObjectId
from mongomoron import insert_one, query

from app import root
from db import conn, ds, ds_classification, ds_list, task_active
from scheduler.task_interface import EXECUTION_TYPE_SINGLE
from scheduler.tasks.cleanup_old_ds import CleanupOldDsTask
from scheduler.tasks.classify_ds import ClassifyDsTask
from scheduler.tasks.detailize_cols import DetailizeColsTask


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


def _get_active_task(task_type: str):
    return next(iter(conn.execute(
        query(task_active).filter(task_active.taskType == task_type))), None)


def _get_ds_list_record(ds_id):
    return next(iter(conn.execute(query(ds_list).filter(ds_list._id == ds_id))))


def _collection_exists(name: str):
    return name in conn.db().list_collection_names()


def _create_collection(name: str):
    conn.db().create_collection(name)