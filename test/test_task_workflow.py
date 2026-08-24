import datetime

import pytest

from db import conn, task_active, task_archive
from mongomoron import query, update
from scheduler.task_interface import create_task, Task, claim_task, \
    EXECUTION_TYPE_SINGLE, EXECUTION_TYPE_RECURRING, STATUS_PENDING, \
    STATUS_RUNNING, STATUS_WAITING, STATUS_PAUSED, STATUS_FAILED, \
    STATUS_TIMED_OUT
from scheduler.task_monitor import check_timeouts


@Task.sub('dummy')
class DummyTask(Task):
    def execute(self, payload: dict):
        pass


@pytest.fixture(autouse=True)
def clean_tasks():
    conn.db()[task_active._name].delete_many({})
    conn.db()[task_archive._name].delete_many({})


def test_single_task_fail():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_SINGLE)

    task_workflow = claim_task(1111)
    assert task_workflow
    task_workflow.fail(Exception('Deliberately failed'))

    task_in_task_active = _get_active_task(task_id)
    task_in_task_archive = _get_archive_task(task_id)
    assert task_in_task_active is None
    assert task_in_task_archive is not None
    assert task_in_task_archive['status'] == STATUS_FAILED
    assert task_in_task_archive['runBy'] == 1111
    assert 'error' in task_in_task_archive


def test_single_task_complete_deletes_active_without_archive():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_SINGLE)

    task_workflow = claim_task(1111)
    task_workflow.complete()

    assert _get_active_task(task_id) is None
    assert _get_archive_task(task_id) is None


def test_single_task_timeout_archives_and_deletes_active():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_SINGLE,
                          timeout=1)
    task_workflow = claim_task(1111)
    _make_timed_out(task_id)

    task_workflow.timeout()

    assert _get_active_task(task_id) is None
    archived_task = _get_archive_task(task_id)
    assert archived_task is not None
    assert archived_task['status'] == STATUS_TIMED_OUT
    assert 'timedOutAt' in archived_task


def test_monitor_times_out_task_in_one_step():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_SINGLE,
                          timeout=1)
    claim_task(1111)
    _make_timed_out(task_id)
    timed_out_tasks = []

    check_timeouts(on_timeout=timed_out_tasks.append)

    assert _get_active_task(task_id) is None
    assert _get_archive_task(task_id)['status'] == STATUS_TIMED_OUT
    assert [task['_id'] for task in timed_out_tasks] == [task_id]


def test_recurring_task_complete_moves_to_waiting_without_archive():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_RECURRING,
                          execution_interval=60)

    task_workflow = claim_task(1111)
    task_workflow.complete()

    active_task = _get_active_task(task_id)
    assert active_task['status'] == STATUS_WAITING
    assert 'runBy' not in active_task
    assert _get_archive_task(task_id) is None


def test_recurring_task_fail_updates_active_task_without_archive():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_RECURRING,
                          execution_interval=60)

    task_workflow = claim_task(1111)
    task_workflow.fail(Exception('Deliberately failed'))

    active_task = _get_active_task(task_id)
    assert active_task['status'] == STATUS_FAILED
    assert 'runBy' not in active_task
    assert 'error' in active_task
    assert 'failedAt' in active_task
    assert _get_archive_task(task_id) is None


def test_recurring_task_timeout_updates_active_task_without_archive():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_RECURRING,
                          execution_interval=60,
                          timeout=1)
    task_workflow = claim_task(1111)
    _make_timed_out(task_id)

    task_workflow.timeout()

    active_task = _get_active_task(task_id)
    assert active_task['status'] == STATUS_TIMED_OUT
    assert 'runBy' not in active_task
    assert 'timedOutAt' in active_task
    assert _get_archive_task(task_id) is None


def test_claim_ignores_not_due_paused_and_running_tasks():
    not_due_id = create_task(task_type='dummy',
                             execution_type=EXECUTION_TYPE_RECURRING,
                             execution_interval=60)
    paused_id = create_task(task_type='dummy',
                            execution_type=EXECUTION_TYPE_RECURRING,
                            execution_interval=60,
                            status=STATUS_PAUSED)
    running_id = create_task(task_type='dummy',
                             execution_type=EXECUTION_TYPE_SINGLE)
    claim_task(1111)
    _set_active_task(not_due_id, {
        'status': STATUS_WAITING,
        'runAt': datetime.datetime.now(),
    })
    _set_active_task(running_id, {'status': STATUS_RUNNING})

    assert claim_task(2222) is None
    assert _get_active_task(not_due_id)['status'] == STATUS_WAITING
    assert _get_active_task(paused_id)['status'] == STATUS_PAUSED
    assert _get_active_task(running_id)['status'] == STATUS_RUNNING


def test_claim_due_recurring_waiting_task():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_RECURRING,
                          execution_interval=60)
    _set_active_task(task_id, {
        'status': STATUS_WAITING,
        'runAt': datetime.datetime.now() - datetime.timedelta(seconds=61),
    })

    task_workflow = claim_task(1111)

    assert task_workflow.task['_id'] == task_id
    assert task_workflow.task['status'] == STATUS_RUNNING
    assert task_workflow.task['runBy'] == 1111


def test_new_recurring_pending_task_is_due_immediately():
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_RECURRING,
                          execution_interval=60,
                          status=STATUS_PENDING)

    task_workflow = claim_task(1111)

    assert task_workflow.task['_id'] == task_id
    assert task_workflow.task['status'] == STATUS_RUNNING


@pytest.mark.parametrize('status', [STATUS_FAILED, STATUS_TIMED_OUT])
def test_claim_due_recurring_failed_and_timed_out_tasks(status):
    task_id = create_task(task_type='dummy',
                          execution_type=EXECUTION_TYPE_RECURRING,
                          execution_interval=60)
    _set_active_task(task_id, {
        'status': status,
        'runAt': datetime.datetime.now() - datetime.timedelta(seconds=61),
    })

    task_workflow = claim_task(1111)

    assert task_workflow.task['_id'] == task_id
    assert task_workflow.task['status'] == STATUS_RUNNING
    assert task_workflow.task['runBy'] == 1111
    assert 'error' not in task_workflow.task
    assert 'failedAt' not in task_workflow.task
    assert 'timedOutAt' not in task_workflow.task


def _get_active_task(task_id):
    tasks = conn.execute(query(task_active).filter(task_active._id == task_id))
    return next(iter(tasks), None)


def _get_archive_task(task_id):
    tasks = conn.execute(query(task_archive).filter(task_archive._id == task_id))
    return next(iter(tasks), None)


def _set_active_task(task_id, changes: dict):
    conn.execute(
        update(task_active)
            .filter(task_active._id == task_id)
            .set(changes)
    )


def _make_timed_out(task_id):
    _set_active_task(task_id, {
        'runAt': datetime.datetime.now() - datetime.timedelta(seconds=2),
    })
