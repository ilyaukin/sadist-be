import datetime
import traceback
from typing import Union, Optional

from bson import ObjectId
from pymongo import ReturnDocument
from mongomoron import query, update, delete, insert_one

from db import conn, task_active, task_archive
from singleton_mixin import SingletonMixin


EXECUTION_TYPE_SINGLE = 'single'
EXECUTION_TYPE_RECURRING = 'recurring'

STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_WAITING = 'waiting'
STATUS_PAUSED = 'paused'
STATUS_FAILED = 'failed'
STATUS_TIMED_OUT = 'timedOut'

DEFAULT_TIMEOUT = 3600
_CLAIMABLE_RECURRING_STATUSES = [
    STATUS_PENDING,
    STATUS_WAITING,
    STATUS_FAILED,
    STATUS_TIMED_OUT,
]
_TASK_MODULES_LOADED = False


class Task(SingletonMixin):
    """
    Base class for all scheduler task implementations.

    Task classes should be registered with ``@Task.sub('taskType')`` and
    implement ``execute``. Scheduler core ignores return values, so task
    implementations must persist all domain side effects themselves.
    """

    def validate(self, payload: dict):
        """
        Validate task payload before creating a task document.

        Raise an exception to reject invalid payload immediately.
        """
        pass

    def execute(self, payload: dict):
        """
        Execute task payload.

        Return value is ignored by the scheduler.
        """
        raise NotImplementedError()


class TaskWorkflow:
    """
    Workflow wrapper for a task DB record.

    It exposes task fields through ``task`` and provides state-transition
    methods used by workers and monitor code.
    """

    def __init__(self, task: dict):
        self.task = task

    def complete(self):
        """Mark successful task execution according to its execution type."""
        raise NotImplementedError()

    def fail(self, error: BaseException):
        """Archive failed execution details and finish current execution."""
        error_text = ''.join(traceback.format_exception(
            type(error), error, error.__traceback__))
        self._archive_and_finish({
            'status': STATUS_FAILED,
            'error': error_text,
            'failedAt': datetime.datetime.now(),
        })

    def timeout(self):
        """Archive timed-out execution details and finish current execution."""
        self._archive_and_finish({
            'status': STATUS_TIMED_OUT,
            'timedOutAt': datetime.datetime.now(),
        })

    def _archive_and_finish(self, extra: dict):
        _archive_and_finish_task(self.task, extra)


class SingleTaskWorkflow(TaskWorkflow):
    """Workflow implementation for one-time tasks."""

    def complete(self):
        """Delete successfully completed one-time task from active queue."""
        _delete_active_task(self.task['_id'])


class RecurringTaskWorkflow(TaskWorkflow):
    """Workflow implementation for interval-based recurring tasks."""

    def fail(self, error: BaseException):
        """Store failed execution details in active recurring task."""
        error_text = ''.join(traceback.format_exception(
            type(error), error, error.__traceback__))
        self._finish_with_status({
            'status': STATUS_FAILED,
            'error': error_text,
            'failedAt': datetime.datetime.now(),
        })

    def timeout(self):
        """Store timed-out execution details in active recurring task."""
        self._finish_with_status({
            'status': STATUS_TIMED_OUT,
            'timedOutAt': datetime.datetime.now(),
        })

    def complete(self):
        """Move successfully executed recurring task back to waiting state."""
        self._move_to_waiting()

    def _finish_with_status(self, changes: dict):
        conn.execute(
            update(task_active)
                .filter(task_active._id == self.task['_id'])
                .set(changes)
                .unset('runBy')
        )

    def _move_to_waiting(self):
        conn.execute(
            update(task_active)
                .filter(task_active._id == self.task['_id'])
                .set({
                    'status': STATUS_WAITING,
                })
                .unset('runBy', 'error', 'failedAt', 'timedOutAt')
        )


def create_task(task_type: str, execution_type: str, payload: dict = None,
                timeout: int = DEFAULT_TIMEOUT,
                execution_interval: int = None,
                status: str = STATUS_PENDING) -> ObjectId:
    """
    Validate payload, create task document, and insert it into active queue.

    Returns inserted task id. Invalid task type, payload, or execution settings
    raise immediately and no DB record is created.
    """
    _ensure_task_modules_loaded()
    payload = payload or {}
    task = Task.get(task_type)
    if not task:
        raise ValueError('Unknown task type: %s' % task_type)
    task.validate(payload)
    document = _create_task_document(
        task_type=task_type,
        execution_type=execution_type,
        payload=payload,
        timeout=timeout,
        execution_interval=execution_interval,
        status=status,
    )
    return conn.execute(insert_one(task_active, document)).inserted_id


def claim_task(run_by: Union[int, str]) -> Optional[TaskWorkflow]:
    """Claim the next due task and return its workflow wrapper."""
    now = datetime.datetime.now()
    collection = conn.db()[task_active._name]

    task = _claim_oldest_pending_task(collection, run_by, now)
    if task:
        return _build_workflow(task)

    task = _claim_due_recurring_task(collection, run_by, now)
    if task:
        return _build_workflow(task)
    return None


def find_timed_out_tasks() -> list[TaskWorkflow]:
    """Find running task workflows whose configured timeout has expired."""
    now = datetime.datetime.now()
    # TODO: implement reusable date arithmetic query helpers in mongomoron and
    # use a DB-side `runAt + timeout <= now` query here.
    tasks = conn.execute(
        query(task_active).filter(task_active.status == STATUS_RUNNING))
    return [_build_workflow(task) for task in tasks if
            task.get('runAt') and task.get('timeout') and
            task['runAt'] + datetime.timedelta(seconds=task['timeout']) <= now]


def _create_task_document(task_type: str, execution_type: str, payload: dict,
                          timeout: int = DEFAULT_TIMEOUT,
                          execution_interval: int = None,
                          status: str = STATUS_PENDING) -> dict:
    _validate_execution_settings(execution_type, execution_interval)
    document = {
        'taskType': task_type,
        'executionType': execution_type,
        'payload': payload,
        'status': status,
        'timeout': timeout,
    }
    if execution_type == EXECUTION_TYPE_RECURRING:
        document['executionInterval'] = execution_interval
    return document


def _ensure_task_modules_loaded():
    global _TASK_MODULES_LOADED
    if _TASK_MODULES_LOADED:
        return
    from scheduler.tasks import load_task_modules
    load_task_modules()
    _TASK_MODULES_LOADED = True


def _validate_execution_settings(execution_type: str, execution_interval: int):
    if execution_type not in [EXECUTION_TYPE_SINGLE, EXECUTION_TYPE_RECURRING]:
        raise ValueError('Unknown execution type: %s' % execution_type)
    if execution_type == EXECUTION_TYPE_RECURRING and not execution_interval:
        raise ValueError('Recurring task requires execution interval')
    if execution_type == EXECUTION_TYPE_SINGLE and execution_interval is not None:
        raise ValueError('Single task must not have execution interval')


def _build_workflow(task: dict) -> TaskWorkflow:
    if task['executionType'] == EXECUTION_TYPE_SINGLE:
        return SingleTaskWorkflow(task)
    if task['executionType'] == EXECUTION_TYPE_RECURRING:
        return RecurringTaskWorkflow(task)
    raise ValueError('Unknown execution type: %s' % task['executionType'])


def _claim_oldest_pending_task(collection, run_by: Union[int, str],
                               now: datetime.datetime) -> Optional[dict]:
    single_task = _find_oldest_task(EXECUTION_TYPE_SINGLE, STATUS_PENDING)
    recurring_task = _find_oldest_due_recurring_task(now)
    task = _choose_oldest_task(single_task, recurring_task)
    if not task:
        return None
    # TODO: implement atomic find_one_and_update in mongomoron and use it here.
    return collection.find_one_and_update(
        {'_id': task['_id'], 'status': task['status']},
        _claim_update(run_by, now),
        return_document=ReturnDocument.AFTER,
    )


def _claim_due_recurring_task(collection, run_by: Union[int, str],
                              now: datetime.datetime) -> Optional[dict]:

    # TODO: implement reusable date arithmetic query helpers in mongomoron and
    # use an atomic due-recurring query instead of candidate iteration.
    tasks = conn.execute(
        query(task_active)
            .filter(task_active.executionType == EXECUTION_TYPE_RECURRING)
            .filter(task_active.status.in_(_CLAIMABLE_RECURRING_STATUSES))
            .sort((task_active._createdAt, 1))
    )
    for task in tasks:
        if not _is_recurring_due(task, now):
            continue
        claimed = collection.find_one_and_update(
            {'_id': task['_id'], 'status': task['status']},
            _claim_update(run_by, now),
            return_document=ReturnDocument.AFTER,
        )
        if claimed:
            return claimed
    return None


def _find_oldest_task(execution_type: str, status: str) -> Optional[dict]:
    tasks = conn.execute(
        query(task_active)
            .filter(task_active.executionType == execution_type)
            .filter(task_active.status == status)
            .sort((task_active._createdAt, 1))
    )
    return next(iter(tasks), None)


def _find_oldest_due_recurring_task(now: datetime.datetime) -> Optional[dict]:
    tasks = conn.execute(
        query(task_active)
            .filter(task_active.executionType == EXECUTION_TYPE_RECURRING)
            .filter(task_active.status.in_(_CLAIMABLE_RECURRING_STATUSES))
            .sort((task_active._createdAt, 1))
    )
    return next((task for task in tasks if _is_recurring_due(task, now)), None)


def _choose_oldest_task(*tasks: Optional[dict]) -> Optional[dict]:
    tasks = [task for task in tasks if task]
    if not tasks:
        return None
    return min(tasks, key=lambda task: task.get('_createdAt') or datetime.datetime.min)


def _claim_update(run_by: Union[int, str], now: datetime.datetime) -> dict:
    return {
        '$set': {
            'status': STATUS_RUNNING,
            'runBy': run_by,
            'runAt': now,
        },
        '$unset': {
            'error': '',
            'failedAt': '',
            'timedOutAt': '',
        },
    }


def _is_recurring_due(task: dict, now: datetime.datetime) -> bool:
    run_at = task.get('runAt')
    if not run_at:
        return True
    return run_at + datetime.timedelta(seconds=task.get('executionInterval', 0)) <= now


def _archive_and_finish_task(task: dict, extra: dict):
    # TODO: implement transactions in mongomoron and use them here.
    archived = dict(task)
    archived.update(extra or {})
    _delete_active_task(task['_id'])
    conn.execute(insert_one(task_archive, archived))


def _delete_active_task(task_id: ObjectId):
    q = delete(task_active)
    q.filter(task_active._id == task_id)
    conn.execute(q)
