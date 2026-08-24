import os
import time

from app import logger
from scheduler.task_interface import Task, claim_task
from scheduler.tasks import load_task_modules


def run_worker(poll_interval: int = 5):
    """Run task worker loop and execute claimed task workflows."""
    load_task_modules()
    run_by = os.getpid()
    logger.info('Task worker %s started', run_by)
    while True:
        workflow = claim_task(run_by)
        if not workflow:
            time.sleep(poll_interval)
            continue
        task = workflow.task
        task_handler = Task.get(task['taskType'])
        try:
            if not task_handler:
                raise RuntimeError('Unknown task type: %s' % task['taskType'])
            task_handler.execute(task.get('payload') or {})
            workflow.complete()
        except Exception as e:
            logger.exception('Task %s failed', task.get('_id'))
            workflow.fail(e)
