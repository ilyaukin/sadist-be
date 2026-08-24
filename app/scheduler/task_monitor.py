import time

from app import logger
from scheduler.task_interface import find_timed_out_tasks


def run_monitor(on_timeout=None, poll_interval: int = 30):
    """Run monitor loop and process timed-out task workflows."""
    logger.info('Task monitor started')
    while True:
        check_timeouts(on_timeout=on_timeout)
        time.sleep(poll_interval)


def check_timeouts(on_timeout=None):
    """Find, mark, archive, and report timed-out task executions."""
    for workflow in find_timed_out_tasks():
        workflow.timeout()
        if on_timeout:
            on_timeout(workflow.task)
