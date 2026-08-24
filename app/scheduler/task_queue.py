import multiprocessing
import os
import signal
import time

from app import logger
from scheduler.task_monitor import check_timeouts
from scheduler.task_worker import run_worker


def run_queue(worker_count: int = 1, worker_poll_interval: int = 5,
              monitor_poll_interval: int = 30):
    workers = []
    for _ in range(worker_count):
        workers.append(_start_worker(worker_poll_interval))

    while True:
        _restart_finished_workers(workers, worker_poll_interval)
        check_timeouts(on_timeout=lambda task: _restart_timed_out_worker(
            workers, task.get('runBy'), worker_poll_interval))
        time.sleep(monitor_poll_interval)


def _start_worker(worker_poll_interval: int):
    worker = multiprocessing.Process(target=run_worker, args=(worker_poll_interval,))
    worker.start()
    logger.info('Started task worker %s', worker.pid)
    return worker


def _restart_finished_workers(workers: list[multiprocessing.Process],
                              worker_poll_interval: int):
    for i, worker in enumerate(workers):
        if not worker.is_alive():
            logger.warning('Task worker %s exited, restarting', worker.pid)
            workers[i] = _start_worker(worker_poll_interval)


def _restart_timed_out_worker(workers: list[multiprocessing.Process], run_by,
                              worker_poll_interval: int):
    for i, worker in enumerate(workers):
        if str(worker.pid) == str(run_by):
            logger.warning('Task worker %s timed out, terminating', worker.pid)
            os.kill(worker.pid, signal.SIGTERM)
            worker.join(timeout=5)
            if worker.is_alive():
                os.kill(worker.pid, signal.SIGKILL)
                worker.join(timeout=5)
            workers[i] = _start_worker(worker_poll_interval)
            return
