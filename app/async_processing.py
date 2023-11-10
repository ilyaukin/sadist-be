import faulthandler
import multiprocessing
import os
import signal
import traceback
from _queue import Empty
from typing import Iterable, Callable, Tuple, Any, Optional, List

from app import logger


def process_in_parallel(input: Iterable, processor: Callable,
                        args: Tuple, timeout: int = 60) -> Iterable:
    """
    Processes tasks in async (parallel) manner, each task from `input`
    will be put into the queue, optimal count of processes will be launched,
    processes will call `processor` for each
    task in the queue, with arguments <task>, *`args`,
    in case of success the the function
    will return iterable of the same length than `input` with processing
    results.
    In case that processor raised an exception for at least one task,
    the processing considering unsuccessful and exception is thrown.
    In case no new result returns within `timeout` seconds, the processing is
    considering unsuccessful and exception is thrown, remaining processes,
    if any, are killed.
    This function itself synchronizes output, i.e. blocks execution while
    all tasks are processed or error is occurred or timeout is happened.
    """

    input_queue = multiprocessing.Queue()
    input_count = 0
    output_queue = multiprocessing.Queue()
    output_count = 0

    for task in input:
        input_queue.put(task)
        input_count += 1

    processes: List[multiprocessing.Process] = []
    for _ in range(MAX_PROCESS_COUNT):
        p = multiprocessing.Process(target=_process, args=(
            input_queue, output_queue, processor, *args))
        p.start()
        processes.append(p)

    def _cleanup():
        pp = [p for p in processes if p.is_alive()]
        for p in pp:
            logger.warn('Process %d is still alive, killing...', p.pid)
            # first try to gracefully terminate, to be able to see traceback
            p.terminate()
            p.join(timeout=1)
            if p.exitcode is None:
                # not stopped within 1 second, kill
                os.kill(p.pid, signal.SIGKILL)

    while output_count < input_count:
        try:
            result: _Result = output_queue.get(timeout=timeout)
            output_count += 1
            if result.success:
                yield result.output
            else:
                error_text = """Exception of async processing:
%s""" % result.exc
                logger.warn(error_text)
                _cleanup()
                raise Exception(error_text)
        except Empty:
            error_text = """Timeout %ds of async processing.
Only %d of %d tasks are completed.""" % (timeout, output_count, input_count)
            logger.warn(error_text)
            _cleanup()
            raise Exception(error_text) from None

    _cleanup()


class _Result(object):
    def __init__(self, success: bool, output: Optional[Any] = None,
                 exc: Optional[str] = None):
        self.success = success
        self.output = output
        self.exc = exc

    @staticmethod
    def success(output: Any):
        return _Result(success=True, output=output)

    @staticmethod
    def error(exc: str):
        return _Result(success=False, exc=exc)


def _process(input_queue: multiprocessing.Queue,
             output_queue: multiprocessing.Queue, processor: Callable, *args):
    try:
        faulthandler.register(signum=signal.SIGTERM)
        while input_queue.qsize():
            try:
                task = input_queue.get(timeout=0)
            except Empty:
                continue
            output = processor(task, *args)
            output_queue.put(_Result.success(output))
    except:
        output_queue.put(_Result.error(traceback.format_exc()))
    finally:
        logger.info('Process %d has terminated normally', os.getpid())


MAX_PROCESS_COUNT = os.cpu_count()
