import atexit
from concurrent.futures._base import Future
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable

_tp = ThreadPoolExecutor(max_workers=1)


def _shutdown():
    _tp.shutdown()

atexit.register(_shutdown)


def call_async(fn: Callable, *args, **kwargs) -> 'Future':
    return _tp.submit(fn, *args, **kwargs)
