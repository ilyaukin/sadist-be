import atexit
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable

_tp = ThreadPoolExecutor(max_workers=1)


def _shutdown():
    _tp.shutdown()

atexit.register(_shutdown)


def call_async(fn: Callable, *args, **kwargs):
    _tp.submit(fn, *args, **kwargs)
