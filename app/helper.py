import multiprocessing
from _queue import Empty


def process_queue(q: multiprocessing.Queue):
    while True:
        try:
            yield q.get(timeout=0)
        except Empty:
            break
