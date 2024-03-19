import collections.abc
import json
from typing import Iterable, Callable


class objectset(collections.abc.MutableSet):
    """
    Set implementation to be able to work with sets of unhashable
    objects, namely lists and dicts. So, almost any object from our DB
    can be used here as value.
    It's implemented by wrapping a key via json.dumps - maybe
    not very efficient, but easy to implement way.
    For the correct behaviour, values should not be edited after
    adding to this collection
    """

    def __init__(self, init_v: Iterable = None):
        self._wrap = json.dumps
        self._wrapped = dict((self._wrap(k), k) for k in init_v) \
            if init_v else dict()

    def __contains__(self, item):
        return self._wrapped.__contains__(self._wrap(item))

    def __iter__(self):
        return self._wrapped.values().__iter__()

    def __len__(self):
        return self._wrapped.__len__()

    def add(self, value):
        self._wrapped[self._wrap(value)] = value

    def discard(self, value):
        del self._wrapped[self._wrap(value)]
