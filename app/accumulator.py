from typing import Iterable

from mongomoron import sum_


class Accumulator(object):
    """
    An interface to define accumulators
    """

    def __init__(self, field=None):
        self.field = field

    def get_accumulator(self):
        """
        get mongo's accumulator
        """
        raise NotImplementedError('implement me!')

    def postprocess(self, result: Iterable[dict]):
        """
        process result in case it's easier to do in python code,
        rather than in mongo db.
        e.g. can use push_ as mongo's accumulator,
        and than process an array here
        @param result result of mongo's query, in form of [{_id: <id1>,
        <field._name>: <mongo's accumulated value1>, ...}, ...]
        """
        pass


class CountAccumulator(Accumulator):
    """
    Just calculate count of groups
    """

    def get_accumulator(self):
        return sum_(1)


def get_accumulator(accumulator: str, field):
    """
    Map accumulator passed from frontend to Accumulator class
    """
    acc_class = {
        # TODO
    }.get(accumulator)
    if acc_class is None:
        raise NotImplementedError(
            'Accumulator "' + accumulator + '" not implemented')
    return acc_class(field)
