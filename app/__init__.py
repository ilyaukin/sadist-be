import inspect
import os

from flask import Flask
from pymongo import MongoClient
from pymongo.database import Database

app = Flask(__name__)
app.logger.setLevel('DEBUG')

# global connection (will do things maximally straightforward
# until meet the problems)

mongo_client_pool = dict()


def _mongo_client():
    pid = os.getpid()
    mongo_client_pool.setdefault(pid,
                                 MongoClient('mongodb://localhost:27017,127.0.0.1:27018/?replicaSet=rs0'))
    return mongo_client_pool[pid]


def _db() -> Database:
    return _mongo_client().get_database('sadist')


def _set(d: dict):
    """
    just a shortcut for mongo updates
    :param d: fields to be updated
    :return: `{"$set": d}` to pass as the second param of `update`
    """
    return {'$set': d}


def transactional(foo):
    def foo_in_transaction(*args, **kwargs):
        session = _mongo_client().start_session()
        session.start_transaction()
        try:
            if 'session' in inspect.getargspec(foo).args:
                kwargs['session'] = session
            result = foo(*args, **kwargs)
            session.commit_transaction()
            return result
        except Exception as e:
            session.abort_transaction()
            raise e

    foo_in_transaction.__name__ = foo.__name__
    return foo_in_transaction


from . import root, labelling