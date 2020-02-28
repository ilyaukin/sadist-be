import inspect

from flask import Flask
from pymongo import MongoClient

app = Flask(__name__)
app.logger.setLevel('DEBUG')

# global connection (will do things maximally straightforward
# until meet the problems)

global mongo_client
mongo_client = MongoClient('mongodb://localhost:27017,127.0.0.1:27018/?replicaSet=rs0')


def _db():
    return mongo_client.get_database('sadist')


def _set(d: dict):
    """
    just a shortcut for mongo updates
    :param d: fields to be updated
    :return: `{"$set": d}` to pass as the second param of `update`
    """
    return {'$set': d}


def transactional(foo):
    def foo_in_transaction(*args, **kwargs):
        session = mongo_client.start_session()
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