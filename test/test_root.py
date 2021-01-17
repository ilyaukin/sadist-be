import json
import os
from unittest import TestCase, mock

import mongomock
import pymongo
import pytest

from app import app, _db

if os.getenv('USE_MONGOMOCK'):
    DB_PATCH = mongomock.patch(
        servers=(('localhost', 27017), ('localhost', 27018)))
else:
    test_client = pymongo.MongoClient(
        'mongodb://localhost:27017,127.0.0.1:27018/test_sadist_be?replicaSet=rs0')


    def _get_test_client(*args, **kwargs):
        return test_client


    DB_PATCH = mock.patch('pymongo.MongoClient', _get_test_client)


def prepare_dataset():
    ds_collection_name = 'ds_1111'
    ds_classification_collection_name = 'ds_1111_classification'

    ds_collection = _db()[ds_collection_name]
    ds_classification_collection = _db()[ds_classification_collection_name]

    ds_collection.delete_many({})
    ds_collection.insert_many([
        {
            '_id': 1,
            'Location': 'Moscow',
            'Comment': '1111'
        },
        {
            '_id': 2,
            'Location': 'Paris',
            'Comment': '2222'
        },
        {
            '_id': 3,
            'Location': 'MOscow',
            'Comment': '2344'
        },
        {
            '_id': 4,
            'Location': 'NYC',
            'Comment': '4444'
        },
        {
            '_id': 5,
            'Location': '???',
            'Comment': '9999'
        }
    ])
    ds_classification_collection.delete_many({})
    ds_classification_collection.insert_many([
        {
            '_id': 1,
            'col': 'Location',
            'row': 1,
            'details': {
                'city': {
                    'name': 'Moscow',
                    'coordinates': [37.61556, 55.75222]
                }
            }
        },
        {
            '_id': 2,
            'col': 'Location',
            'row': 2,
            'details': {
                'city': {
                    'name': 'Paris',
                    'coordinates': [2.3488, 48.85341]
                }
            }
        },
        {
            '_id': 3,
            'col': 'Location',
            'row': 3,
            'details': {
                'city': {
                    'name': 'Moscow',
                    'coordinates': [37.61556, 55.75222]
                }
            }
        },
        {
            '_id': 4,
            'col': 'Location',
            'row': 4,
            'details': {
                'city': {
                    'name': 'New York',
                    'coordinates': [-74.00597, 40.71427]
                }
            }
        },
        {
            '_id': 5,
            'col': 'Location',
            'row': 5
        }
    ])


@pytest.fixture
def client():
    return app.test_client()


@DB_PATCH
def test_visualize(client):
    prepare_dataset()

    result = client \
        .get('/ds/1111/visualize',
             query_string='pipeline=' + json.dumps(
                 [{'col': 'Location', 'key': 'city'}])) \
        .get_json()
    assert isinstance(result, dict)
    assert result['success'] == True
    expected_list = [
        {
            'id': {
                'name': 'Moscow',
                'coordinates': [37.61556, 55.75222]
            },
            'count': 2
        },
        {
            'id': {
                'name': 'Paris',
                'coordinates': [2.3488, 48.85341]
            },
            'count': 1
        },
        {
            'id': {
                'name': 'New York',
                'coordinates': [-74.00597, 40.71427]
            },
            'count': 1
        },
        {
            'id': None,
            'count': 1
        }
    ]
    TestCase().assertCountEqual(expected_list, result["list"])


@DB_PATCH
def test_filter(client):
    prepare_dataset()

    result = client \
        .get('/ds/1111/filter',
             query_string='query=' + json.dumps(
                 [{'col': 'Location', 'key': 'city.name',
                   'values': ['Moscow']}])) \
        .get_json()
    assert isinstance(result, dict)
    assert result['success'] == True

    expected_list = [
        {
            'id': 1,
            'Location': 'Moscow',
            'Comment': '1111'
        },
        {
            'id': 3,
            'Location': 'MOscow',
            'Comment': '2344'
        },
    ]
    TestCase().assertCountEqual(expected_list, result['list'])


@DB_PATCH
def test_filter_uncategorized(client):
    prepare_dataset()

    result = client \
        .get('/ds/1111/filter',
             query_string='query=' + json.dumps(
                 [{'col': 'Location', 'key': 'city.name',
                   'values': [None]}])) \
        .get_json()
    assert isinstance(result, dict)
    assert result['success'] == True

    expected_list = [
        {
            '_id': 5,
            'Location': '???',
            'Comment': '9999'
        },
    ]
    TestCase().assertCountEqual(expected_list, result['list'])
