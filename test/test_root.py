import json
import os
from unittest import TestCase, mock

import mongomock
import pymongo
import pytest
from bson import ObjectId
from mongomoron import delete, insert_many, insert_one

from app import app
from db import conn, ds, ds_classification, ds_list

test_database_url = 'mongodb://localhost:27017,127.0.0.1:27018/test_sadist_be?replicaSet=rs0'
if os.getenv('USE_MONGOMOCK'):
    test_client = mongomock.MongoClient(test_database_url)
else:
    test_client = pymongo.MongoClient(test_database_url)
Patch = mock.patch.object(conn, 'mongo_client', lambda: test_client)


@pytest.fixture
@Patch
def dataset1():
    ds_id = ObjectId()
    ds_collection = ds[str(ds_id)]
    ds_classification_collection = ds_classification[str(ds_id)]

    conn.execute(
        delete(ds_list)
    )
    conn.execute(
        insert_one(ds_list, {
            '_id': ds_id,
            'name': '1111.csv',
            'extra': {
                'access': {'type': 'public'}
            }
        })
    )

    conn.execute(
        delete(ds_collection)
    )
    conn.execute(
        insert_many(
            ds_collection,
            [
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
            ]
        )
    )

    conn.execute(
        delete(ds_classification_collection)
    )
    conn.execute(
        insert_many(
            ds_classification_collection,

            [
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
            ]
        )
    )

    return {
        'ds_id': ds_id,
    }


@pytest.fixture
def client():
    return app.test_client()


@Patch
def test_visualize(client, dataset1):
    result = client \
        .get('/ds/%s/visualize' % dataset1['ds_id'],
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


@Patch
def test_filter(client, dataset1):
    result = client \
        .get('/ds/%s/filter' % dataset1['ds_id'],
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


@Patch
def test_filter_uncategorized(client, dataset1):
    result = client \
        .get('/ds/%s/filter' % dataset1['ds_id'],
             query_string='query=' + json.dumps(
                 [{'col': 'Location', 'key': 'city.name',
                   'values': [None]}])) \
        .get_json()
    assert isinstance(result, dict)
    assert result['success'] == True

    expected_list = [
        {
            'id': 5,
            'Location': '???',
            'Comment': '9999'
        },
    ]
    TestCase().assertCountEqual(expected_list, result['list'])
