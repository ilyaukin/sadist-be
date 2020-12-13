import json
from unittest import TestCase

import mongomock
import pytest

from app import app, _db


def prepare_dataset():
    ds_collection_name = 'ds_1111'
    ds_classification_collection_name = 'ds_1111_classification'

    ds_collection = _db()[ds_collection_name]
    ds_classification_collection = _db()[ds_classification_collection_name]

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
        }
    ])
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
        }
    ])


@pytest.fixture
def client():
    return app.test_client()


@mongomock.patch(servers=(('localhost', 27017), ('localhost', 27018)))
def test_visualize(client):
    prepare_dataset()

    for iii in _db()['ds_1111_classification'].aggregate([
        {'$group': {'_id': '$row', 'colDet': {
            '$push': {'col': '$col', 'details': '$details'}}}},
        # here is mongomock bug ($arrayElemAt applied to result of $filter
        # does not work.. todo: fix bug and make this test work
        {'$project': {
            'Location': {'$arrayElemAt': [{
                '$filter': {
                    'input': '$colDet',
                    'cond': {'$literal': True}
                }
            }, 0]}
        }}

        # {'$project': {
        #     'Location': {'$arrayElemAt': [{'$filter': {'input': '$colDet',
        #                                                'cond': {
        #                                                    '$eq': ['$$this.col',
        #                                                            'Location']}}},
        #                                   0]}}},
        # {'$project': {'Location': '$Location.details.city'}},
        # {'$group': {'_id': '$Location', 'count': {'$sum': 1}}}
    ]):
        print(iii)

    result = client \
        .get('/ds/1111/visualize',
             query_string='pipeline='+json.dumps([{'col': 'Location', 'key': 'city'}])) \
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
        }
    ]
    TestCase().assertCountEqual(expected_list, result["list"])
