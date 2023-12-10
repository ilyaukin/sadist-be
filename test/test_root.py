import json
import logging
import os
from typing import Iterable, Any
from unittest import TestCase, mock

import mongomock
import pymongo
import pytest
from bson import ObjectId
from mongomoron import delete, insert_many, insert_one

from app import app
from db import conn, ds, ds_classification, ds_list, geo_city

test_database_url = 'mongodb://localhost:27017,127.0.0.1:27018/test_sadist_be?replicaSet=rs0'
if os.getenv('USE_MONGOMOCK'):
    test_client = mongomock.MongoClient(test_database_url)
else:
    test_client = pymongo.MongoClient(test_database_url)
Patch = mock.patch.object(conn, 'mongo_client', lambda: test_client)


def insert_cities():
    conn.execute(delete(geo_city))
    conn.execute(insert_many(geo_city, [
        {
            '_id': '1',
            'name': 'Moscow',
            'loc': {
                'type': 'Point',
                'coordinates': [37.61556, 55.75222],
            }
        },
        {
            '_id': '2',
            'name': 'Paris',
            'loc': {
                'type': 'Point',
                'coordinates': [2.3488, 48.85341],
            }
        },
        {
            '_id': '3',
            'name': 'New York',
            'loc': {
                'type': 'Point',
                'coordinates': [-74.00597, 40.71427],
            }
        },
    ]))


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
            'status': 'active',
            'extra': {
                'access': {'type': 'public'}
            },
            'cols': ['Location', 'Comment'],
            'detailization': {
                'Location': {
                    'labels': ['city']
                }
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
                            'id': '1'
                        }
                    }
                },
                {
                    '_id': 2,
                    'col': 'Location',
                    'row': 2,
                    'details': {
                        'city': {
                            'id': '2'
                        }
                    }
                },
                {
                    '_id': 3,
                    'col': 'Location',
                    'row': 3,
                    'details': {
                        'city': {
                            'id': '1'
                        }
                    }
                },
                {
                    '_id': 4,
                    'col': 'Location',
                    'row': 4,
                    'details': {
                        'city': {
                            'id': '3'
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

    insert_cities()

    return {
        'ds_id': ds_id,
    }


@pytest.fixture
@Patch
def dataset2():
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
                    'Profession': 'Architest',
                    'Comment': '1111'
                },
                {
                    '_id': 2,
                    'Location': 'Paris',
                    'Profession': 'Soution architest',
                    'Comment': '2222'
                },
                {
                    '_id': 3,
                    'Location': 'MOscow',
                    'Profession': 'Frontend developer',
                    'Comment': '2344'
                },
                {
                    '_id': 4,
                    'Location': 'MOscow',
                    'Profession': 'Backend developer',
                    'Comment': '2344'
                },
                {
                    '_id': 5,
                    'Location': 'NYC',
                    'Profession': 'backend',
                    'Comment': '4444'
                },
                {
                    '_id': 6,
                    'Location': 'Msk',
                    'Profession': 'Java Backend developer',
                    'Comment': ''
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
                            'id': '1'
                        }
                    }
                },
                {
                    '_id': 2,
                    'col': 'Location',
                    'row': 2,
                    'details': {
                        'city': {
                            'id': '2'
                        }
                    }
                },
                {
                    '_id': 3,
                    'col': 'Location',
                    'row': 3,
                    'details': {
                        'city': {
                            'id': '1'
                        }
                    }
                },
                {
                    '_id': 4,
                    'col': 'Location',
                    'row': 4,
                    'details': {
                        'city': {
                            'id': '1'
                        }
                    }
                },
                {
                    '_id': 5,
                    'col': 'Location',
                    'row': 5,
                    'details': {
                        'city': {
                            'id': '3'
                        }
                    }
                },
                {
                    '_id': 6,
                    'col': 'Location',
                    'row': 6,
                    'details': {
                        'city': {
                            'id': '1'
                        }
                    }
                },
                {
                    '_id': 7,
                    'col': 'Profession',
                    'row': 1,
                    'details': {
                        'profession': {
                            'role': 'System Architest'
                        }
                    }
                },
                {
                    '_id': 8,
                    'col': 'Profession',
                    'row': 2,
                    'details': {
                        'profession': {
                            'role': 'System Architest'
                        }
                    }
                },
                {
                    '_id': 9,
                    'col': 'Profession',
                    'row': 3,
                    'details': {
                        'profession': {
                            'role': 'Frontend Developer'
                        }
                    }
                },
                {
                    '_id': 10,
                    'col': 'Profession',
                    'row': 4,
                    'details': {
                        'profession': {
                            'role': 'Backend Developer'
                        }
                    }
                },
                {
                    '_id': 11,
                    'col': 'Profession',
                    'row': 5,
                    'details': {
                        'profession': {
                            'role': 'Backend Developer'
                        }
                    }
                },
                {
                    '_id': 12,
                    'col': 'Profession',
                    'row': 6,
                    'details': {
                        'profession': {
                            'role': 'Backend Developer'
                        }
                    }
                },
            ]
        )
    )

    insert_cities()

    return {
        'ds_id': ds_id,
    }


@pytest.fixture
def client():
    return app.test_client()


def assert_list_elements_equal(expected: Iterable, actual: Iterable, msg: Any = None):
    case = TestCase()
    case.maxDiff = None
    case.assertCountEqual(expected, actual, msg)


@Patch
def test_list(client, dataset1):
    result = client.get('/ls').get_json()
    assert isinstance(result, dict)
    assert result['success'] == True
    expected_list = [
        {
            'id': str(dataset1['ds_id']),
            'name': '1111.csv',
            'status': 'active',
            'extra': {
                'access': {'type': 'public'}
            },
            'cols': ['Location', 'Comment'],
            'detailization': {
                'Location': {
                    'labels': ['city']
                }
            }
        }
    ]
    assert_list_elements_equal(expected_list, result['list'])


@Patch
def test_list_with_v_and_f(client, dataset1):
    result = client.get('/ls?id=%s&-v=true&-f=true' % dataset1['ds_id']).get_json()
    assert isinstance(result, dict)
    assert result['success'] == True
    expected_list = [
        {
            'id': str(dataset1['ds_id']),
            'name': '1111.csv',
            'status': 'active',
            'extra': {
                'access': {'type': 'public'}
            },
            'cols': ['Location', 'Comment'],
            'detailization': {
                'Location': {
                    'labels': ['city']
                }
            },
            'visualization': {
                'Location': [
                    {
                        'key': 'Location city',
                        'type': 'globe',
                        'props': {
                            'action': 'group',
                            'col': 'Location',
                            'label': 'city'
                        },
                        'stringrepr': 'Show cities',
                        'labelselector': 'id.name'
                    }
                ]
            },
            'filtering': {
                'Location': [
                    {
                        'type': 'multiselect',
                        'col': 'Location',
                        'label': 'city',
                        'values': [
                            {
                                'id': '1',
                                'name': 'Moscow',
                                'loc': {
                                    'type': 'Point',
                                    'coordinates': [37.61556, 55.75222]
                                }
                            },
                            {
                                'id': '3',
                                'name': 'New York',
                                'loc': {
                                    'type': 'Point',
                                    'coordinates': [-74.00597, 40.71427]
                                }
                            },
                            {
                                'id': '2',
                                'name': 'Paris',
                                'loc': {
                                    'type': 'Point',
                                    'coordinates': [2.3488, 48.85341]
                                }
                            },
                        ],
                        'selected': [],
                        'labelselector': 'name',
                        'valueselector': 'id',
                        'valuefield': 'city.id'
                    }
                ]
            }
        }
    ]
    assert_list_elements_equal(expected_list, result['list'])


@Patch
def test_visualize(client, dataset1):
    result = client \
        .get('/ds/%s/visualize' % dataset1['ds_id'],
             query_string='pipeline=' + json.dumps(
                 [{'action': 'group', 'col': 'Location', 'label': 'city', 'key': 'Location city'},
                  {'action': 'accumulate', 'accumulater': 'count', 'key': 'count'}])) \
        .get_json()
    assert isinstance(result, dict)
    assert result['success'] == True
    expected_list = [
        {
            'id': {
                'name': 'Moscow',
                'id': '1',
                'loc': {
                    'type': 'Point',
                    'coordinates': [37.61556, 55.75222]
                }
            },
            'count': 2
        },
        {
            'id': {
                'id': '2',
                'name': 'Paris',
                'loc': {
                    'type': 'Point',
                    'coordinates': [2.3488, 48.85341]
                }
            },
            'count': 1
        },
        {
            'id': {
                'id': '3',
                'name': 'New York',
                'loc': {
                    'type': 'Point',
                    'coordinates': [-74.00597, 40.71427]
                }
            },
            'count': 1
        },
        {
            'id': None,
            'count': 1
        }
    ]
    assert_list_elements_equal(expected_list, result["list"])


@Patch
def test_visualize_nested_group(client, dataset2):
    result = client \
        .get('/ds/%s/visualize' % dataset2['ds_id'],
             query_string='pipeline=' + json.dumps(
                 [{'action': 'group', 'col': 'Location', 'label': 'city', 'key': 'Location city'},
                  {'action': 'group', 'col': 'Profession', 'label': 'profession.role', 'key': 'Profession role'},
                  {'action': 'accumulate', 'accumulater': 'count', 'key': 'count'}])) \
        .get_json()
    assert isinstance(result, dict)
    assert result['success'] == True
    expected_list = [{'Profession role': [{'count': 1, 'id': 'System Architest'}],
                      'id': {'loc': {'coordinates': [2.3488, 48.85341], 'type': 'Point'}, 'name': 'Paris', 'id': '2'}},
                     {'Profession role': [{'count': 1, 'id': 'Backend Developer'}],
                      'id': {'loc': {'coordinates': [-74.00597, 40.71427], 'type': 'Point'}, 'name': 'New York',
                             'id': '3'}}, {
                         'Profession role': [{'count': 1, 'id': 'Frontend Developer'},
                                             {'count': 1, 'id': 'System Architest'},
                                             {'count': 2, 'id': 'Backend Developer'}],
                         'id': {'loc': {'coordinates': [37.61556, 55.75222], 'type': 'Point'}, 'name': 'Moscow',
                                'id': '1'}}]

    # assert nested list is equal without order
    assert len(expected_list) == len(result['list'])
    for item in expected_list:
        item_id = item['id']
        for key, subitem in item.items():
            result_subitem = next(item for item in result['list'] if item_id == item['id'])[key]
            assert_list_elements_equal(subitem, result_subitem, f'for {item_id}')


@Patch
def test_filter(client, dataset1):
    result = client \
        .get('/ds/%s/filter' % dataset1['ds_id'],
             query_string='query=' + json.dumps(
                 [{'col': 'Location', 'label': 'city.id',
                   'predicate': {'op': 'in', 'values': ['1']}}])) \
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
    assert_list_elements_equal(expected_list, result['list'])


@Patch
def test_filter_uncategorized(client, dataset1):
    result = client \
        .get('/ds/%s/filter' % dataset1['ds_id'],
             query_string='query=' + json.dumps(
                 [{'col': 'Location', 'label': 'city.id',
                   'predicate': {'op': 'in', 'values': [None]}}])) \
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
    assert_list_elements_equal(expected_list, result['list'])


# @Patch
# def test_filter_text_search(client, dataset1):
#     result = client \
#         .get('/ds/%s/filter' % dataset1['ds_id'],
#              query_string='query=' + json.dumps(
#                  [{'col': 'Comment',
#                    'predicate': {'op': 'instr', 'value': '44'}}])) \
#         .get_json()
#     assert isinstance(result, dict)
#     assert result['success'] == True
#
#     expected_list = [
#         {
#             'id': 3,
#             'Location': 'MOscow',
#             'Comment': '2344'
#         },
#         {
#             'id': 4,
#             'Location': 'NYC',
#             'Comment': '4444'
#         },
#     ]
#     TestCase().assertCountEqual(expected_list, result['list'])


@Patch
def test_get_label_values(client, dataset1):
    result = client \
        .get('/ds/%s/label-values?col=Location&label=city' % dataset1['ds_id']) \
        .get_json()
    assert isinstance(result, dict)
    assert result['success'] == True

    expected_list = [
        {
            'id': '1',
            'name': 'Moscow',
            'loc': {
                'type': 'Point',
                'coordinates': [37.61556, 55.75222]
            }
        },
        {
            'id': '2',
            'name': 'Paris',
            'loc': {
                'type': 'Point',
                'coordinates': [2.3488, 48.85341]
            }
        },
        {
            'id': '3',
            'name': 'New York',
            'loc': {
                'type': 'Point',
                'coordinates': [-74.00597, 40.71427]
            }
        },
    ]
    assert_list_elements_equal(expected_list, result['list'])
