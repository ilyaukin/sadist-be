import datetime
import multiprocessing
import os
from concurrent.futures._base import Future
from typing import Union, Tuple, Dict, Optional, Any

from bson import ObjectId

from app import _db, _set
from async_loop import call_async
from classification.abstract_classifier import AbstractClassifier

# settings
from helper import process_queue

MAX_PROCESS_COUNT = os.cpu_count()
CHUNK_SIZE = 100


def classify_cells(ds_id: Union[str, ObjectId],
                   classifier: AbstractClassifier):
    """
    Classify each non-empty cell of the data source.
    Results will be saved to `ds_<ds_id>_classification` collection.
    :param ds_id: Data source ID
    :param classifier: Classifier
    :return:
    """

    collection_name = 'ds_%s' % ds_id
    result_collection_name = 'ds_%s_classification' % ds_id
    if _collection_exists(result_collection_name):
        _db()[result_collection_name].drop()
    _db().create_collection(result_collection_name)
    _db()[result_collection_name].create_index('row')
    _db()[result_collection_name].create_index('col')

    # task is a tuple cell, value, where
    # cell is a dict (row=row, col=col);
    # result is cell with added label
    input_queue = multiprocessing.Queue()
    input_count = 0
    output_queue = multiprocessing.Queue()
    output_count = 0
    for task in (({'row': record['_id'], 'col': col}, value)
                 for record in _db()[collection_name].find()
                 for col, value in record.items()
                 if col != '_id' and value):
        input_queue.put(task)
        input_count += 1

    _update_ds_list_record(ds_id, {
        'status': 'in progress',
        'started': datetime.datetime.now(),
        'estimated': _get_estimated_duration(count=input_count)
    })
    cl_stat_id = _create_cl_stat_record(count=input_count)

    for x in range(MAX_PROCESS_COUNT):
        p = multiprocessing.Process(target=_execute_task,
                                    args=(classifier, input_queue, output_queue))
        p.start()

    cells = []
    while output_count < input_count:
        cell = output_queue.get()
        output_count += 1
        cells.append(cell)

        if len(cells) >= CHUNK_SIZE:
            _db()[result_collection_name]\
                .insert_many(cells)
            cells.clear()

    if cells:
        _db()[result_collection_name].insert_many(cells)

    _update_ds_list_record(ds_id, {
        'status': 'finished'
    })
    _update_cl_stat_record(cl_stat_id)


def call_classify_cells(ds_id: Union[str, ObjectId],
                        classifier: AbstractClassifier) -> Future:
    """
    Call classify_cells and don't wait for
    completion
    :param ds_id: Data source ID
    :param classifier: Classifier
    :return:
    """
    return call_async(classify_cells, ds_id, classifier)


def _execute_task(classifier: AbstractClassifier,
                  input_queue: multiprocessing.Queue,
                  output_queue: multiprocessing.Queue):
    for cell, value in process_queue(input_queue):
        cell.update({'label': classifier.classify(value)})
        output_queue.put(cell)


def _collection_exists(collection_name):
    return collection_name in _db().list_collection_names()


def _update_ds_list_record(ds_id: Union[str, ObjectId],
                           classification: dict):
    _db()['ds_list'].update({'_id': ObjectId(ds_id)},
                            _set({'classification': classification}))


def _create_cl_stat_record(count: int) -> ObjectId:
    return _db()['cl_stat'].insert_one({
        'count': count,
        'started': datetime.datetime.now()
    }).inserted_id


def _update_cl_stat_record(cl_stat_id: ObjectId):
    _db()['cl_stat'].update({'_id': cl_stat_id},
                            _set({'finished': datetime.datetime.now()}))


def _get_estimated_duration(count: int) -> Optional[Any]:
    cursor = _db()['cl_stat'].aggregate([
        {
            "$match": {
                "finished": {"$ne": None}
            }
        },
        {
            "$addFields": {
                "duration": {
                    "$subtract": ["$finished", "$started"]
                }
            }
        },
        {
            "$addFields": {
                "oneDuration": {
                    "$divide": ["$duration", "$count"]
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "avgOneDuration": {
                    "$avg": "$oneDuration"
                }
            }
        }]
    )
    if cursor:
        for record in cursor:
            return record['avgOneDuration'] * count
    return None
