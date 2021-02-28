import datetime
import multiprocessing
import os
from concurrent.futures._base import Future
from typing import Union, Optional, Any

from async_loop import call_async
from bson import ObjectId
from classification.abstract_classifier import AbstractClassifier
from db import conn, ds_classification, ds, ds_list, cl_stat
# settings
from helper import process_queue
from mongomoron import index, query, insert_many, update, insert_one, aggregate, \
    avg

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

    classification_collection = conn.create_collection(ds_classification[ds_id])
    conn.create_index(index(classification_collection).asc('row'))
    conn.create_index(index(classification_collection).asc('col'))

    # task is a tuple cell, value, where
    # cell is a dict (row=row, col=col);
    # result is cell with added label
    input_queue = multiprocessing.Queue()
    input_count = 0
    output_queue = multiprocessing.Queue()
    output_count = 0
    for task in (({'row': record['_id'], 'col': col}, value)
                 for record in conn.execute(query(ds[ds_id]))
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
                                    args=(
                                        classifier, input_queue, output_queue))
        p.start()

    cells = []
    while output_count < input_count:
        cell = output_queue.get()
        output_count += 1
        cells.append(cell)

        if len(cells) >= CHUNK_SIZE:
            conn.execute(insert_many(classification_collection, cells))
            cells.clear()

    if cells:
        conn.execute(insert_many(classification_collection, cells))

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


def _update_ds_list_record(ds_id: Union[str, ObjectId],
                           classification: dict):
    conn.execute(
        update(ds_list) \
            .filter(ds_list._id == ObjectId(ds_id))
            .set({'classification': classification})
    )


def _create_cl_stat_record(count: int) -> ObjectId:
    return conn.execute(
        insert_one(cl_stat, {
            'count': count,
            'started': datetime.datetime.now()
        })
    ).inserted_id


def _update_cl_stat_record(cl_stat_id: ObjectId):
    return conn.execute(
        update(cl_stat) \
            .filter(cl_stat._id == cl_stat_id)
            .set({'finished': datetime.datetime.now()})
    )


def _get_estimated_duration(count: int) -> Optional[Any]:
    p = aggregate(cl_stat) \
        .match(cl_stat.finished != None) \
        .add_fields(duration=cl_stat.finished - cl_stat.started) \
        .add_fields(duration_one=cl_stat.duration / cl_stat.count) \
        .group(None, avg_duration=avg(cl_stat.duration_one))
    for result in conn.execute(p):
        return result['avg_duration'] * count
    return None
