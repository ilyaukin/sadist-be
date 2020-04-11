import inspect
import multiprocessing
import os
from concurrent.futures._base import Future
from typing import Union, Type, Iterable

from bson import ObjectId

import detailization
from app import _db, _set, app
from async_loop import call_async
from detailization.abstract_detailizer import AbstractDetailizer
from helper import process_queue

DETAILIZER_CLASSES = [t[1] for t in inspect.getmembers(detailization,
                                                lambda m: inspect.isclass(
                                                    m) and issubclass(m,
                                                                      AbstractDetailizer))]

MAX_PROCESS_COUNT = os.cpu_count()


def get_details_for_cells(ds_id: Union[str, ObjectId],
                          col: str,
                          detaililzer: AbstractDetailizer):
    input_queue = multiprocessing.Queue()
    input_count = 0
    output_queue = multiprocessing.Queue()
    output_count = 0

    collection_name = 'ds_%s' % ds_id
    classification_collection_name = 'ds_%s_classification' % ds_id

    for cell in _db()[classification_collection_name].find({'col': col}):
        input_queue.put((cell['_id'],
                         _db()[collection_name].find_one({'_id': cell['row']})[
                             col]))
        input_count += 1

    _update_ds_list_record(ds_id, col, {'status': 'in progress'})

    for x in range(MAX_PROCESS_COUNT):
        p = multiprocessing.Process(target=_execute_task,
                                    args=(
                                    detaililzer, input_queue, output_queue))
        p.start()

    while output_count < input_count:
        _id, details = output_queue.get()
        output_count += 1
        if details:
            _db()[classification_collection_name].update({'_id': _id},
                                                         _set({'details': details}))

    _update_ds_list_record(ds_id, col, {'status': 'finished',
                                        'labels': detaililzer.labels})


def call_get_details_for_cells(ds_id: Union[str, ObjectId],
                               col: str,
                               detailizer: AbstractDetailizer) -> Future:
    return call_async(get_details_for_cells, ds_id, col, detailizer)


def call_get_details_for_all_cols(ds_id: Union[str, ObjectId]) -> Iterable[Future]:
    app.logger.debug('Detailization of DS %s started' % ds_id)
    ff = []
    ds_classification_collection = _db()['ds_%s_classification' % ds_id]
    for aggregation_row in ds_classification_collection.aggregate([
        {
            '$group': {
                '_id': {'col': '$col', 'label': '$label'},
                'count': {'$sum': 1}
            }
        },
        {
            '$group': {
                '_id': '$_id.col',
                'labels': {
                    '$push': {
                        'label': '$_id.label',
                        'count': '$count'
                    }
                }
            }
        }
    ]):
        col = aggregation_row['_id']
        labels = aggregation_row['labels']
        detailizer_class: Type[AbstractDetailizer]
        for detailizer_class in DETAILIZER_CLASSES:
            if sum(d['count'] for d in labels if d['label'] in detailizer_class.labels) / \
                sum(d['count'] for d in labels) > detailizer_class.threshold:
                app.logger.info('Col %s of DS %s will be detailized'
                                ' via %s' % (col, ds_id, detailizer_class))
                _update_ds_list_record(ds_id, col, {'status': 'pending'})
                f = call_get_details_for_cells(ds_id, col, detailizer_class())
                ff.append(f)
    return ff


def _execute_task(detailizer: AbstractDetailizer,
                  input_queue: multiprocessing.Queue,
                  output_queue: multiprocessing.Queue):
    for _id, value in process_queue(input_queue):
        details = detailizer.get_details(value)
        output_queue.put((_id, details))


def _update_ds_list_record(ds_id: Union[str, ObjectId],
                           col: str,
                           detailization: dict):
    _db()['ds_list'].update({'_id': ObjectId(ds_id)},
                            _set({'detailization.%s' % col: detailization}))