import inspect
import traceback
from concurrent.futures._base import Future
from typing import Union, Type, Iterable, Tuple

import detailization
import error_handler
from app import app
from async_loop import call_async
from async_processing import process_in_parallel
from bson import ObjectId
from db import conn, ds_classification, ds, ds_list
from detailization.abstract_detailizer import AbstractDetailizer
from mongomoron import update, aggregate, dict_, document, \
    sum_, push_

DETAILIZER_CLASSES = [t[1] for t in inspect.getmembers(detailization,
                                                       lambda
                                                           m: inspect.isclass(
                                                           m) and issubclass(m,
                                                                             AbstractDetailizer))]


def get_details_for_cells(ds_id: Union[str, ObjectId],
                          col: str,
                          detaililzer: AbstractDetailizer):
    input = ((cell['_id'], cell['value']) \
             for cell in conn.execute(
        aggregate(ds_classification[ds_id]) \
            .match(document.col == col)
            .lookup(ds[ds_id], local_field='row',
                    foreign_field='_id',
                    as_='row_data') \
            .project(row_data=document.row_data[0]) \
            .project(value=document.row_data.get_field(col))
    ))

    _update_ds_list_record(ds_id, col, {'status': 'in progress'})

    for _id, details in process_in_parallel(input, processor=_execute_task,
                                            args=(detaililzer,), timeout=120):
        if details:
            conn.execute(
                update(ds_classification[ds_id]) \
                    .filter(ds_classification[ds_id]._id == _id)
                    .set({'details': details})
            )

    _update_ds_list_record(ds_id, col, {'status': 'finished',
                                        'labels': detaililzer.labels})


def call_get_details_for_cells(ds_id: Union[str, ObjectId],
                               col: str,
                               detailizer: AbstractDetailizer) -> Future:
    def _handle_async_exception(f: Future):
        e = f.exception()
        if e:
            error_handler.error(traceback.format_exc())
            _update_ds_list_record(ds_id, col,
                                   {'status': 'failed', 'error': str(e)})

    f = call_async(get_details_for_cells, ds_id, col, detailizer)
    f.add_done_callback(_handle_async_exception)
    return f


def call_get_details_for_all_cols(ds_id: Union[str, ObjectId]) -> Iterable[
    Future]:
    app.logger.debug('Detailization of DS %s started' % ds_id)
    ff = []
    p = aggregate(ds_classification[ds_id]) \
        .group(dict_(col=document.col, label=document.label), count=sum_(1)) \
        .group(document._id.col, labels=push_(
        dict_(label=document._id.label, count=document.count)))

    for aggregation_row in conn.execute(p):
        col = aggregation_row['_id']
        labels = aggregation_row['labels']
        detailizer_class: Type[AbstractDetailizer]
        for detailizer_class in DETAILIZER_CLASSES:
            if sum(d['count'] for d in labels if
                   d['label'] in detailizer_class.labels) / \
                    sum(d['count'] for d in
                        labels) > detailizer_class.threshold:
                app.logger.info('Col %s of DS %s will be detailized'
                                ' via %s' % (col, ds_id, detailizer_class))
                _update_ds_list_record(ds_id, col, {'status': 'pending'})
                f = call_get_details_for_cells(ds_id, col, detailizer_class())
                ff.append(f)
    return ff


def _execute_task(task: Tuple, detailizer: AbstractDetailizer):
    _id, value = task
    return _id, detailizer.get_details(value)


def _update_ds_list_record(ds_id: Union[str, ObjectId],
                           col: str,
                           detailization: dict):
    conn.execute(
        update(ds_list) \
            .filter(ds_list._id == ObjectId(ds_id)) \
            .set({'detailization.%s' % col: detailization})
    )
