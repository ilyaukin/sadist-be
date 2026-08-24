from typing import Union, Tuple

from async_processing import process_in_parallel
from bson import ObjectId
from db import conn, ds_classification, ds, ds_list
from detailization.abstract_detailizer import AbstractDetailizer
from mongomoron import update, aggregate, document


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
