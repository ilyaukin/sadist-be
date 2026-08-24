from bson import ObjectId
from mongomoron import aggregate, dict_, document, push_, sum_, update

from app import logger
from db import conn, ds_classification, ds_list
from detailization import AbstractDetailizer, get_details_for_cells
from scheduler.task_interface import Task


@Task.sub('detailize_cols')
class DetailizeColsTask(Task):
    """Detailize all suitable dataset columns after classification."""

    def validate(self, payload: dict):
        """Validate detailization task payload before task creation."""
        if not payload.get('dsId'):
            raise ValueError('dsId is required')

    def execute(self, payload: dict):
        """Find suitable detailizers and run them synchronously."""
        ds_id = payload['dsId']
        for col, labels, detailizer in _iter_detailization_tasks(ds_id):
            logger.info('Col %s of DS %s will be detailized via %s',
                        col, ds_id, detailizer.__class__.__name__)
            _update_detailization_status(ds_id, col, {'status': 'pending'})
            try:
                get_details_for_cells(ds_id, col, detailizer)
                _update_detailization_status(ds_id, col, {
                    'status': 'finished',
                    'labels': detailizer.labels,
                })
            except Exception as e:
                _update_detailization_status(ds_id, col, {
                    'status': 'failed',
                    'error': str(e),
                })
                raise


def _iter_detailization_tasks(ds_id):
    p = aggregate(ds_classification[ds_id]) \
        .group(dict_(col=document.col, label=document.label), count=sum_(1)) \
        .group(document._id.col, labels=push_(
        dict_(label=document._id.label, count=document.count)))

    for aggregation_row in conn.execute(p):
        col = aggregation_row['_id']
        labels = aggregation_row['labels']
        for detailizer in AbstractDetailizer.get_all(lambda cls:
                                                     sum(d['count'] for d in labels if
                                                         d['label'] in cls.labels) /
                                                     sum(d['count'] for d in labels) > cls.threshold):
            yield col, labels, detailizer


def _update_detailization_status(ds_id, col: str, detailization: dict):
    conn.execute(
        update(ds_list)
            .filter(ds_list._id == ObjectId(ds_id))
            .set({'detailization.%s' % col: detailization})
    )