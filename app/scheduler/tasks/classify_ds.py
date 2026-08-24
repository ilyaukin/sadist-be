from bson import ObjectId
from mongomoron import update

from classification import AbstractClassifier, SequenceClassifier, classify_cells
from db import conn, ds_list
from scheduler.task_interface import EXECUTION_TYPE_SINGLE, Task, create_task


@Task.sub('classify_ds')
class ClassifyDsTask(Task):
    """Classify all cells in a dataset and schedule detailization."""

    def validate(self, payload: dict):
        """Validate classification task payload before task creation."""
        if not payload.get('dsId'):
            raise ValueError('dsId is required')
        classifier_key = payload.get('classifier') or SequenceClassifier.__key__
        if not AbstractClassifier.get(classifier_key):
            raise ValueError('Unknown classifier: %s' % classifier_key)

    def execute(self, payload: dict):
        """Run classification and create the follow-up detailization task."""
        ds_id = payload['dsId']
        classifier_key = payload.get('classifier') or SequenceClassifier.__key__
        classifier = AbstractClassifier.get(classifier_key)
        try:
            classify_cells(ds_id, classifier)
            create_task(
                task_type='detailize_cols',
                execution_type=EXECUTION_TYPE_SINGLE,
                payload={'dsId': ds_id},
                timeout=payload.get('detailizationTimeout') or 3600,
            )
        except Exception as e:
            _update_classification_status(ds_id, {
                'status': 'failed',
                'error': str(e),
            })
            raise


def _update_classification_status(ds_id, classification: dict):
    conn.execute(
        update(ds_list)
            .filter(ds_list._id == ObjectId(ds_id))
            .set({'classification': classification})
    )