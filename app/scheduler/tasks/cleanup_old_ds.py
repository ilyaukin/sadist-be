from bson import ObjectId
from mongomoron import delete, query

from db import conn, ds, ds_classification, ds_list
from scheduler.task_interface import Task


@Task.sub('cleanup_old_ds')
class CleanupOldDsTask(Task):
    """Remove outdated dataset versions for a dataset name."""

    def validate(self, payload: dict):
        """Validate cleanup task payload before task creation."""
        if not payload.get('name'):
            raise ValueError('name is required')
        if 'keep' in payload and (not isinstance(payload['keep'], int) or payload['keep'] < 0):
            raise ValueError('keep must be a non-negative integer')

    def execute(self, payload: dict):
        """Drop old dataset collections and delete extra old ds_list records."""
        old_records = list(conn.execute(
            query(ds_list)
                .filter(ds_list.name == payload['name'])
                .filter(ds_list.status == 'old')
                .sort((ds_list._createdAt, -1))
        ))
        keep = payload.get('keep', 1)
        for record in old_records[keep:]:
            _delete_ds(record['_id'])


def _delete_ds(ds_id: ObjectId):
    conn.drop_collection(ds[ds_id])
    conn.drop_collection(ds_classification[ds_id])
    delete_ds_list = delete(ds_list)
    delete_ds_list.filter(ds_list._id == ds_id)
    conn.execute(delete_ds_list)