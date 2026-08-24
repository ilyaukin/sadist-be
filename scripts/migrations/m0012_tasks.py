from pymongo import ASCENDING
from pymongo.database import Database


def upgrade(db: Database):
    db.task_active.create_index([
        ('executionType', ASCENDING),
        ('status', ASCENDING),
        ('_createdAt', ASCENDING),
    ])
    db.task_active.create_index([
        ('executionType', ASCENDING),
        ('status', ASCENDING),
        ('runAt', ASCENDING),
        ('executionInterval', ASCENDING),
    ])
    db.task_active.create_index([
        ('status', ASCENDING),
        ('runAt', ASCENDING),
        ('timeout', ASCENDING),
    ])
    db.task_active.create_index([('taskType', ASCENDING)])
    db.task_archive.create_index([('taskType', ASCENDING), ('_createdAt', ASCENDING)])


def downgrade(db: Database):
    db.task_active.drop_indexes()
    db.task_archive.drop_indexes()
