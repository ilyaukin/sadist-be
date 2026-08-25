from pymongo import ASCENDING
from pymongo.database import Database


def upgrade(db: Database):
    db.ds_subscription.create_index([('dsName', ASCENDING)])
    db.ds_subscription.create_index([('userIds', ASCENDING)])


def downgrade(db: Database):
    db.ds_subscription.drop_indexes()