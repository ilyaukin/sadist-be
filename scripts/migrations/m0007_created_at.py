import datetime

from pymongo.database import Database


def upgrade(db: Database):
    fake_date = datetime.datetime.fromtimestamp(0)
    db.ds_list.update_many({'_createdAt': {'$exists': False}},
                           {'$set': {'_createdAt': fake_date,
                                     '_updatedAt': fake_date}})


def downgrade(db: Database):
    db.ds_list.update_many({}, {'$unset': {'_createdAt': '', '_updatedAt': ''}})
