import datetime

from pymongo.database import Database


def upgrade(db: Database):
    fake_date = datetime.datetime.fromtimestamp(0)
    db.app_user.update_many({'_createdAt': {'$exists': False}},
                            {'$set': {'_createdAt': fake_date,
                                      '_updatedAt': fake_date}})
    db.app_user_session.update_many({'_createdAt': {'$exists': False}},
                                    {'$set': {'_createdAt': fake_date,
                                              '_updatedAt': fake_date}})


def downgrade(db: Database):
    db.app_user.update_many({},
                            {'$unset': {'_createdAt': '', '_updatedAt': ''}})
    db.app_user_session.update_many({},
                                    {'$unset': {'_createdAt': '',
                                                '_updatedAt': ''}})
