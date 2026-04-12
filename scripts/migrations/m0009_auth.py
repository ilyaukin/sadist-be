import datetime

from pymongo.database import Database


def upgrade(db: Database):
    db.app_user.update_many({}, [
        {'$set': {'extra': {'$mergeObjects': ['$extra', '$jwt_decoded']}}},
        {'$unset': 'jwt_decoded'},
    ])


def downgrade(db: Database):
    # no downgrade needed
    pass
