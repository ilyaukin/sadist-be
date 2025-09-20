import datetime

from pymongo.database import Database


def upgrade(db: Database):
    db.drop_collection('nn_model')


def downgrade(db: Database):
    # no downgrade needed
    pass
