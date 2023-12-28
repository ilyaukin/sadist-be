import pymongo
from pymongo.database import Database


def upgrade(db: Database):
    db.dl_seq.create_index([("text", pymongo.ASCENDING)])
    db.dl_seq_label.insert_many({'_id': label} for label in [
        # basic
        'number',
        'word',
        'separator',
        'whitespace',
        # date-time
        'year',
        'month',
        'month[name]',
        'day',
        'hour',
        'minute',
        'second',
        'fraction',
        'time-unit',
        # geography
        'city',
        'country',
        'state',
        'province',
        'side',
        # number
        'roman-number',
        'decimal-point',
        'operator[minus]',
        'operator[plus]',
        'operator[per]',
        'operator[percent]',
        'operator[times]',
        # currency
        'currency-code',
        'currency-sign',
        'currency-name',
        # gender
        'gender[male]',
        'gender[female]',
        'gender[non-binary]',
    ])


def downgrade(db: Database):
    db.dl_seq_label.delete_many({})
