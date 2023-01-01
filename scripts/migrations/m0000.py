import pymongo
from pymongo.collation import Collation, CollationAlternate, CollationMaxVariable
from pymongo.database import Database


def upgrade(db: Database):
    db.ds_list.create_index([("name",  pymongo.ASCENDING), ("status", pymongo.ASCENDING)])
    db.ds_list.create_index([("status", pymongo.ASCENDING)])
    db.cl_pattern.create_index([("level", pymongo.ASCENDING)])
    db.cl_pattern_char.create_index([("level", pymongo.ASCENDING)])
    db.dl_master.create_index([("text",  pymongo.ASCENDING)], unique=True)
    db.dl_geo.create_index([("text", pymongo.ASCENDING)], unique=True)
    # TODO figure out where this collation comes from
    c = Collation(locale='en',
                  caseLevel=False,
                  caseFirst="off",
                  strength=1,
                  numericOrdering=False,
                  alternate="non-ignorable",
                  maxVariable="punct",
                  normalization=False,
                  backwards=False)
    db.geo_city.create_index([("name", pymongo.ASCENDING)], collation=c)
    db.geo_city.create_index([("alternatenames", pymongo.ASCENDING)], collation=c)
    db.geo_country.create_index([("name", pymongo.ASCENDING)])


def downgrade(db: Database):
    # no need to downgrade since it's an initial schema
    pass
