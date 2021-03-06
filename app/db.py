import os

import pymongo
from mongomoron import DatabaseConnection, Collection
from pymongo.database import Database


class SadistDatabaseConnection(DatabaseConnection):
    DATABASE_URL = os.environ.get('DATABASE_URL') or \
                   'mongodb://127.0.0.1:27017,127.0.0.1:27018/sadist?replicaSet=rs0'

    def __init__(self):
        super().__init__(None, None)
        self.mongo_client_pool = dict()

    def mongo_client(self):
        pid = os.getpid()
        self.mongo_client_pool.setdefault(pid,
                                          pymongo.MongoClient(
                                              SadistDatabaseConnection.DATABASE_URL))
        return self.mongo_client_pool[pid]

    def db(self) -> Database:
        return self.mongo_client().get_database()


conn = SadistDatabaseConnection()


class CollectionFamily(object):

    def __init__(self, pattern: str):
        self.pattern = pattern

    def __getitem__(self, item) -> Collection:
        return Collection(self.pattern % item)


ds = CollectionFamily('ds_%s')
ds_classification = CollectionFamily('ds_%s_classification')
ds_list = Collection('ds_list')

cl_stat = Collection('cl_stat')
cl_pattern = Collection('cl_pattern')
cl_pattern_char = Collection('cl_pattern_char')

geo_city = Collection('geo_city')
geo_country = Collection('geo_country')

dl_session = CollectionFamily('dl_session_%s')
dl_session_list = Collection('dl_session')
dl_master = Collection('dl_master')
