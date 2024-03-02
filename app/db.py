import os
from typing import Union

import gridfs
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
        if pid not in self.mongo_client_pool:
            self.mongo_client_pool[pid] = self._new_client()
        return self.mongo_client_pool[pid]

    def db(self) -> Database:
        return self.mongo_client().get_database()

    @property
    def fs(self) -> gridfs.GridFS:
        return gridfs.GridFS(self.db())

    def _new_client(self) -> pymongo.MongoClient:
        return pymongo.MongoClient(SadistDatabaseConnection.DATABASE_URL, socketTimeoutMS=30000)


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

currency_list = Collection('currency_list')

dl_session = CollectionFamily('dl_session_%s')
dl_session_list = Collection('dl_session')
dl_master = Collection('dl_master')
dl_geo = Collection('dl_geo')
dl_currency = Collection('dl_currency')
dl_seq = Collection('dl_seq')
dl_seq_label = Collection('dl_seq_label')

nn_model = Collection('nn_model')

app_config = Collection('app_config')
app_user = Collection('app_user')
app_user_session = Collection('app_user_session')
app_db_migration = Collection('app_db_migration')

wc_proxy = Collection('wc_proxy')
wc_script_template = Collection('wc_script_template')


def replace_grid_file(data: bytes, filename: str):
    """
    Create or replace a file in GridFS by filename
    @param data: Binary file data
    @param filename: filename
    @return: None
    """
    old_file = conn.fs.find_one({'filename': filename})
    if old_file:
        conn.fs.delete(old_file._id)
    conn.fs.put(data, filename=filename)


def read_grid_file(filename: str) -> Union[bytes, None]:
    """
    Get data from the file in GridFS
    @param filename: filename
    @return: Binary data
    """
    file = conn.fs.find_one({'filename': filename})
    if file:
        return file.read()
    return None
