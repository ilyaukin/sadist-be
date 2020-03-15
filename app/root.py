import csv
import csv
import io
import json
import traceback
from typing import Union

from bson import ObjectId
from flask import render_template, url_for, request
from pymongo.cursor import Cursor

from app import app, _db, transactional
from classification import PatternClassifier, \
    call_classify_cells


@app.route('/')
def root():
    return render_template('spa.html',
                           fe_root=url_for('static', filename='root.js'))


@app.route('/ds', methods=['PUT'])
def create_ds(session=None):
    csv_file = request.files['csv']
    ds_type = request.form['type']
    ds_extra_string = request.form['extra']
    ds_extra = None
    if (ds_extra_string):
        ds_extra = json.loads(ds_extra_string)

    ds_id = create_ds_list_blank_record(csv_file, ds_type, ds_extra)

    ds_collection_name = 'ds_%s' % ds_id
    _db().create_collection(ds_collection_name)
    try:
        add_ds(csv_file, ds_id, ds_collection_name)
    except Exception as e:
        update_ds_list_record(ds_id, {'status': 'failed', 'error': str(e)})
        # delete failed collection
        _db()[ds_collection_name].drop(session=session)
        return error(e)

    call_classify_cells(ds_id, PatternClassifier())

    return {
        'item': serialize(get_ds_list_active_record(csv_file.filename)),
        'success': True
    }


@app.route('/ls')
def list_ds():
    cursor = _db()['ds_list'].find({'status': 'active'})
    return list_response(cursor)


@app.route('/ds/<ds_id>')
def get_ds(ds_id):
    ds_collection_name = 'ds_%s' % ds_id
    cursor = _db()[ds_collection_name].find()
    return list_response(cursor)


@transactional
def add_ds(csv_file, ds_id, ds_collection_name, session=None):
    old_record = get_ds_list_active_record(csv_file.filename)
    ds = _db()[ds_collection_name]
    stream = io.StringIO(csv_file.stream.read().decode('UTF8'), newline=None)
    csv_reader = csv.DictReader(stream)
    csv_rows = [csv_row for csv_row in csv_reader]
    app.logger.debug(csv_rows)
    ds.insert_many(csv_rows, session=session)

    # update old collection status to "old"
    # and new collection status to "active"
    if old_record:
        update_ds_list_record(old_record['_id'], {'status': 'old'}, session=session)
    update_ds_list_record(ds_id, {'status': 'active', 'cols': csv_reader.fieldnames}, session=session)


def update_ds_list_record(ds_id: Union[str, ObjectId], d: dict, session=None):
    _db()['ds_list'].update_one({'_id': ObjectId(ds_id)}, {
        '$set': d
    }, session=session)


def create_ds_list_blank_record(csv_file, type, extra, session=None):
    ds_list = _db()['ds_list']
    name = csv_file.filename
    ds_id = ds_list.insert_one({
        'name': name,
        'type': type,
        'status': 'blank',
        'extra': extra
    }, session=session).inserted_id
    return ds_id


def get_ds_list_active_record(name):
    ds_list = _db()['ds_list']
    return ds_list.find_one({'name': name, 'status': 'active'})


@app.errorhandler(Exception)
def handle_exception(e: Exception):
    return error(e)


def error(e):
    app.logger.error(traceback.format_exc())
    return {'error': str(e)}, 500


def list_response(cursor: Cursor):
    return {
        'list': [serialize(record) for record in cursor],
        'success': True
    }


def serialize(record: dict):
    record['id'] = str(record['_id'])
    del record['_id']
    return record
