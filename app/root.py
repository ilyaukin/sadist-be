import csv
import io
import json
import traceback
from concurrent.futures._base import Future
from typing import Union

from bson import ObjectId
from flask import render_template, url_for, request
from pymongo.cursor import Cursor

from app import app, _db, transactional
from classification import PatternClassifier, \
    call_classify_cells
from detailization import call_get_details_for_all_cols


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

    process_ds(ds_id)

    return {
        'item': serialize(get_ds_list_active_record(csv_file.filename)),
        'success': True
    }


@app.route('/ls')
def list_ds():
    filter = {'status': 'active'}
    _id = request.args.get('id')
    if (_id):
        filter.update({'_id': ObjectId(_id)})

    cursor = _db()['ds_list'].find(filter)
    return list_response(cursor)


@app.route('/ds/<ds_id>')
def get_ds(ds_id):
    ds_collection_name = 'ds_%s' % ds_id
    cursor = _db()[ds_collection_name].find()
    return list_response(cursor)


@app.route('/ds/<ds_id>/visualize')
def visualize_ds(ds_id):
    pipeline_str = request.args['pipeline']
    pipeline = json.loads(pipeline_str)

    classification_collection_name = 'ds_%s_classification' % ds_id
    aggregation_pipeline = []

    # stage #1. combine all cols details by row id
    stage1 = {
        '$group': {
            '_id': '$row',
            'colDet': {
                '$push': {
                    'col': '$col',
                    'details': '$details'
                }
            }
        }
    }
    aggregation_pipeline.append(stage1)

    # stage #2. extract cols which are used in the pipeline
    # to the separate fields
    stage2 = {
        '$project': dict(
            (item['col'], {
                '$arrayElemAt': [
                    {
                        '$filter': {
                            'input': '$colDet',
                            'cond': {
                                '$eq': ['$$this.col', item['col']]
                            }
                        },
                    },
                    0
                ]
            })
            for item in pipeline
        )
    }
    aggregation_pipeline.append(stage2)

    # stage #3. get needed part of col details
    stage3 = {
        '$project': dict(
            (item['col'], '$%s.details.%s' % (item['col'], item['key']))
            for item in pipeline
        )
    }
    aggregation_pipeline.append(stage3)

    # stage #4. group and calculate values.
    # we will consider pipeline items with `value` as
    # scalar items (such as mean, median etc.) and items
    # without this filed as group items, which will be nested
    # one to another,
    scalar_items = list(filter(lambda item: 'value' in item, pipeline))
    group_items = list(filter(lambda item: 'value' not in item, pipeline))

    def get_group_id(items, in_id):
        col_prefix = '$' + ('_id.' if in_id else '')
        if len(items) > 1:
            return dict(
                (item['col'], col_prefix + item['col']) for item in items)
        elif len(items) == 1:
            return col_prefix + items[0]['col']
        return None

    def get_aggregation_operation(item):
        # this applies something like '$avg'
        return {
            item['value']: '$%s' % item['col']
        }

    aggregation_expr = dict(
        (item['col'], get_aggregation_operation(item))
        for item in scalar_items) or dict(count={'$sum': 1})
    transition_expr = dict((key, '$%s' % key)
                           for key in aggregation_expr.keys())

    stage4 = {
        '$group': {**{
            '_id': get_group_id(group_items, in_id=False)
        }, **aggregation_expr}
    }
    aggregation_pipeline.append(stage4)
    while len(group_items) > 1:
        item = group_items.pop()
        stage4 = {
            '$group': {
                '_id': get_group_id(group_items, in_id=True),
                'list': {
                    '$push': {
                        **{
                            item['col']: '$_id.%s' % item['col']
                        },
                        **transition_expr
                    }
                }
            }
        }
        aggregation_pipeline.append(stage4)
        transition_expr = dict(list='$list')

    app.logger.debug('db.%s.aggregate( %s )' %
                     (classification_collection_name, aggregation_pipeline))
    cursor = _db()[classification_collection_name].aggregate(
        aggregation_pipeline)
    return list_response(cursor)


@app.route('/ds/<ds_id>/filter')
def filter_ds(ds_id):
    query_str = request.args['query']
    query = json.loads(query_str)

    # if query is empty, return all rows
    if not query:
        return get_ds(ds_id)

    collection_name = 'ds_%s' % ds_id
    classification_collection_name = 'ds_%s_classification' % ds_id

    aggregation_pipeline = []
    aggregation_pipeline.append({
        '$match': {
            '$expr': {
                '$and': [{
                    '$eq': ['$col', query[0]['col']]
                }, {
                    '$in': [
                        '$details.%s' % query[0]['key'],
                        query[0]['values']
                    ]
                }]
            }
        }
    })
    aggregation_pipeline.append({
        '$project': {
            'row': 1
        }
    })
    for item in query[1:]:
        aggregation_pipeline.append({
            '$lookup': {
                'from': classification_collection_name,
                'localField': 'row',
                'foreignField': 'row',
                'as': item['col']
            }
        })
        aggregation_pipeline.append({
            '$project': {
                'row': 1,
                item['col']: {
                    '$arrayElemAt': [
                        {
                            '$filter': {
                                'input': '$%s' % item['col'],
                                'cond': {
                                    '$eq': ['$$this.col', item['col']]
                                }
                            }
                        },
                        0
                    ]
                }
            }
        })
        aggregation_pipeline.append({
            '$match': {
                '$expr': {
                    '$in': {
                        '$%s.details.%s' % (item['col'], item['key']),
                        item['values']
                    }
                }
            }
        })
        aggregation_pipeline.append({
            '$project': {
                'row': 1
            }
        })
    aggregation_pipeline.append({
        '$lookup': {
            'from': collection_name,
            'localField': 'row',
            'foreignField': '_id',
            'as': 'rowData'
        }
    })
    aggregation_pipeline.append({
        '$replaceRoot': {
            'newRoot': {
                '$arrayElemAt': ['$rowData', 0]
            }
        }
    })

    app.logger.debug('db.%s.aggregate( %s )' %
                     (classification_collection_name, aggregation_pipeline))
    cursor = _db()[classification_collection_name].aggregate(
        aggregation_pipeline)
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
        update_ds_list_record(old_record['_id'], {'status': 'old'},
                              session=session)
    update_ds_list_record(ds_id,
                          {'status': 'active', 'cols': csv_reader.fieldnames},
                          session=session)


def process_ds(ds_id):
    def on_classify_done(future: Future):
        call_get_details_for_all_cols(ds_id)

    call_classify_cells(ds_id, PatternClassifier()) \
        .add_done_callback(on_classify_done)


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
    record['id'] = str(record['_id']) \
        if isinstance(record['_id'], ObjectId) \
        else record['_id']
    del record['_id']
    return record
