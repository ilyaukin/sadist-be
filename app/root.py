import csv
import io
import json
import traceback
from concurrent.futures._base import Future
from typing import Union, Iterable

from mongomoron.mongomoron import Expression

from accumulator import get_accumulator, CountAccumulator
from app import app
from bson import ObjectId
from classification import PatternClassifier, \
    call_classify_cells
from db import conn, ds, ds_list, ds_classification
from detailization import call_get_details_for_all_cols
from flask import render_template, url_for, request, session
from mongomoron import insert_many, query, document, aggregate, push_, dict_, \
    filter_, and_, update_one, insert_one, query_one, or_

from serializer import serialize


@app.route('/')
def root():
    return render_template('spa.html',
                           fe_root=url_for('static', filename='root.js'))


@app.route('/ds', methods=['PUT'])
def create_ds():
    csv_file = request.files['csv']
    ds_type = request.form['type']
    ds_extra_string = request.form.get('extra', '{}')
    ds_extra = json.loads(ds_extra_string)

    ds_id = create_ds_list_blank_record(csv_file, ds_type, ds_extra)

    conn.create_collection(ds[ds_id])
    try:
        add_ds(ds_id, csv_file)
    except Exception as e:
        update_ds_list_record(ds_id, {'status': 'failed', 'error': str(e)})
        # delete failed collection
        conn.drop_collection(ds[ds_id])
        return error(e)

    process_ds(ds_id)

    return {
        'item': serialize(get_ds_list_active_record(csv_file.filename)),
        'success': True
    }


@app.route('/ls')
def list_ds():
    q = query(ds_list).filter(_get_access_clause()).filter(document.status == 'active')
    _id = request.args.get('id')
    if (_id):
        q.filter(document._id == ObjectId(_id))

    return list_response(conn.execute(q))


@app.route('/ds/<ds_id>')
def get_ds(ds_id):
    if not _has_access(ds_id):
        return list_response([])

    return list_response(conn.execute(query(ds[ds_id])))


@app.route('/ds/<ds_id>/visualize')
def visualize_ds(ds_id):
    if not _has_access(ds_id):
        return list_response([])

    # visualization pipeline (not to be confused with aggregation pipeline)
    # pipeline ::= [item 1, ...]
    # item ::= {col: <col name>, key: <path to value*>,
    # [accumulator: <accumulator>]}
    # * path to the value to be used in visualization.
    # if an item does not contain accumulator, grouping by this item
    # will be applied, otherwise the accumulator will be applied.
    pipeline_str = request.args['pipeline']
    pipeline = json.loads(pipeline_str)

    # build aggregation pipeline
    p = aggregate(ds_classification[ds_id]) \
        .match(document.col.in_(item['col'] for item in pipeline)) \
        .group(document.row, col_details=push_(
        dict_(col=document.col, details=document.details))) \
        .project(**dict((item['col'], filter_(lambda x: x.col == item['col'],
                                              document.col_details)[0]) for item
                        in pipeline)) \
        .project(**dict((item['col'],
                         document.get_field(item['col']).details.get_field(
                             item['key'])) for item in pipeline))

    # only 0 / 1 group level one so far
    if 'accumulator' not in pipeline[0]:
        _id = getattr(document, pipeline[0]['col'])
        accumulated_items = pipeline[1:]
    else:
        _id = None
        accumulated_items = pipeline
    if accumulated_items:
        accumulators = dict((item['col'],
                             get_accumulator(item['accumulator'],
                                             document.get_field(item['col'])))
                            for item in accumulated_items)
    else:
        # default accumulator
        accumulators = dict(count=CountAccumulator())

    p.group(_id, **dict(
        (col_name, accumulator.get_accumulator()) for col_name, accumulator in
        accumulators.items()))

    cursor = conn.execute(p)
    result = list(cursor)

    for accumulator in accumulators.values():
        accumulator.postprocess(result)

    return list_response(result)


@app.route('/ds/<ds_id>/filter')
def filter_ds(ds_id):
    if not _has_access(ds_id):
        return list_response([])

    query_str = request.args['query']
    query = json.loads(query_str)

    # if query is empty, return all rows
    if not query:
        return get_ds(ds_id)

    p = aggregate(ds_classification[ds_id]) \
        .match(and_(document.col == query[0]['col'],
                    document.details.get_field(query[0]['key']).if_null(
                        None).in_(query[0]['values']))) \
        .project(document.row)

    # match rest of the columns
    for item in query[1:]:
        p.lookup(ds_classification[ds_id], local_field='row',
                 foreign_field='row', as_=item['col']) \
            .project(document.row, **{item['col']: filter_(
            lambda x: x.col == item['col'], document.get_field(item['col']))[
            0]}) \
            .match(
            document.get_field(item['col']).details.get_field(
                item['key']).if_null(None).in_(item['values'])) \
            .project(document.row)

    # match rows for the row id's
    p.lookup(ds[ds_id], local_field='row', foreign_field='_id', as_='row_data') \
        .replace_root(document.row_data[0])

    return list_response(conn.execute(p))


@conn.transactional
def add_ds(ds_id, csv_file):
    old_record = get_ds_list_active_record(csv_file.filename)
    stream = io.StringIO(csv_file.stream.read().decode('UTF8'), newline=None)
    csv_reader = csv.DictReader(stream)
    csv_rows = [csv_row for csv_row in csv_reader]
    app.logger.debug(csv_rows)
    conn.execute(insert_many(ds[ds_id], csv_rows))

    # update old collection status to "old"
    # and new collection status to "active"
    if old_record:
        update_ds_list_record(old_record['_id'], {'status': 'old'})
    update_ds_list_record(ds_id,
                          {'status': 'active', 'cols': csv_reader.fieldnames})


def process_ds(ds_id):
    def on_classify_done(future: Future):
        if not future.exception():
            call_get_details_for_all_cols(ds_id)

    call_classify_cells(ds_id, PatternClassifier()) \
        .add_done_callback(on_classify_done)


def update_ds_list_record(ds_id: Union[str, ObjectId], d: dict):
    conn.execute(update_one(ds_list) \
                 .filter(document._id == ObjectId(ds_id)) \
                 .set(d))


def create_ds_list_blank_record(csv_file, type, extra):
    record = {
        'name': csv_file.filename,
        'type': type,
        'status': 'blank',
        'extra': extra
    }
    if 'access' not in extra or 'type' not in extra['access'] or\
            extra['access']['type'] not in ['private', 'public']:
        raise Exception('extra.access.type: \'public\' | \'private\' must be set')
    if "user" in session:
        record['owner'] = session['user']['_id']
    return conn.execute(insert_one(ds_list, record)).inserted_id


def get_ds_list_active_record(name):
    return conn.execute(query_one(ds_list).filter(
        and_(ds_list.name == name, ds_list.status == 'active')))


@app.errorhandler(Exception)
def handle_exception(e: Exception):
    return error(e)


def error(e):
    app.logger.error(traceback.format_exc())
    return {'error': str(e)}, 500


def list_response(cursor: Iterable):
    return {
        'list': [serialize(record) for record in cursor],
        'success': True
    }


def _get_access_clause() -> Expression:
    clause = ds_list.extra.access.type == 'public'
    if "user" in session:
        return or_(clause, ds_list.owner == session["user"]['_id'])
    return clause


def _has_access(ds_id: Union[ObjectId, str]) -> bool:
    q = query_one(ds_list).filter(and_(ds_list._id == ObjectId(ds_id), _get_access_clause()))
    return conn.execute(q) is not None
