import csv
import io
import json
from concurrent.futures._base import Future
from typing import Union, Iterable, Optional

import mongomoron.mongomoron
from bson import ObjectId
from flask import render_template, request, session
from mongomoron import insert_many, query, document, aggregate, push_, dict_, \
    filter_, and_, update_one, insert_one, query_one, or_
from mongomoron.mongomoron import Expression, avg, sum_, min_, max_

from app import app
from classification import PatternClassifier, \
    call_classify_cells
from db import conn, ds, ds_list, ds_classification
from detailization import call_get_details_for_all_cols
from error_handler import error
from serializer import serialize


@app.route('/')
def root():
    return render_template('spa.html')


@app.route('/ds', methods=['PUT'])
def create_ds():
    csv_file = request.files['csv']
    ds_type = request.form['type']
    ds_extra_string = request.form.get('extra', '{}')
    ds_extra = json.loads(ds_extra_string)

    ds_id = _create_ds_list_blank_record(csv_file, ds_type, ds_extra)

    conn.create_collection(ds[ds_id])
    try:
        _add_ds(ds_id, csv_file)
    except Exception as e:
        _update_ds_list_record(ds_id, {'status': 'failed', 'error': str(e)})
        # delete failed collection
        conn.drop_collection(ds[ds_id])
        return error(e)

    _process_ds(ds_id)

    return {
        'item': serialize(_get_ds_list_active_record(csv_file.filename)),
        'success': True
    }


@app.route('/ls')
def list_ds():
    q = query(ds_list).filter(_get_access_clause()).filter(document.status == 'active')
    _id = request.args.get('id')
    if (_id):
        q.filter(document._id == ObjectId(_id))

    return _list_response(conn.execute(q))


@app.route('/ds/<ds_id>')
def get_ds(ds_id):
    if not _has_access(ds_id):
        return _list_response([])

    return _list_response(conn.execute(query(ds[ds_id])))


@app.route('/ds/<ds_id>/visualize')
def visualize_ds(ds_id):
    if not _has_access(ds_id):
        return _list_response([])

    # visualization pipeline (not to be confused with aggregation pipeline).
    # format is defined in the frontend repo.
    pipeline_str = request.args['pipeline']
    pipeline = json.loads(pipeline_str)

    # build aggregation pipeline.
    # first, combine details of the all involved columns,
    # in the format {_id: <row id>, <col>: <detail>}
    # todo: support different details of the same col in the same pipeline
    # todo 2: support literal values if there's no label (from ds_xxx rather than ds_xxx_classification collection)
    pipeline1 = [item for item in pipeline if 'col' in item and 'label' in item]
    p = aggregate(ds_classification[ds_id]) \
        .match(document.col.in_(item['col'] for item in pipeline1)) \
        .group(document.row, col_details=push_(
        dict_(col=document.col, details=document.details))) \
        .project(**dict((item['col'], filter_(lambda x, item=item: x.col == item['col'],
                                              document.col_details)[0]) for item
                        in pipeline1)) \
        .project(**dict((item['col'],
                         document.get_field(item['col']).details.get_field(
                             item['label'])) for item in pipeline1))

    # make group by all items with action="group",
    # and fields with all items with action="accumulate"
    fields = dict()
    reference = document
    for i in reversed(range(len(pipeline))):
        item = pipeline[i]
        action = item['action']
        key = item.get('key', 'f%i' % i)
        col = item.get('col')
        if action == 'accumulate':
            accumulator = item.get('accumulater')
            if accumulator == 'count':
                fields[key] = sum_(1)
            elif accumulator == 'average':
                fields[key] = avg(document.get_field(col))
            elif accumulator == 'min':
                fields[key] = min_(document.get_field(col))
            elif accumulator == 'max':
                fields[key] = max_(document.get_field(col))
            else:
                app.logger.warn("Accumulator %s not implemented, skip %s" % (accumulator, key))
                fields[key] = None
        elif action == 'group':
            if i:
                _id = dict_(**dict((item1['col'], reference.get_field(item1['col'])) for item1 in pipeline[:i + 1]))
                reference = document._id
            elif col:
                _id = reference.get_field(col)
            else:
                _id = None
            p.group(_id, **fields)
            fields = {col: push_(dict_(_id=reference.get_field(col),
                                       **dict((key1, document.get_field(key1)) for key1 in fields.keys())))}
        else:
            app.logger.warn("Action %s not implemented, skip" % action)

    cursor = conn.execute(p)
    result = list(cursor)

    # todo here I aimed to do post-processing, but not implemented for now

    return _list_response(result)


@app.route('/ds/<ds_id>/filter')
def filter_ds(ds_id):
    if not _has_access(ds_id):
        return _list_response([])

    query_str = request.args['query']
    query = json.loads(query_str)

    # if query is empty, return all rows
    if not query:
        return get_ds(ds_id)

    query_labeled = [item for item in query if 'label' in item]
    query_raw = [item for item in query if 'label' not in item]

    def parse_predicate(predicate: Optional[dict], arg: mongomoron.mongomoron.Expression) -> Optional[mongomoron.mongomoron.Expression]:
        # parse JSON predicate into expression
        if not predicate:
            app.logger.warn("Predicate is empty")
            return None

        if predicate['op'] == 'eq':
            expr = arg == predicate['value']
        elif predicate['op'] == 'in':
            expr = arg.in_(predicate['values'])
        else:
            app.logger.warn("Predicate operation %s not implemented" % predicate['op'])
            expr = None
        return expr

    p: Optional[mongomoron.mongomoron.AggregationPipelineBuilder] = None
    for item in query_labeled:
        if not p:
            p = aggregate(ds_classification[ds_id]) \
                .match(document.col == item['col'])
        else:
            p = p.lookup(ds_classification[ds_id], local_field='row',
                         foreign_field='row', as_='f') \
                .project(document.row,
                         f=filter_(lambda x, item=item: x.col == item['col'], document.f)[0]) \
                .project(document.row, details=document.f.details)
        expr = parse_predicate(item.get('predicate'),
                               document.details.get_field(item['label']).if_null(None))
        if expr:
            p = p.match(expr).project(document.row)

    # match rows for the row id's
    if not p:
        # use aggregate instead of query here to have the same interface
        p = aggregate(ds[ds_id])
    else:
        p.lookup(ds[ds_id], local_field='row', foreign_field='_id', as_='row_data')\
            .replace_root(document.row_data[0])

    for item in query_raw:
        expr = parse_predicate(item.get('predicate'),
                               document.get_field(item['col']))
        if expr:
            p.match(expr)

    return _list_response(conn.execute(p))


@app.route('/ds/<ds_id>/label-values')
def get_label_values(ds_id):
    if not _has_access(ds_id):
        return _list_response([])

    col = request.args['col']
    label = request.args['label']

    p = aggregate(ds_classification[ds_id])\
        .match(document.col == col)\
        .project(v=document.details.get_field(label))\
        .group(None, vv=mongomoron.push_unique(document.v))

    return _list_response(list(conn.execute(p))[0]['vv'])


@conn.transactional
def _add_ds(ds_id, csv_file):
    old_record = _get_ds_list_active_record(csv_file.filename)
    stream = io.StringIO(csv_file.stream.read().decode('UTF8'), newline=None)
    csv_reader = csv.DictReader(stream)
    csv_rows = [{'_id': i, **csv_row} for i, csv_row in enumerate(csv_reader)]
    app.logger.debug(csv_rows)
    conn.execute(insert_many(ds[ds_id], csv_rows))

    # update old collection status to "old"
    # and new collection status to "active"
    if old_record:
        _update_ds_list_record(old_record['_id'], {'status': 'old'})
    _update_ds_list_record(ds_id,
                           {'status': 'active', 'cols': csv_reader.fieldnames})


def _process_ds(ds_id):
    def on_classify_done(future: Future):
        if not future.exception():
            call_get_details_for_all_cols(ds_id)

    call_classify_cells(ds_id, PatternClassifier()) \
        .add_done_callback(on_classify_done)


def _update_ds_list_record(ds_id: Union[str, ObjectId], d: dict):
    conn.execute(update_one(ds_list) \
                 .filter(document._id == ObjectId(ds_id)) \
                 .set(d))


def _create_ds_list_blank_record(csv_file, type, extra):
    record = {
        'name': csv_file.filename,
        'type': type,
        'status': 'blank',
        'extra': extra
    }
    if 'access' not in extra or 'type' not in extra['access'] or \
            extra['access']['type'] not in ['private', 'public']:
        raise Exception('extra.access.type: \'public\' | \'private\' must be set')
    if "user" in session:
        record['owner'] = session['user']['_id']
    return conn.execute(insert_one(ds_list, record)).inserted_id


def _get_ds_list_active_record(name):
    return conn.execute(query_one(ds_list).filter(
        and_(ds_list.name == name, ds_list.status == 'active')))


def _list_response(cursor: Iterable):
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
