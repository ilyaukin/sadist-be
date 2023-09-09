import pymongo
from flask import request, make_response
from mongomoron import query_one, document, update_one, query

from app import app
from db import conn, wc_script_template
from error_handler import error


@app.route('/wc/template', methods=['PUT'])
def create_script_template():
    content_type = request.headers.get('Content-Type')
    script_template_name = request.headers.get('X-Template-Name')
    script_template_data = request.get_data(as_text=True)

    # script template is passed in serialize-javascript format (superset of JSON
    # and subset of JS); we don't verify it here. only verify headers
    if content_type != 'application/x-script-template' or not script_template_name:
        return error(Exception('Invalid script template'))

    # check if it's one of readonly pre-defined templates
    t = conn.execute(query_one(wc_script_template).filter(document._id == script_template_name))
    if t and t['readonly']:
        return error(Exception('Script template is readonly'))

    # write with upsert
    # TODO sanitize script. since it will be eval()'ed in users' browser!!!
    conn.execute(update_one(wc_script_template, upsert=True)
                 .filter(document._id == script_template_name)
                 .set({'data': script_template_data, 'readonly': False}))

    return {"success": True}


@app.route('/wc/template', methods=['GET'])
def list_script_template():
    cursor = conn.execute(query(wc_script_template)) \
        .sort([('readonly', pymongo.DESCENDING), ('_id', pymongo.ASCENDING)])
    data = '[' + ','.join([record['data'] for record in cursor]) + ']'
    response = make_response()
    response.stream.write(bytearray(data, 'utf-8'))
    response.headers.set('content-type', 'text/plain; charset=utf-8')
    return response
