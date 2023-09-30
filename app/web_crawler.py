from typing import List, Any, Optional, Dict

import pyjsparser
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
    sanitizer.sanitize_script_template(script_template_data)
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


class _Sanitizer(object):
    _function_body_map: Optional[Dict[str, List[Any]]] = None

    def sanitize_script_template(self, script_template_data: str):
        """
        Sanitize script template to ensure there is no malicious code,
        uploaded by a user,
        to break the web site for other users.
        Because of checking malicious code in common is not a trivial task,
        we check for our particular use case of how new script templates are
        generated, in particular all methods of the template must be the same as ones
        of our pre-defined templates, all non-function fields can contain any string literal
        (if "text" itself contains malicious script, it does not matter,
        because a user sees it before execution)
        @param script_template_data: Script template as a string (JS object)
        @return: None; if check fails, throw an Exception
        """
        tree = pyjsparser.parse("var a = " + script_template_data)
        assert len(tree["body"]) == 1
        assert len(tree["body"][0]["declarations"]) == 1
        script_template_properties = tree["body"][0]["declarations"][0]["init"]["properties"]
        for property in script_template_properties:
            prop_name = property["key"]["value"]
            prop_type = property["value"]["type"]
            if prop_type == "FunctionExpression":
                function_body = property["value"]["body"]
                assert function_body in self._get_allowed_function_bodies(prop_name)
            elif prop_type == "Literal":
                prop_value = property["value"]["value"]
                assert isinstance(prop_value, str)
            else:
                assert False

    def _get_allowed_function_bodies(self, function_name: str) -> List[Any]:
        if self._function_body_map is None:
            self._function_body_map = {}
            for script_template in conn.execute(
                    query(wc_script_template).filter(document.readonly == True)):
                data = script_template['data']
                tree = pyjsparser.parse('var a = ' + data)
                script_template_properties = tree["body"][0]["declarations"][0]["init"]["properties"]
                for property in script_template_properties:
                    prop_name = property["key"]["value"]
                    prop_type = property["value"]["type"]
                    if prop_type == "FunctionExpression":
                        function_body = property["value"]["body"]
                        self._function_body_map.setdefault(prop_name, [])
                        self._function_body_map[prop_name].append(function_body)
        return self._function_body_map.get(function_name, [])


sanitizer = _Sanitizer()
