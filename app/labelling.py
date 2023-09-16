import json
from typing import Callable

from app import app
import random

from bson import ObjectId
from db import conn, dl_session, dl_session_list, ds, \
    dl_master, dl_geo, geo_country, geo_city
from flask import request, render_template, url_for
from mongomoron import query_one, update_one, query, insert_one, index, \
    insert_many, document, Collection
from werkzeug.utils import redirect


class LabellingInterface(object):
    """
    Interface for labelling (assigning to each text some kind of label)
    """

    def __init__(self, type: str, prefix: str, collection: Collection):
        self._type = type
        self._prefix = prefix
        self._collection = collection

    def routes(self):
        def _route(rule: str, func: Callable, **options):
            app.add_url_rule(self._prefix + rule,
                             self._type + '.' + func.__name__,
                             func,
                             **options)

        _route('/session', self.session)
        _route('/session/<session_id>', self.next_sample, methods=['GET'])
        _route('/session/<session_id>', self.label_sample, methods=['POST'])
        _route('/session/<session_id>/merge', self.merge, methods=['POST'])
        _route('/session/<session_id>/resolve-conflicts',
               self.resolve_conflicts,
               methods=['POST'])

    def session(self):
        session_id = request.args.get('session_id', None)
        if not session_id:
            ds_id = request.args.get('ds_id', None)
            col = request.args.get('col', None)
            if not ds_id:
                raise Exception("For labelling session ds_id must be provided")
            limit = int(request.args.get('limit', 1000))

            # create new session based on data source
            # `ds_id` with `limit` capacity
            data = set()
            for record in conn.execute(query(ds[ds_id])):
                del record['_id']
                if col:
                    s = str(record[col]).strip()
                    if s:
                        data.add(s)
                else:
                    for value in record.values():
                        s = str(value).strip()
                        if s:
                            data.add(s)

            # any data from any columns (or `col` if specified) of the data
            # source
            # in random order
            session_values = list(data)
            random.shuffle(session_values)
            session_values = session_values[:limit]

            # add session to dl_session
            session_id = conn.execute(
                insert_one(dl_session_list, {'status': 'open', 'source': ds_id,
                                             'sourceCol': col,
                                             'type': self._type})
            ).inserted_id

            # create collection for session values
            session_collection = conn.create_collection(dl_session[session_id])
            conn.create_index(index(session_collection).asc('text').unique())
            conn.execute(
                insert_many(session_collection, [dict(text=value) for value in
                                                 session_values])
            )

            # redirect to session Page
            return redirect(
                self._prefix + '/session?session_id=' + str(session_id))

        # get the page for labelling given session.
        # return the page itself  with embed session_id and labels
        # (maybe better to return bare page and do async API call,
        # not sure)
        return render_template('spa.html',
                               root='labelling.js',
                               data={'type': self._type, 'prefix': self._prefix,
                                     'sessionId': session_id,
                                     'labels': self.labels()})

    def next_sample(self, session_id):
        sample = conn.execute(
            query_one(dl_session[session_id]).filter(
                dl_session[session_id].labels == None)
        )

        if sample:
            return {
                'text': sample['text'],
                'success': True
            }

        conn.execute(
            update_one(dl_session_list) \
                .filter(dl_session_list._id == ObjectId(session_id)) \
                .set({'status': 'finished'})
        )

        return {
            'status': 'finished',
            'success': True
        }

    def label_sample(self, session_id):
        text = request.form['text']
        text = text.replace('\r\n', '\n')
        label = request.form['label']
        # UI sends only one label, but we`ll save a list
        # to be able generalize in the future
        conn.execute(
            update_one(dl_session[session_id]) \
                .filter(document.text == text) \
                .set({'labels': [self._ftob(label)]})
        )

        return self.next_sample(session_id)

    @conn.transactional
    def merge(self, session_id):
        session_dict = dict((sample['text'], sample) for sample in
                            conn.execute(query(dl_session[session_id]).filter(
                                document.labels != None)))

        master_dict = dict((sample['text'], sample) for sample in
                           conn.execute(query(self._collection).filter(
                               document.text.in_(list(session_dict.keys())))))

        conflicts = []
        for text, master_sample in master_dict.items():
            session_sample = session_dict[text]
            master_labels = set(master_sample['labels'])
            session_labels = set(session_sample['labels'])

            if (master_labels == session_labels):
                pass
            elif session_sample.get('override', False):
                pass
            else:
                all_labels = master_labels.union(session_labels)
                diff = []
                for label in all_labels:
                    label_in_master = label in master_labels
                    label_in_session = label in session_labels
                    source = None
                    if label_in_master and label_in_session:
                        source = 'both'
                    elif label_in_master:
                        source = 'master'
                    elif label_in_session:
                        source = 'session'
                    diff.append({
                        'label': self._btof(label),
                        'source': source
                    })
                conflicts.append({
                    'text': text,
                    'diff': diff
                })

        if not conflicts:
            # insert new samples
            conn.execute(
                insert_many(self._collection,
                            ({'text': text, 'labels': sample['labels']} for
                             text, sample in
                             session_dict.items()
                             if text not in master_dict.keys()))
            )
            # update overridden samples
            for sample in filter(lambda sample: sample.get('override', False),
                                 session_dict.values()):
                conn.execute(
                    update_one(self._collection) \
                        .filter(document.text == sample['text']) \
                        .set({'labels': sample['labels']})
                )

            conn.execute(
                update_one(dl_session_list) \
                    .filter(dl_session_list._id == ObjectId(session_id))
                    .set({'status': 'merged'})
            )
            return {
                'status': 'merged',
                'success': True
            }

        conn.execute(
            update_one(dl_session_list) \
                .filter(dl_session_list._id == ObjectId(session_id))
                .set({'status': 'merging'})
        )
        return {
            'status': 'merging',
            'conflicts': conflicts,
            'success': True
        }

    def resolve_conflicts(self, session_id):
        samples_string = request.form['samples']
        samples = json.loads(samples_string)

        for sample in samples:
            if not isinstance(sample['labels'], list) or not sample['labels']:
                raise Exception(
                    'Please specify labels for ' + repr(sample['text']))
            conn.execute(
                update_one(dl_session[session_id]) \
                    .filter(document.text == sample['text']) \
                    .set({'labels': [self._ftob(label) for label in
                                     sample['labels']], 'override': True})
            )

        return self.merge(session_id)

    def labels(self) -> list:
        """
        Labels of the given type of labelling interface,
        in the format needed by frontend
        """
        return []

    def _btof(self, label):
        return label

    def _ftob(self, label):
        return label


class ClassLabellingInterface(LabellingInterface):
    """
    Labelling for a class of text (such as phrase, number, city etc.)
    """

    def __init__(self):
        super(ClassLabellingInterface, self).__init__(type='class',
                                                      prefix='/dl',
                                                      collection=dl_master)

    def labels(self) -> list:
        return ['city', 'country',
                'timestamp', 'money',
                'profession', 'phrase',
                'number', 'trash']


class GeoLabellingInterface(LabellingInterface):
    """
    Labelling interface for geo locations - cities and countries
    """

    def __init__(self):
        super(GeoLabellingInterface, self).__init__(type='geo', prefix='/dlgeo',
                                                    collection=dl_geo)

    def labels(self) -> list:
        countries = dict((country['_id'], country['name']) for country in
                         conn.execute(query(geo_country)))

        return [{'value': 'null,null', 'text': '-'}] \
               + [{'value': 'null,%s' % _id, 'text': name} for _id, name in
                  countries.items()] \
               + [{'value': '%s,%s' % (city['_id'], city['country_code']),
                   'text': '%s, %s' % (
                       city['name'], countries[city['country_code']])} for city
                  in
                  conn.execute(query(geo_city))]

    def _btof(self, label: dict):
        city_id = label.get('city_id', None)
        country_id = label.get('country_id', None)

        if country_id:
            country = conn.execute(
                query_one(geo_country).filter(geo_country._id == country_id))
        else:
            country = {'_id': 'null', 'name': '-'}
        if city_id:
            city = conn.execute(
                query_one(geo_city).filter(geo_city._id == city_id))
        else:
            city = {'_id': 'null', 'name': '-'}

        return {'value': '%s,%s' % (city['_id'], country['_id']),
                'text': '%s, %s' % (city['name'], country['name'])}

    def _ftob(self, label: str):
        [city_id, country_id] = label.split(',')
        result = {}
        if city_id != 'null':
            result.update({'city_id': city_id})
        if country_id != 'null':
            result.update({'country_id': country_id})
        return result


# register all routes
ClassLabellingInterface().routes()
GeoLabellingInterface().routes()
