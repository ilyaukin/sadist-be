import json
import random

from app import app, _db, _set, transactional
from bson import ObjectId
from flask import request, redirect, render_template, url_for


@app.route('/dl/session')
def session():
    session_id = request.args.get('session_id', None)
    if not session_id:
        ds_id = request.args.get('ds_id', None)
        if not ds_id:
            raise Exception("For labelling session ds_id must be provided")
        limit = int(request.args.get('limit', 1000))

        # create new session based on data soucre
        # `ds_id` with `limit` capacity
        collection_name = 'ds_%s' % ds_id
        ds = _db()[collection_name]
        data = set()
        for record in ds.find():
            del record['_id']
            for value in record.values():
                s = str(value).strip()
                if s: data.add(s)

        # any data from any columns of the data source
        # in random order
        session_values = list(data)
        random.shuffle(session_values)
        session_values = session_values[:limit]

        # add session to dl_session
        session_id = _db()['dl_session'] \
            .insert_one({'status': 'open', 'source': ds_id}) \
            .inserted_id

        # create collection for session values
        collection_name = 'dl_session_%s' % session_id
        _db().create_collection(collection_name)
        session_collection = _db()[collection_name]
        session_collection.create_index('text', unique=True)
        session_collection.insert_many({'text': value} for value in session_values)

        # redirect to session Page
        return redirect('/dl/session?session_id=' + str(session_id))

    # get the page for labelling given session.
    # return the page itself  with embed session_id and labels
    # (maybe better to return bare page and do async API call,
    # not sure)
    return render_template('labelling.html',
                           fe_root=url_for('static', filename='labelling.js'),
                           data={'sessionId': session_id, 'labels': labels()})


@app.route('/dl/session/<session_id>', methods=['GET'])
def next_sample(session_id):
    collection_name = 'dl_session_%s' % session_id
    session_collection = _db()[collection_name]
    sample = session_collection.find_one({'labels': None})

    if sample:
        return {
            'text': sample['text'],
            'success': True
        }

    _db()['dl_session'].update_one({'_id': ObjectId(session_id)}, _set({'status': 'finished'}))
    return {
        'status': 'finished',
        'success': True
    }


@app.route('/dl/session/<session_id>', methods=['POST'])
def label_sample(session_id):
    collection_name = 'dl_session_%s' % session_id
    session_collection = _db()[collection_name]
    text = request.form['text']
    label = request.form['label']
    # UI sends only one label, but we`ll save a list
    # to be able generalize in the future
    session_collection.update_one({'text': text}, _set({'labels': [label]}))

    return next_sample(session_id)


@app.route('/dl/session/<session_id>/merge', methods=['POST'])
@transactional
def merge(session_id, session=None):
    collection_name = 'dl_session_%s' % session_id
    session_collection = _db()[collection_name]
    master_collection = _db()['dl_master']

    session_dict = dict((sample['text'], sample) for sample in
                        session_collection.find({'labels': {'$ne': None}}))

    master_dict = dict((sample['text'], sample) for sample in
                       master_collection.find({'text': {'$in': list(session_dict.keys())}}))

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
                    'label': label,
                    'source': source
                })
            conflicts.append({
                'text': text,
                'diff': diff
            })

    if not conflicts:
        # insert new samples
        master_collection.insert_many(
            ({'text': text, 'labels': sample['labels']} for text, sample in session_dict.items()
             if text not in master_dict.keys()),
            session=session
        )
        # update overridden samples
        for sample in filter(lambda sample: sample.get('override', False), session_dict.values()):
            master_collection.update_one({'text': sample['text']}, _set({'labels': sample['labels']}),
                                         session=session)

        _db()['dl_session'].update_one({'_id': ObjectId(session_id)}, _set({'status': 'merged'}),
                                       session=session)
        return {
            'status': 'merged',
            'success': True
        }

    _db()['dl_session'].update_one({'_id': ObjectId(session_id)}, _set({'status': 'merging'}),
                                   session=session)
    return {
        'status': 'merging',
        'conflicts': conflicts,
        'success': True
    }


@app.route('/dl/session/<session_id>/resolve-conflicts', methods=['POST'])
def resolve_conflicts(session_id):
    samples_string = request.form['samples']
    samples = json.loads(samples_string)

    collection_name = 'dl_session_%s' % session_id
    session_collection = _db()[collection_name]

    for sample in samples:
        if not isinstance(sample['labels'], list) or not sample['labels']:
            raise Exception('Please specify labels for ' + repr(sample['text']))
        session_collection.update_one({'text': sample['text']},
                                      _set({'labels': sample['labels'], 'override': True}))

    return merge(session_id)


def labels() -> list:
    # hardcoded so far
    return ['city', 'country',
            'timestamp', 'money',
            'profession', 'phrase',
            'number', 'trash']
