import json
import random

from app import app
from bson import ObjectId
from db import conn, dl_session, dl_session_list, ds, \
    dl_master
from flask import request, redirect, render_template, url_for
from mongomoron import query_one, update_one, query, insert_one, index, \
    insert_many, document


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
        data = set()
        for record in conn.execute(query(ds[ds_id])):
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
        session_id = conn.execute(
            insert_one(dl_session_list, {'status': 'open', 'source': ds_id})
        ).inserted_id

        # create collection for session values
        session_collection = conn.create_collection(dl_session[session_id])
        conn.create_index(index(session_collection).asc('text').unique())
        conn.execute(
            insert_many(session_collection, [dict(text=value) for value in
                                             session_values])
        )

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


@app.route('/dl/session/<session_id>', methods=['POST'])
def label_sample(session_id):
    text = request.form['text']
    text = text.replace('\r\n', '\n')
    label = request.form['label']
    # UI sends only one label, but we`ll save a list
    # to be able generalize in the future
    conn.execute(
        update_one(dl_session[session_id]) \
            .filter(document.text == text) \
            .set({'labels': [label]})
    )

    return next_sample(session_id)


@app.route('/dl/session/<session_id>/merge', methods=['POST'])
@conn.transactional
def merge(session_id):
    session_dict = dict((sample['text'], sample) for sample in
                        conn.execute(query(dl_session[session_id]).filter(
                            document.labels != None)))

    master_dict = dict((sample['text'], sample) for sample in
                       conn.execute(query(dl_master).filter(
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
                    'label': label,
                    'source': source
                })
            conflicts.append({
                'text': text,
                'diff': diff
            })

    if not conflicts:
        # insert new samples
        conn.execute(
            insert_many(dl_master,
                        ({'text': text, 'labels': sample['labels']} for
                         text, sample in
                         session_dict.items()
                         if text not in master_dict.keys()))
        )
        # update overridden samples
        for sample in filter(lambda sample: sample.get('override', False),
                             session_dict.values()):
            conn.execute(
                update_one(dl_master) \
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


@app.route('/dl/session/<session_id>/resolve-conflicts', methods=['POST'])
def resolve_conflicts(session_id):
    samples_string = request.form['samples']
    samples = json.loads(samples_string)

    for sample in samples:
        if not isinstance(sample['labels'], list) or not sample['labels']:
            raise Exception('Please specify labels for ' + repr(sample['text']))
        conn.execute(
            update_one(session[session_id]) \
                .filter(document.text == label_sample['text']) \
                .set({'labels': sample['labels'], 'override': True})
        )

    return merge(session_id)


def labels() -> list:
    # hardcoded so far
    return ['city', 'country',
            'timestamp', 'money',
            'profession', 'phrase',
            'number', 'trash']
