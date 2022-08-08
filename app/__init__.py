import logging
import logging.config
import os

import flask_session
from flask import Flask

import db

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'mongodb'
app.config['SESSION_MONGODB'] = db.conn.mongo_client()
app.config['SESSION_MONGODB_DB'] = 'sadist'
app.config['SESSION_MONGODB_COLLECT'] = 'app_user_session'
app.secret_key = os.environ.get('FLASK_SECRET_KEY') or 'test'
flask_session.Session(app)

if os.path.exists('logging.ini'):
    logging.config.fileConfig('logging.ini')

from . import root, user, labelling, debug
