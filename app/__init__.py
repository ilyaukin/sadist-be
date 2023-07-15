import logging
import logging.config
import os

from flask_session import Session
from flask import Flask

from config import config
from user_session import UserSessionInterface

app = Flask(__name__)
Session(app)
app.session_interface = UserSessionInterface()

if os.path.exists('logging.ini'):
    logging.config.fileConfig('logging.ini', disable_existing_loggers=False)

from . import root, user, labelling, debug, extra_pages
