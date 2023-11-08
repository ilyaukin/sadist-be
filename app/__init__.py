import logging
import logging.config
import os

from asgiref.wsgi import WsgiToAsgi
from flask import Flask

from config import config
from user_session import UserSessionInterface

app = Flask(__name__)
asgi_app = WsgiToAsgi(app)
app.session_interface = UserSessionInterface()
app.static_folder = 'static'
logger: logging.Logger = app.logger

if os.path.exists('logging.ini'):
    logging.config.fileConfig('logging.ini', disable_existing_loggers=False)

from . import root, error_handler, user, labelling, debug, web_proxy, web_crawler, extra_pages
