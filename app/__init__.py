import logging
import logging.config
import os

from flask import Flask

app = Flask(__name__)

if os.path.exists('logging.ini'):
    logging.config.fileConfig('logging.ini')

from . import root, labelling, debug
