import logging

from flask import Flask

app = Flask(__name__)
# TODO move all logging settings to the config file
app.logger.setLevel('DEBUG')
logging.getLogger('mongomoron').setLevel('DEBUG')

from . import root, labelling
