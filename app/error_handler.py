import traceback

import werkzeug.exceptions

from app import app, logger


def error(e):
    logger.error(traceback.format_exc())
    return {'error': e.__class__.__name__ + ': ' + str(e)}, 500


@app.errorhandler(Exception)
def handle_exception(e: Exception):
    return error(e)


@app.errorhandler(werkzeug.exceptions.HTTPException)
def handle_http_exception(e: werkzeug.exceptions.HTTPException):
    return {'error': e.name}, e.code

