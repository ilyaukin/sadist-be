import traceback

from app import app


def error(e):
    app.logger.error(traceback.format_exc())
    return {'error': e.__class__.__name__ + ': ' + str(e)}, 500


@app.errorhandler(Exception)
def handle_exception(e: Exception):
    return error(e)
