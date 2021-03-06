import faulthandler
import datetime

from app import app
from flask import make_response


@app.route('/debug/traceback')
def traceback():
    tmpfilename = '/tmp/' + datetime.datetime.now().strftime('%Y%m%d%H%M%S'
                                                             '.traceback')
    output = open(tmpfilename, 'w')
    faulthandler.dump_traceback(output, all_threads=True)
    output.close()
    response = make_response(open(tmpfilename, 'r').read())
    response.mimetype = 'text/plain'
    return response
