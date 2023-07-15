from flask import render_template, url_for

from app import app


@app.route('/privacy-policy')
def private_policy():
    return render_template('privacy-policy.html',
                           fe_root=url_for('static', filename='privacy-policy.js'))


@app.route('/tos')
def tos():
    return render_template('tos.html',
                           fe_root=url_for('static', filename='tos.js'))
