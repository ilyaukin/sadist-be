from flask import render_template

from app import app


@app.route('/privacy-policy')
def private_policy():
    return render_template('spa.html', root='policy.js')


@app.route('/tos')
def tos():
    return render_template('spa.html', root='tos.js')
