# CCM's __init__.py
""" Here we are initializing the app for CCM
    Herein the code for the Flask blueprint registration for the
    CCM controls would also go in.
"""

from flask import Flask
from CCM.PAY03.pay03 import pay03_bp  # imported pay03_bp Blueprint from PAY03

from .models import db

app = Flask(__name__)
app.config.from_object('config')

db.init_app(app)

app.register_blueprint(pay03_bp, url_prefix='/CCM/pay03')
