# CCM's __init__.py
""" Here we are initializing the app for CCM
    Herein the code for the Flask blueprint registration for the
    CCM controls would also go in.
"""

from flask import Flask
from CCM.PAY03.pay03 import pay03_bp  # imported pay03_bp Blueprint from PAY03
from werkzeug.utils import import_string

from .models import db

# here we define the class that contains all the configurations
# 'config.DevelopmentConfig' 'config.ProductionConfig'
APP_CONFIG = 'config.DevelopmentConfig'
cfg = import_string(APP_CONFIG)()  # all configurations even if they are @property of the class would also be loaded.

app = Flask(__name__)

app.config.from_object(cfg)  # will load all the uppercase attributes including properties
# app.config.from_object('config')

# Initializing with the DB of the APP as defined in the .models
db.init_app(app)

# Registering the views Blueprints
app.register_blueprint(pay03_bp, url_prefix='/CCM/pay03')
