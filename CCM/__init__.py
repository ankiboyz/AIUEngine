# CCM's __init__.py
""" Here we are initializing the app for CCM
    Herein the code for the Flask blueprint registration for the
    CCM controls would also go in.
"""

from flask import Flask
import os
from werkzeug.utils import import_string

from CCM.PAY03.pay03 import pay03_bp  # imported pay03_bp Blueprint from PAY03

from .models import db


def configure_app(application):
    """ This method provides the configuration to the application - default < Override < env variables settings """

    """ This section is default configuration """
    # here we define the class that contains all the configurations
    # 'config.DevelopmentConfig' 'config.ProductionConfig'
    APP_CONFIG = 'config.DevelopmentConfig'
    cfg = import_string(
        APP_CONFIG)()  # all configurations even if they are @property of the class would also be loaded.

    application.config.from_object(cfg)  # will load all the uppercase attributes including properties
    # app.config.from_object('config')

    """This section is if configuration's override is present i.e. there is a file config_overrides.py present in the 
    same path as this __init__.py file"""

    pwd = os.path.dirname(os.path.abspath(__file__))
    config_override_file_path = os.path.join(pwd, 'config_overrides.py')

    if os.path.exists(config_override_file_path):
        pwd_relative = os.path.relpath(pwd, start=os.curdir)
        application.config.from_object(pwd_relative + '.' + 'config_overrides')

    """ This override is provided from the environment variable APP_CONFIG.
    The environment variable is the path to the settings config file.    
    """
    if os.environ.get("APP_CONFIG", None) is not None:
        application.config.from_envvar('APP_CONFIG')

        # Initializing with the DB of the APP as defined in the .models
    db.init_app(application)


app = Flask(__name__)

# Configure the application
configure_app(app)

print(app.config)

# Registering the views Blueprints
app.register_blueprint(pay03_bp, url_prefix='/CCM/pay03')
