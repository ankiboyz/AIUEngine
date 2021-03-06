# BCM's __init__.py
""" Here we are initializing the app for BCM
    Herein the code for the Flask blueprint registration for the
    BCM controls would also go in.
"""

from flask import Flask, g
import os
import BCM.logging_setup
from werkzeug.utils import import_string
import logging, commons.general_methods

from BCM.REALIZER.realizer import realizer_bp  # imported realizer_bp Blueprint from REALIZER

from .models import db
import config as config
# logger = logging.getLogger(__name__)
# print(logger.parent, 'parent of BCM logger')
# logger.info("logger set :D")

def configure_app(application):
    """ This method provides the configuration to the application - default < Override < env variables settings """

    """ This section is default configuration """
    # here we define the class that contains all the configurations
    # 'config.DevelopmentConfig' 'config.ProductionConfig'
    # This value would need be passed as environ variable it seems
    # APP_CONFIG = 'config.DevelopmentConfig'     # 'config.DevelopmentConfig'   'config.ProductionConfig'
    APP_CONFIG = config.APP_CONFIG_MODE     # taking value from configuration.

    cfg = import_string(
        APP_CONFIG)()  # all configurations even if they are @property of the class would also be loaded.

    application.config.from_object(cfg)  # will load all the uppercase attributes including properties
    application.config.from_object('config')  # also load the upper case attributes in the config outside of the class as well.

    """This section is if configuration's override is present i.e. there is a file config_overrides.py present in the 
    same path as this __init__.py file"""

    pwd = os.path.dirname(os.path.abspath(__file__))
    config_override_file_path = os.path.join(pwd, 'config_overrides.py')

    if os.path.exists(config_override_file_path):

        pwd_relative = os.path.relpath(pwd, start=os.curdir)
        print(f'For Configuration overrides, Relative path gathered is {pwd_relative}')
        application.config.from_object(pwd_relative + '.' + 'config_overrides')

    """ This override is provided from the environment variable APP_CONFIG.
    The environment variable is the path to the settings config file.
    Please note the settings file should have upper case settings keys with values.
    eg:    LOG_CNFG_PATH = 'log_cnfg_dflt.yaml'   
    """
    if os.environ.get("APP_CONFIG", None) is not None:
        application.config.from_envvar('APP_CONFIG')
        print("APP_CONFIG env variable value", os.environ["APP_CONFIG"])

    BCM.logging_setup.setup_logging(application)
    # Initializing with the DB of the APP as defined in the .models
    db.init_app(application)


app = Flask(__name__)


@app.before_request
def setup_request_context():
    ''' The values set here would be available per request, across the request's thread for the duration of the
    request execution '''

    # let's print the existing values in the g proxy
    print('values in the g when the request comes', g.__dict__)
    g.passport = commons.general_methods.generate_passport()
    print('values in the g when passport is set', g.__dict__)


# Configure the application
configure_app(app)

logger = logging.getLogger(__name__)
print(logger.parent, 'parent of BCM logger')
logger.info("logger set :D")


logger.info(f'CONFIGURATION:::: These are the APP Level settings: {app.config}')
print(app.config)  # try to have this information emit out only settings which have non null values first & then d rest

# Registering the views Blueprints
app.register_blueprint(realizer_bp, url_prefix='/BCM/realizer')
