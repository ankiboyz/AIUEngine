# logging_setup.py
""" Here, we will add some contextual information to the logging.
Amongst others there is a PassportNUmber that is been set in the
contextual information. This Passport Number will bind all the calls
happened during a call to this application. There might be different requirements for contextual
logging as per the APP (here it's CCM) , hence defining the way logs been set up per APP."""

import logging
import logging.config   # module in a package
# from flask import request
import flask
from datetime import datetime
import random
import yaml


def generate_passport():
    now = datetime.now()
    date_time_concat = now.strftime("%Y%m%d%H%M%S%f")
    random_num = random.randint(1000, 9999)      # Generating a 4 digit random number
    now_an_id = str(date_time_concat)+str(random_num)

    print('Passport Number', now_an_id)
    return now_an_id


class ContextualFilter(logging.Filter):
    def filter(self, log_record):

        passport_num = generate_passport()
        log_record.passport = passport_num

        if flask.has_request_context():
            log_record.url = flask.request.path
            log_record.method = flask.request.method
            log_record.ip = flask.request.environ.get("REMOTE_ADDR")

        return True


def setup_logging(app):

    ''' This method will set up the logging configuration as per defined in the yaml file .
        The configuration file for logs to be considered is defined by the LOG_CNFG_PATH variable
        of the configurations.
        The configurations are resolved as per the config resolver method.
        We will check if the app.config refers to DEBUG as True then logger level will set as
        DEBUG else it will be set as INFO.
    '''

    # this is the value we need to gather from the application configuration.
    is_debug = app.config['DEBUG']
    path = app.config['LOG_CNFG_PATH']

    with open('C:\\Users\\ASaxena\\PycharmProjects\\AIUEngine\\CCM\\log_cnfg.yaml', 'r') as f:
        log_cnfg_settings = yaml.safe_load(f.read())
        logging.config.dictConfig(log_cnfg_settings)
