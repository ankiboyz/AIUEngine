# logging_setup.py
""" Here, we will add some contextual information to the logging.
Amongst others there is a PassportNUmber that is been set in the
contextual information. This Passport Number will bind all the calls
happened during a call to this application. There might be different requirements for contextual
logging as per the APP (here it's CCM) , hence defining the way logs been set up per APP."""

import logging
from flask import request
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
        log_record.url = request.path
        log_record.method = request.method
        log_record.ip = request.environ.get("REMOTE_ADDR")

        return True

