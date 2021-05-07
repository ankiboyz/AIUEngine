# services.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods invoked from the web services' call to this CCM control.
                This module is the service provide to the front ending calls received by the Python APP.

    Input: Would be the specific method with arguments received from the the frontend.

    Output: returns the structured response (error/success) object to the front end ie the rest call receiver; which
            further emits out the structured response to the caller.
"""

import logging
from datetime import datetime
from CCM import models
# from flask_sqlalchemy import SQLAlchemy
from CCM.app_scope_methods import ccm_sequences
from commons import structured_response

db = models.db

logger = logging.getLogger(__name__)

class CCMHeader:
    def __init__(self):
        pass

    # kwargs are supposed to gather the value of the keys passed to it.
    def insert(self, **kwargs):
        self.check_b4_insert()
        print(kwargs)  # putting it as **kwargs would enforce it as if input params are passed to the print method.


        # Object of the class should be created at the run time itself so the same issue
        # which comes when a immutable object is passed as a default value for the method(wherein the value is gathered
        # at the method creation time) should not come here.

        ccmhdr = models.CCMMonitorHDR(**kwargs)
        ccmhdr.created_date = datetime.now()
        ccmhdr.updated_date = datetime.now()

        ret_op = ccm_sequences.sequences_provider(ip_tablename='glt_ccm_xtnd_monitor_header',
                                                  ip_columnname='id',
                                                  ip_batchsize=1)
        ccmhdr.id = ret_op['start_value']  # since batchsize was given as 1, so start and end will be same.
        db.session.add(ccmhdr)

        try:
            db.session.commit()
            db.session.close()  # under observation in order to minimize sessions remained open

        except Exception as error:
            logger.error(error)
            # print('error', error)
            db.session.rollback()
            db.session.close()  # under observation in order to minimize sessions remained open

            # Converting into standard ERROR response
            std_err_resp_dict = structured_response.StructuredResponse(error, 'SQLAlchemyException')\
                .structure_the_error()

            print('std_err_resp_dict', std_err_resp_dict)
            return std_err_resp_dict

        # Formulating the standard SUCCESS response
        std_success_resp_dict = structured_response.StructuredResponse(dict(), 'GenericSuccess') \
            .structure_the_success()

        print(std_success_resp_dict)
        return std_success_resp_dict

    def check_b4_insert(self):
        pass

    def publish_to_kafka_topic(self, topic_name='DEFAULT'):
        ''' Here we will publish to the topic for the specific control '''
        pass
