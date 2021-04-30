# services.py
""" This module will provide various methods invoked from the web services call to this CCM control"""
from datetime import datetime
from CCM import models
from flask_sqlalchemy import SQLAlchemy
from CCM.app_scope_methods import ccm_sequences

db = SQLAlchemy()


class CCMHeader:
    def __init__(self):
        pass

    # kwargs are supposed to gather the value of the keys passed to it.
    def insert(self, **kwargs):
        self.check_b4_insert()
        print(kwargs)  # putting it as **kwargs would enforce it as if input params are passed to the print method.


        # Object of the class should be created at the run time itself so the same issue
        # which comes when a immutable object is passed as a default value for the method(wherein the value is gathered
        # at the method creation time) should not come here,  should not happen here.

        ccmhdr = models.CCMMonitorHDR(**kwargs)
        ccmhdr.created_date = datetime.now()
        ccmhdr.updated_date = datetime.now()

        ret_op = ccm_sequences.sequences_provider(ip_tablename='glt_ccm_xtnd_ccm_header',
                                                  ip_columnname='id',
                                                  ip_batchsize=1)
        ccmhdr.id = ret_op['start_value']  # since batchsize was given as 1, so start and end will be same.
        db.session.add(ccmhdr)
        db.session.commit()

    def check_b4_insert(self):
        pass

    def publish_to_kafka_topic(self, topic_name='DEFAULT'):
        pass
