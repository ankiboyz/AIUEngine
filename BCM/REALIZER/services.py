# services.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods invoked from the web services' call to this BCM control.
                This module is the service provide to the front ending calls received by the Python APP.

    Input: Would be the specific method with arguments received from the the frontend.

    Output: returns the structured response (error/success) object to the front end ie the rest call receiver; which
            further emits out the structured response to the caller.
"""

import logging
from datetime import datetime
from flask import current_app

import config
from BCM import models
# from flask_sqlalchemy import SQLAlchemy
from BCM.app_scope_methods import ccm_sequences
from commons import structured_response

# print('BCM  ', dir(BCM))
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json

db = models.db

logger = logging.getLogger(__name__)
# # logger.setLevel(BCM.app.config["LOG_LEVEL"])   # not working here
# logger.setLevel(config.LOG_LEVEL) #
# # print('current_app.config', current_app.config)
# print('logger.getEffectiveLevel()', logger.getEffectiveLevel(), logging.getLevelName(logger.getEffectiveLevel()))
# logger.info(f'Logger\'s effective level is {logger.getEffectiveLevel()} and level name is {logging.getLevelName(logger.getEffectiveLevel())}')
# logger.warning("Loggers effective level is {logger.getEffectiveLevel()} and level name is {logging.getLevelName(logger.getEffectiveLevel())}")
# logger.debug("Loggers effective level is {logger.getEffectiveLevel()} and level name is {logging.getLevelName(logger.getEffectiveLevel())}")


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

        # here all the input values those have will be taken in , key denotes name of the table column
        ccmhdr = models.BCMMonitorHDR(**kwargs)
        ccmhdr.created_date = datetime.now()
        ccmhdr.updated_date = datetime.now()

        # this provides the string repr of the object returned, string is needed else error in the sequence provider
        bcm_monitor_hdr_table_name = f'{models.BCMMonitorHDR.__dict__["__table__"]}'

        print('bcm_monitor_hdr_table_name', bcm_monitor_hdr_table_name, type(bcm_monitor_hdr_table_name))
        # ret_op = ccm_sequences.sequences_provider(ip_tablename='glt_ccm_xtnd_monitor_header',
        #                                           ip_columnname='id',
        #                                           ip_batchsize=1)
        # this also to be under try purview
        ret_op = models.sequences_provider(ip_tablename=bcm_monitor_hdr_table_name,
                                           ip_columnname='id',
                                           ip_batchsize=1, appln=current_app)
        ccmhdr.id = ret_op['start_value']  # since batchsize was given as 1, so start and end will be same.
        db.session.add(ccmhdr)

        # Setting up the variables which would be used to publish to Kafka Topic
        ccmhdr_control_id = ccmhdr.control_id
        data_feed_dict = {"ID": ccmhdr.id, "CONTROL_ID": ccmhdr.control_id}
        logger.debug(f'Data to be submitted to Kafka: ID is {ccmhdr.id} and CONTROL_ID is {ccmhdr.control_id}')
        try:
            db.session.commit()
            db.session.close()  # under observation in order to minimize sessions remained open

        except Exception as error:
            logger.error(error, exc_info=True)
            # print('error', error)
            db.session.rollback()
            db.session.close()  # under observation in order to minimize sessions remained open,
                                # session close returns the connection to the pool, for transactions within a connection with db.session.begin

            # Converting into standard ERROR response
            std_err_resp_dict = structured_response.StructuredResponse(error, 'SQLAlchemyException')\
                .structure_the_error()

            print('std_err_resp_dict', std_err_resp_dict)
            return std_err_resp_dict

        if current_app.config["WHETHER_SUBMIT_TO_KAFKA"]:
            try:
                # Make a call to publish to the Kafka Topic Partition Queue
                publish_to_kafka_topic(topic_name=ccmhdr_control_id, data_dict=data_feed_dict)

            except Exception as error:
                logger.error("EXCEPTION OCCURRED", exc_info=True)
                print(type(error), isinstance(error, str), str(error))
                # Converting into standard ERROR response
                std_err_resp_dict = dict()  # initialize it
                std_err_resp_dict = structured_response.StructuredResponse(error, 'GenericException') \
                    .structure_the_error()
                return std_err_resp_dict

            # once submitted to Kafka , spawn a thread and start/check the consumer if it is up and running for the
            # submitted topic and partition ; and start also for other

        # Formulating the standard SUCCESS response
        std_success_resp_dict = structured_response.StructuredResponse(dict(), 'GenericSuccess') \
            .structure_the_success()

        print(std_success_resp_dict)
        return std_success_resp_dict

    def check_b4_insert(self):
        pass


def publish_to_kafka_topic(topic_name, data_dict):
    ''' Here we will publish to the topic for the specific control '''

    # the client_id to be passed as the Engine_id of this instance
    # kafka_admin_client = KafkaAdminClient(bootstrap_servers=BCM.app.config["KAFKA_BROKER_URLS"]
    #                                       ,client_id=BCM.app.config["ENGINE_ID"])
    #
    # print('kafka_admin_client', kafka_admin_client.list_consumer_groups())

    # We will publish to the topic as per the name of the control
    # print('BCM --another place ', dir(BCM))

    kafka_producer = KafkaProducer(bootstrap_servers=current_app.config["KAFKA_BROKER_URLS"]
                                   , value_serializer=lambda m: json.dumps(m).encode("utf-8"))
    kafka_producer_obj = kafka_producer.send(topic_name, data_dict)
    print(kafka_producer_obj)

    kafka_producer.flush()  # make the records immediately available to be sent.
    kafka_producer.close()  # closes the kafka producer connection

    pass

