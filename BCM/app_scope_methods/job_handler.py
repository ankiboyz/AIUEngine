# job_handler.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods for handling the job submitted.

"""
import logging
from flask import current_app
from BCM import models, app
from BCM.app_scope_methods.kafka_consumers import kafka_consumer_algo
from BCM.app_scope_methods.thread_based_job_execution import thread_job_algo

logger = logging.getLogger(__name__)
db = models.db


def list_of_jobs_to_be_handled():
    ''' This will provide the list of jobs to be handled '''

    with app.app_context():
        cntrl_monitor_hdr = models.BCMMonitorHDR()
        query = cntrl_monitor_hdr.query.filter_by(status=models.StatusEnum.SUBMITTED).all()
        logger.debug(f'There are these many jobs to be handled {len(query)}')

        print(query)
        if app.config["WHETHER_SUBMIT_TO_KAFKA"]:
            # Then we know submission to Kafka to happen
            kafka_consumer_algo()
        else:
            # Thread based execution
            thread_job_algo()