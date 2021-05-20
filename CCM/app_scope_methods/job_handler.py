# job_handler.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods for handling the job submitted.

"""
import logging
from flask import current_app
from CCM import models, app
from CCM.app_scope_methods.kafka_consumers import kafka_consumer_algo

logger = logging.getLogger(__name__)
db = models.db


def list_of_jobs_to_be_handled():
    ''' This will provide the list of jobs to be handled '''

    with app.app_context():
        cntrl_monitor_hdr = models.CCMMonitorHDR()
        query = cntrl_monitor_hdr.query.filter_by(status=models.StatusEnum.SUBMITTED).all()
        logger.debug(f'There are these many jobs to be handled {len(query)}')

        print(query)
        if app.config["WHETHER_SUBMIT_TO_KAFKA"]:
            # Then we know submission to Kafka to happen
            kafka_consumer_algo()