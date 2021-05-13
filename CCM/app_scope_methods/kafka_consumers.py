# kafka_consumers.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods related to the kafka_consumers; eg starting up and checking
                whether the consumers are up or not.
"""

import logging
from kafka import KafkaConsumer

from CCM import models

logger = logging.getLogger(__name__)
db = models.db

