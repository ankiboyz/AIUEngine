# controls_per_engine.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods related to the configuration of the controls supported per
                engine and as well as methods to help in lifecycle management of those controls eg listing out the
                controls per engine and their status (UP/DOWN) etc.

"""
import logging
import CCM

from flask import current_app
from CCM import models

logger = logging.getLogger(__name__)

db = models.db


def list_of_controls_per_engine():
    engine_id = CCM.app.config["ENGINE_ID"]
    control_list = CCM.app.config["LIST_OF_CONTROLS"]

    logger.info(f'List of Controls by this Engine Id {engine_id} are {control_list}')
    with CCM.app.app_context():
        cntrl_ngn_assoc_obj = models.CCMControlEngineAssoc()
        query = cntrl_ngn_assoc_obj.query.filter_by(engine_id=engine_id).all()
        # print('cntrl_ngn_assoc_obj.query.filter_by().all()  --', query)
        # logger.debug(f' There are records in the control engine association table {query}')

        # cntrl_ngn_assoc_obj = models.CCMControlEngineAssoc()
        # cntrl_ngn_assoc_obj.engine_id = engine_id
        for i in query:
            db.session.delete(i)  # delete the entries and re-insert
            db.session.commit()

        # re-insert here
        for cntrl_id in control_list:
            cntrl_ngn_assoc_obj = models.CCMControlEngineAssoc(control_id=cntrl_id, engine_id=engine_id
                                                               , status=models.KafkaConsumerEnum.DOWN)

            db.session.add(cntrl_ngn_assoc_obj)

        try:
            db.session.commit()
        except Exception as error:
            logger.error(f'ERROR:::: Error while inserting records in the control engine association table {error}'
                         , exc_info=True)