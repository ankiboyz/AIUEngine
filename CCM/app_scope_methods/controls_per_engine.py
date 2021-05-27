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
    controls_requiring_multiproc = CCM.app.config["LIST_OF_MULTI_PROC_CONTROLS"]

    # Here, making a dictionary with count of the controls out of the list, to identify if any control has been listed
    # more than once.
    control_list_dict = {k: control_list.count(k) for k in control_list}
    duplicate_control_list = [k for k, v in control_list_dict.items() if v > 1]
    unique_control_list = [k for k, v in control_list_dict.items() if v == 1]

    if len(duplicate_control_list) > 0:     # Report the list of duplicate controls.
        logger.warning(f'There are some duplicate controls configured for this Engine Id {engine_id} are'
                       f' {duplicate_control_list}')

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
        for cntrl_id in unique_control_list:

            if cntrl_id in controls_requiring_multiproc:
                multiproc = models.YesNoEnum.Y
            else:
                multiproc = models.YesNoEnum.N

            cntrl_ngn_assoc_obj = models.CCMControlEngineAssoc(control_id=cntrl_id, engine_id=engine_id
                                                               , is_multiproc=multiproc
                                                               , status=models.KafkaConsumerEnum.DOWN)

            db.session.add(cntrl_ngn_assoc_obj)

        try:
            db.session.commit()
        except Exception as error:
            logger.error(f'ERROR:::: Error while inserting records in the control engine association table {error}'
                         , exc_info=True)