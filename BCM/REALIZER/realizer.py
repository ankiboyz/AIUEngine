# realizer.py
"""Herein the Blueprint implementation for REALIZER control would go in.
This structure makes it easier for you to find the code and resources related to a given functionality.
For example, if you want to find the application logic about REALIZER control,
 then you can go to the REALIZER Blueprint in BCM/realizer.py instead of scrolling through a big app.py (in this specific case
  it is BCM/__init__.py )."""
import config
from flask import Blueprint, request, jsonify
from BCM.models import BCMMonitorHDR, BCMMonitorDTL, StatusEnum
import logging
import json
from BCM.REALIZER import services
from datetime import datetime

logger = logging.getLogger(__name__)

realizer_bp = Blueprint('realizer_bp', __name__,
                     template_folder='templates',
                     static_folder='static', static_url_path='pay05_static')


@realizer_bp.route('/')
def list_jobs():
    pay05_executions = BCMMonitorHDR.query.all()
    print(logger.parent, 'parent of logger of pay03 module')  # Here,the parent was coming as BCM.(chief of this APP)
    logger.info("I am in REALIZER with args as {0}".format(request.args))
    # logging.debug("I am in PA03 with args as {}", request.args)

    return str(pay05_executions)


@realizer_bp.route('/submitjob')
def submit_job():
    # Here, first of all lets gather the input args been passed.

    print(logger.parent, 'parent of logger of pay03 module')  # Here,the parent was coming as BCM.(chief of this APP)
    logger.info("I am in REALIZER with args as {0}".format(request.args))

    # Gathering the control ID from the request params
    control_id = request.args.get('CONTROL_ID', 'NONE_GATHERED')

    print(request.args, jsonify(request.args), json.dumps(request.args))
    CCMhdr_obj = services.CCMHeader()

    # created_date and updated_date will be filled up in the insert/update methods for the
    # particular obj eg CCMHeader and CCMDetail tables
    ret_op = CCMhdr_obj.insert(control_id=control_id, operation_type=config.OPERATION_TYPE
                               , parameters=json.dumps(request.args)
                               , start_date=datetime.now()
                               , status=StatusEnum.SUBMITTED)

    # pay05_executions = CCMMonitorHDR.query.all()
    # print('pay05_executions', pay05_executions)

    # logging.debug("I am in PA03 with args as {}", request.args)

    # return str(pay05_executions)
    return jsonify(ret_op)
