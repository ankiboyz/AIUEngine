# pay05.py
"""Herein the Blueprint implementation for PAY05 control would go in.
This structure makes it easier for you to find the code and resources related to a given functionality.
For example, if you want to find the application logic about PAY05 control,
 then you can go to the PAY05 Blueprint in CCM/pay05.py instead of scrolling through a big app.py (in this specific case
  it is CCM/__init__.py )."""

from flask import Blueprint, request, jsonify
from CCM.models import CCMMonitorHDR, CCMonitorDTL, StatusEnum
import logging
import json
from CCM.PAY05 import services
from datetime import datetime

logger = logging.getLogger(__name__)

pay05_bp = Blueprint('pay05_bp', __name__,
                     template_folder='templates',
                     static_folder='static', static_url_path='pay05_static')


@pay05_bp.route('/')
def list_jobs():
    pay05_executions = CCMMonitorHDR.query.all()
    print(logger.parent, 'parent of logger of pay03 module')  # Here,the parent was coming as CCM.(chief of this APP)
    logger.info("I am in PAY05 with args as {0}".format(request.args))
    # logging.debug("I am in PA03 with args as {}", request.args)

    return str(pay05_executions)


@pay05_bp.route('/submitjob')
def submit_job():
    # Here, first of all lets gather the input args been passed.

    print(logger.parent, 'parent of logger of pay03 module')  # Here,the parent was coming as CCM.(chief of this APP)
    logger.info("I am in PAY05 with args as {0}".format(request.args))

    print(request.args, jsonify(request.args), json.dumps(request.args))
    CCMhdr_obj = services.CCMHeader()

    # created_date and updated_date will be filled up in the insert/update methods for the
    # particular obj eg CCMHeader and CCMDetail tables
    ret_op = CCMhdr_obj.insert(control_id='PAY05', operation_type='CCM'
                               , parameters=json.dumps(request.args)
                               , start_date=datetime.now()
                               , status=StatusEnum.SUBMITTED)

    # pay05_executions = CCMMonitorHDR.query.all()
    # print('pay05_executions', pay05_executions)

    # logging.debug("I am in PA03 with args as {}", request.args)

    # return str(pay05_executions)
    return jsonify(ret_op)
