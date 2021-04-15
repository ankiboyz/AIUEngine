# pay03.py
"""Herein the Blueprint implementation for PAY03 control would go in.
This structure makes it easier for you to find the code and resources related to a given functionality.
For example, if you want to find the application logic about PAY03 control,
 then you can go to the PAY03 Blueprint in CCM/pay03.py instead of scrolling through a big app.py (in this specific case
  it is CCM/__init__.py )."""

from flask import Blueprint, request
from CCM.models import CCMMonitorHDR, CCMonitorDTL
import logging

logger = logging.getLogger(__name__)

pay03_bp = Blueprint('pay03_bp', __name__,
                     template_folder='templates',
                     static_folder='static', static_url_path='pay03_static')


@pay03_bp.route('/')
def list():
    pay03_executions = CCMMonitorHDR.query.all()
    print(logger.parent, 'parent of logger of pay03 module') # Here,the parent was coming as CCM.(chief of this APP)
    logger.info("I am in PA03 with args as {0}".format(request.args))
    # logging.debug("I am in PA03 with args as {}", request.args)

    return str(pay03_executions)
