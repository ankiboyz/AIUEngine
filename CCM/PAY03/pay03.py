# pay03.py
"""Herein the Blueprint implementation for PAY03 control would go in.
This structure makes it easier for you to find the code and resources related to a given functionality.
For example, if you want to find the application logic about PAY03 control,
 then you can go to the PAY03 Blueprint in CCM/pay03.py instead of scrolling through a big app.py (in this specific case
  it is CCM/__init__.py )."""

from flask import Blueprint
from CCM.models import CCMMonitorHDR, CCMonitorDTL

pay03_bp = Blueprint('pay03_bp', __name__,
                     template_folder='templates',
                     static_folder='static', static_url_path='pay03_static')


@pay03_bp.route('/')
def list():
    pay03_executions = CCMMonitorHDR.query.all()
    return str(pay03_executions)
