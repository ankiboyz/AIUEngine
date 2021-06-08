#control_TFA02_IFA19.py
'''
Author: Ankur Saxena
Objective : This module outlays the logic executed for the control TFA02_IFA19
'''

import logging
logger = logging.getLogger(__name__)

# response_dict_success = {'STATUS':'SUCCESS', 'DETAIL_SECTION':'', }
# response_dict_failure = {''}

def method_TFA02_IFA19(appln_cntxt, control_params_dict):
    passed_dict = control_params_dict
    ''' Accepts the keyword arguments'''
    logger.info(f' Executing the control library with args {passed_dict} ')

    # dummy return as of now, but need to be part of the structured response. -- from the commons
    # return True / False as per the pipeline execution logic -
    return True

#{'STATUS': 'SUCCESS', 'DETAIL_SECTION': '', }