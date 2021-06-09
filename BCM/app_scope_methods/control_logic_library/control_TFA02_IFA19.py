#control_TFA02_IFA19.py
'''
Author: Ankur Saxena
Objective : This module outlays the logic executed for the control TFA02_IFA19.
Here we will have all the logic executed for the TFA02_IFA19 control.
'''

import logging
logger = logging.getLogger(__name__)

# response_dict_success = {'STATUS':'SUCCESS', 'DETAIL_SECTION':'', }
# response_dict_failure = {''}


def method_TFA02_IFA19(appln_cntxt, mongo_client, control_params_dict):
    passed_dict = control_params_dict
    ''' Accepts the keyword arguments'''
    logger.info(f' Executing the control library with args {passed_dict} ')

    mongo_client.TFA02_COPY.update_many({"runID": {"$eq": "2bc36ea9-4865-4f39-8ebe-49f556acd566-1622621443070"},
                                        "GLT_is_this_realized": "in-process"},
                                         {"$set": {"GLT_is_this_realized": ""}}  # make it blank
                                        )

    # dummy return as of now, but need to be part of the structured response. -- from the commons
    # return True / False as per the pipeline execution logic -
    return True

#{'STATUS': 'SUCCESS', 'DETAIL_SECTION': '', }


# def test_method_TFA02_IFA19(appln_cntxt, control_params_dict):
#     passed_dict = control_params_dict
#     ''' Accepts the keyword arguments'''
#     logger.info(f' Executing the control library with args {passed_dict} ')
#
#     # dummy return as of now, but need to be part of the structured response. -- from the commons
#     # return True / False as per the pipeline execution logic -
#     return True