#control_TFA02_IFA19.py
'''
Author: Ankur Saxena
Objective : This module outlays the logic executed for the control TFA02_IFA19.
Here we will have all the logic executed for the TFA02_IFA19 control.
'''

import logging
import commons.structured_response as structured_response
logger = logging.getLogger(__name__)


def method_tfa02_ifa19_1(appln_cntxt, db_session, mongo_client, control_params_dict):
    passed_dict = control_params_dict
    ''' Accepts the keyword arguments'''
    logger.info(f' Executing the control library with args {passed_dict} ')

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    # Setting transiently for now.
    control_params_dict["FUNCTION_ID"] = 'TFA02_COPY'
    control_params_dict["RUN_ID"] = '1b90f9d5-094d-4816-a8a8-8ddf04247486-1622726323002'

    try:
        # this will get the database from the URI string
        db_from_uri_string = mongo_client.get_database()

        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('DATABASE_NAME', db_from_uri_string.name
                                                       , 'This is the Mongo DB connected for this stage' )

        # getting the function collection to operate upon
        func_collection = control_params_dict["FUNCTION_ID"]

        stg_op_response_obj.add_to_detail_section_dict('FUNCTION_COLLECTION', control_params_dict["FUNCTION_ID"]
                                                       , 'This is the Mongo DB collection operated upon for this stage')

        # getting the RunId
        runId_passed = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', runId_passed
                                                       , 'This is the runID passed for this stage execution')

        # Execute the Pipeline Code
        result_from_update = db_from_uri_string[func_collection]\
            .update_many({"runID": {"$eq": runId_passed},
                          "GLT_is_this_realized": "IN-PROCESS"},
                         {"$set": {"GLT_is_this_realized": ""}}  # make it blank
                         )

        stg_op_response_obj.add_to_detail_section_dict('RESULT_FROM_OPERATION', 'UPDATE_MANY'
                                                       , f'modified count = {result_from_update.modified_count} '
                                                         f'matched count =  {result_from_update.matched_count} '
                                                       )

        stg_op_response_obj.add_to_status(1)    # 1 denotes SUCCESS
        stg_op_response_obj.add_to_status_comments('executed_successfully')

        logger.debug(f' As a result of the operation of the method on the input parameters {control_params_dict} '
                     f'following is the response output {stg_op_response_obj.method_op_dict}')

    except Exception as error:
        logger.error(f'Error encountered while executing method having input params as {control_params_dict} '
                     f'error being {error}', exc_info=True)

        stg_op_response_obj.add_to_status(0)    # 0 denotes Failure
        stg_op_response_obj.add_to_status_comments(str(error))

    finally:
        return stg_op_response_obj.method_op_dict

#{'STATUS': 'SUCCESS', 'DETAIL_SECTION': '', }


# def test_method_TFA02_IFA19(appln_cntxt, control_params_dict):
#     passed_dict = control_params_dict
#     ''' Accepts the keyword arguments'''
#     logger.info(f' Executing the control library with args {passed_dict} ')
#
#     # dummy return as of now, but need to be part of the structured response. -- from the commons
#     # return True / False as per the pipeline execution logic -
#     return True