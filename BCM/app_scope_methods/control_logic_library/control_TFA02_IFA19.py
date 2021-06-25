#control_TFA02_IFA19.py
'''
Author: Ankur Saxena
Objective : This module outlays the logic executed for the control TFA02_IFA19.
Here we will have all the logic executed for the TFA02_IFA19 control.

These are the methods called for TFA02_IFA19 control , Now here the db_session and the app_context is already set at
pipeline execution and their teardown is also taken care of.
So in case of any DB operation , it can be done straightaway without need of the context manager with db.session()
or with app.app_context().
'''

import logging
import commons.structured_response as structured_response
logger = logging.getLogger(__name__)
import commons.external_app_specifics.mongo_db as mongo_db_specifics
import pymongo


def method_tfa02_ifa19_1(appln_cntxt, db_session, mongo_client, control_params_dict):
    ''' This method is used to work as Stage 1 for the control.
     Wherein it prepares the Playground for the execution of the control. '''

    logger.info(f' Executing the control library with args {control_params_dict} ')

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    # with db_session.begin():    #Not needed in the methods now taken care at the pipeline execution side to put in additional details for committing to DB if needed
    #     pass

    # # Setting transiently for now. These are already set in the set_controls_param dict now
    # control_params_dict["FUNCTION_ID"] = 'TFA02_COPY'
    # control_params_dict["RUN_ID"] = '1b90f9d5-094d-4816-a8a8-8ddf04247486-1622726323002'

    try:
        # this will get the database from the URI string
        db_from_uri_string = mongo_client.get_database()

        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('DATABASE_NAME', db_from_uri_string.name
                                                       , 'This is the Mongo DB connected for this stage' )

        # getting the function collection to operate upon
        func_collection = control_params_dict["FUNCTION_ID"]

        # Temporarily placed here for Testing
        print('FINAL PARAMETERS DICT', control_params_dict)
        print('Indexes --> for this control', control_params_dict['INDEXES']['KEY1'].KEY_COMPONENT)

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

def method_tfa02_ifa19_2(appln_cntxt, db_session, mongo_client, control_params_dict):
    '''
    This method is used to work as Stage 2 for the control.
    Wherein this stage checks whether the exception collection is available in Mongo.
    And whether the needed indexes are there on the exception collection.
    '''

    logger.info(f' Inside the method {method_tfa02_ifa19_2.__name__} Executing the control library with args {control_params_dict} ')

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    # indexes needed by this control.- in the indexmodel syntax.
    index1 = pymongo.IndexModel([""], unique=True)

    try:
        # this will get the database that is specified in the URI string
        db_from_uri_string = mongo_client.get_database()
        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('DATABASE_NAME', db_from_uri_string.name
                                                       , 'This is the Mongo DB connected for this stage')

        # getting the function collection to operate upon
        exception_collection_name = control_params_dict["EXCEPTION_COLLECTION_NAME"]
        stg_op_response_obj.add_to_detail_section_dict('EXCEPTION_COLLECTION_NAME', exception_collection_name
                                                       , 'This is the Mongo DB exception collection')

        # Making call to Mongo to check whether the collection is there.
        result_dict = mongo_db_specifics.get_collection_existence(mongo_client, exception_collection_name)
        if not result_dict.get(exception_collection_name, True):        # if the value coming is False
            # create the collection with the index
            mongo_db_specifics.create_collection_name(mongo_client, exception_collection_name)
            # here it will only proceed further if no error comes out since its under try block, so index is created iff
            # the collection exists.

            pass
        else:

            # check for index and create needed index on the collection.
            pass

        # stg_op_response_obj.add_to_detail_section_dict('RESULT_FROM_OPERATION', 'EXCEPTION_COLLECTION_LOOK_UP'
        #                                                , f'modified count = {result_from_update.modified_count} '
        #                                                  f'matched count =  {result_from_update.matched_count} '
        #                                                )

        stg_op_response_obj.add_to_status(1)  # 1 denotes SUCCESS
        stg_op_response_obj.add_to_status_comments('executed_successfully')

        logger.debug(f' As a result of the operation of the method on the input parameters {control_params_dict} '
                     f'following is the response output {stg_op_response_obj.method_op_dict}')

    except Exception as error:
        logger.error(f'Error encountered while executing method having input params as {control_params_dict} '
                     f'error being {error}', exc_info=True)

        stg_op_response_obj.add_to_status(0)  # 0 denotes Failure
        stg_op_response_obj.add_to_status_comments(str(error))

    finally:
        return stg_op_response_obj.method_op_dict