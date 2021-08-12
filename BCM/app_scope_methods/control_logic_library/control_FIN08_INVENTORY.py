#control_FIN08_INVENTORY.py
'''
Author: Ankur Saxena
Objective : This module outlays the logic executed for the control FIN08_INVENTORY.
Here we will have all the logic executed for the FIN08_INVENTORY control.

These are the methods called for FIN08_INVENTORY control , Now here the db_session and the app_context is already set at
pipeline execution and their teardown is also taken care of.
So in case of any DB operation , it can be done straightaway without need of the context manager with db.session()
or with app.app_context().
'''

import logging
import commons.external_app_specifics.mongo_db as mongo_db_specifics
import pymongo
import commons.structured_response as structured_response
import BCM.app_scope_methods.control_logic_library.list_of_mongo_statements as list_of_mongo_statements
import datetime, time
logger = logging.getLogger(__name__)


def method_fin08_inventory_1(appln_cntxt, db_session, mongo_client, control_params_dict):
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
        function_id = control_params_dict["FUNCTION_ID"]

        # Temporarily placed here for Testing
        # print('FINAL PARAMETERS DICT', control_params_dict)
        # print('Indexes --> for this control', control_params_dict['INDEXES']['KEY1'].KEY_COMPONENT)

        stg_op_response_obj.add_to_detail_section_dict('FUNCTION_COLLECTION', function_id
                                                       , 'This is the Mongo DB collection operated upon for this stage')

        # getting the RunId
        run_id = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', run_id
                                                       , 'This is the runID passed for this stage execution')

        # Execute the Pipeline Code, Here this statement for update is different for this control.
        result_from_update = db_from_uri_string[function_id]\
            .update_many({"runID": {"$eq": run_id},
                          "GLT_is_this_realized": "IN-PROCESS"},
                         {"$set": {"GLT_is_this_realized": "", "GLT_exception_status": ""
                                   , "GLT_do_exception_exist": "", "GLT_do_discard": ""}}  # make it blank
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


# def test_method_fin08_inventory(appln_cntxt, control_params_dict):
#     passed_dict = control_params_dict
#     ''' Accepts the keyword arguments'''
#     logger.info(f' Executing the control library with args {passed_dict} ')
#
#     # dummy return as of now, but need to be part of the structured response. -- from the commons
#     # return True / False as per the pipeline execution logic -
#     return True

def method_fin08_inventory_3(appln_cntxt, db_session, mongo_client, control_params_dict):
    ''' This method is used to work as Stage 3 for the control.
     3rd - Mark the ones to be handled in this run - Not to overflow Mongo memory - also acts as a floodgate mechanism.
     For this control FIN08_AP_AR; both the function_id and exception_collection_name are needed in this stage. '''

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    try:
    # this will get the database from the URI string
        db_from_uri_string = mongo_client.get_database()

        # Getting the limit of the records to be processed in this run.
        limit_for_recs_processing_in_one_iteration = control_params_dict['LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION']
        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION', limit_for_recs_processing_in_one_iteration
                                                       , 'This is the limit of records to be processed for this control, in one run' )

        # Getting the exception collection name.
        exception_collection_name = control_params_dict['EXCEPTION_COLLECTION_NAME']
        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('EXCEPTION_COLLECTION_NAME', exception_collection_name
                                                   ,'This is the exception_collection_name for this control, in one run')

        # getting the function collection to operate upon
        function_id = control_params_dict["FUNCTION_ID"]

        stg_op_response_obj.add_to_detail_section_dict('FUNCTION_ID', function_id
                                                       , 'This is the Mongo DB collection operated upon for this stage')

        # getting the RunId
        run_id = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', run_id
                                                       , 'This is the runID passed for this stage execution')

        # Execute the Pipeline Code

        mongo_pipeline_code_str = list_of_mongo_statements.AG_PIPELINE_FIN08_AP_AR_1_3


        # Below is the make up to creation of the pipeline
        #Below the function_id is also to be resolved dynamically
        call_to_be_executed_pre_str = 'db_from_uri_string.{function_id}.aggregate(' \
                                      + mongo_pipeline_code_str \
                                      + ',allowDiskUse=True)'

        returned_op_str = ''

        call_to_get_execution_str = 'returned_op_str=' + 'f\'' + call_to_be_executed_pre_str + '\''
        # returned_op_str = f'db_from_uri_string.{function_id}.aggregate([{{"$match": {{"runID": {{"$eq": "{run_id}" }}                                                                 ,"GLT_is_this_realized": {{"$ne": "DONE"}}                                                                 }}                                                     }}, # refer all  those where neq DONE                              {{"$limit": {limit_for_recs_processing_in_one_iteration}}}                              # {{"$project": {{"_id": 0}}}}, # comment this bcoz on the Function Collection no unique key on COMPOSITEKEY                             ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()                                             , "GLT_is_this_realized": "IN-PROCESS"	#                                               }}                              }}                             ,{{"$merge" : {{ "into": "{function_id}"                                          , "on": "_id" # to be on the _id                                          , "whenMatched":[ # not merging here, coz there could have been some other fields updated by workflow or other engine                                                         {{"$addFields":                                                                    {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"                                                                    ,"GLT_is_this_realized": "$$new.GLT_is_this_realized"                                                                    }}                                                         }}                                                        ]                                          , "whenNotMatched": "discard" }} # this should not be the case since the doc is picked from this source only                              }},                              ],allowDiskUse=True)'
        logger.debug(f' This is the mongo code to be executed for code string creation {call_to_get_execution_str}')

        start_time = time.time()

        # There are 2 stages for dynamic execution:
        #     a. The f-string resolves all the string variables needed to be placed in the code.
        #     b. The actual resolved call is then executed.
        # This call returns the final filled in with values of variable resolved string of the call to be executed.

        # in  order to get the value back in the local scope need to pass the scope to exec call,
        # else returned_op_str is not resolved.
        local_env= locals()
        dynamic_execution_to_stringify_call = exec(call_to_get_execution_str, None, local_env)
        returned_op_str = local_env['returned_op_str']

        print('returned_op_str', local_env['returned_op_str'])
        logger.debug(f' This is the mongo code to be actually executed {returned_op_str}')
        stg_op_response_obj.add_to_detail_section_dict('MONGO_CODE', returned_op_str
                                                       , 'This is the mongo code executed')

        with mongo_client.start_session() as session:
            dynamic_execution_to_execute_on_mongo= exec('mongo_cursor_op='+returned_op_str, None, local_env)

        end_time = time.time()
        print(returned_op_str)

        mongo_code_execution_time = end_time - start_time
        stg_op_response_obj.add_to_detail_section_dict('MONGO_CODE_EXECUTION_TIME', mongo_code_execution_time
                                                       , 'This is the mongo code execution time')

        # result_from_update = db_from_uri_string[function_id]\
        #     .update_many({"runID": {"$eq": run_id},
        #                   "GLT_is_this_realized": "IN-PROCESS"},
        #                  {"$set": {"GLT_is_this_realized": ""}}  # make it blank
        #                  )

        # stg_op_response_obj.add_to_detail_section_dict('RESULT_FROM_OPERATION', 'UPDATE_MANY'
        #                                                , f'modified count = {result_from_update.modified_count} '
        #                                                  f'matched count =  {result_from_update.matched_count} '
        #                                                )

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

def method_fin08_inventory_4(appln_cntxt, db_session, mongo_client, control_params_dict):
    ''' This method is used to work as Stage 4 for the control.
     Processing Logic for the marked records. '''

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    try:
    # this will get the database from the URI string
        db_from_uri_string = mongo_client.get_database()

        # Getting the exception collection name.
        exception_collection_name = control_params_dict['EXCEPTION_COLLECTION_NAME']
        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('EXCEPTION_COLLECTION_NAME', exception_collection_name
                                                       , 'This is the exception_collection_name for this control, in one run' )


        # # getting the function collection to operate upon
        function_id = control_params_dict["FUNCTION_ID"]

        stg_op_response_obj.add_to_detail_section_dict('FUNCTION_ID', function_id
                                                       , 'This is the Mongo DB collection operated upon for this stage')

        # getting the RunId
        run_id = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', run_id
                                                       , 'This is the runID passed for this stage execution')

        control_id = control_params_dict["CONTROL_ID"]
        stg_op_response_obj.add_to_detail_section_dict('CONTROL_ID', control_id
                                                   , 'This is the control_id passed for this stage execution')

        # Execute the Pipeline Code

        mongo_pipeline_code_str = list_of_mongo_statements.AG_PIPELINE_FIN08_AP_AR_1_4


        # Below is the make up to creation of the pipeline
        #Below the function_id is also to be resolved dynamically
        call_to_be_executed_pre_str = 'db_from_uri_string.{function_id}.aggregate(' \
                                      + mongo_pipeline_code_str \
                                      + ',allowDiskUse=True)'

        returned_op_str = ''

        call_to_get_execution_str = 'returned_op_str=' + 'f\'' + call_to_be_executed_pre_str + '\''
        # returned_op_str = f'db_from_uri_string.{function_id}.aggregate([{{"$match": {{"runID": {{"$eq": "{run_id}" }}                                                                 ,"GLT_is_this_realized": {{"$ne": "DONE"}}                                                                 }}                                                     }}, # refer all  those where neq DONE                              {{"$limit": {limit_for_recs_processing_in_one_iteration}}}                              # {{"$project": {{"_id": 0}}}}, # comment this bcoz on the Function Collection no unique key on COMPOSITEKEY                             ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()                                             , "GLT_is_this_realized": "IN-PROCESS"	#                                               }}                              }}                             ,{{"$merge" : {{ "into": "{function_id}"                                          , "on": "_id" # to be on the _id                                          , "whenMatched":[ # not merging here, coz there could have been some other fields updated by workflow or other engine                                                         {{"$addFields":                                                                    {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"                                                                    ,"GLT_is_this_realized": "$$new.GLT_is_this_realized"                                                                    }}                                                         }}                                                        ]                                          , "whenNotMatched": "discard" }} # this should not be the case since the doc is picked from this source only                              }},                              ],allowDiskUse=True)'
        logger.debug(f' This is the mongo code to be executed for code string creation {call_to_get_execution_str}')

        start_time = time.time()

        # There are 2 stages for dynamic execution:
        #     a. The f-string resolves all the string variables needed to be placed in the code.
        #     b. The actual resolved call is then executed.
        # This call returns the final filled in with values of variable resolved string of the call to be executed.

        # in  order to get the value back in the local scope need to pass the scope to exec call,
        # else returned_op_str is not resolved.
        local_env= locals()
        dynamic_execution_to_stringify_call = exec(call_to_get_execution_str, None, local_env)
        returned_op_str = local_env['returned_op_str']

        print('returned_op_str', local_env['returned_op_str'])
        logger.debug(f' This is the mongo code to be actually executed {returned_op_str}')
        stg_op_response_obj.add_to_detail_section_dict('MONGO_CODE', returned_op_str
                                                       , 'This is the mongo code executed')

        with mongo_client.start_session() as session:
            dynamic_execution_to_execute_on_mongo= exec('mongo_cursor_op='+returned_op_str, None, local_env)

        end_time = time.time()
        print(returned_op_str)

        mongo_code_execution_time = end_time - start_time
        stg_op_response_obj.add_to_detail_section_dict('MONGO_CODE_EXECUTION_TIME', mongo_code_execution_time
                                                       , 'This is the mongo code execution time')

        # result_from_update = db_from_uri_string[function_id]\
        #     .update_many({"runID": {"$eq": run_id},
        #                   "GLT_is_this_realized": "IN-PROCESS"},
        #                  {"$set": {"GLT_is_this_realized": ""}}  # make it blank
        #                  )

        # stg_op_response_obj.add_to_detail_section_dict('RESULT_FROM_OPERATION', 'UPDATE_MANY'
        #                                                , f'modified count = {result_from_update.modified_count} '
        #                                                  f'matched count =  {result_from_update.matched_count} '
        #                                                )

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
