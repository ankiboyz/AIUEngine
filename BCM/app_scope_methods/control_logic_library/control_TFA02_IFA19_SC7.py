#control_TFA02_IFA19_SC7.py
'''
Author: Ankur Saxena
Objective : This module outlays the logic executed for the control TFA02_IFA19_SC7.
Here we will have all the logic executed for the TFA02_IFA19_SC7 control.

These are the methods called for TFA02_IFA19_SC7 control , Now here the db_session and the app_context is already set at
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


def method_tfa02_ifa19_sc7_1(appln_cntxt, db_session, mongo_client, control_params_dict):
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

        # Execute the Pipeline Code
        result_from_update = db_from_uri_string[function_id]\
            .update_many({"runID": {"$eq": run_id},
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


# def test_method_tfa02_ifa19_sc7(appln_cntxt, control_params_dict):
#     passed_dict = control_params_dict
#     ''' Accepts the keyword arguments'''
#     logger.info(f' Executing the control library with args {passed_dict} ')
#
#     # dummy return as of now, but need to be part of the structured response. -- from the commons
#     # return True / False as per the pipeline execution logic -
#     return True

def method_tfa02_ifa19_sc7_2(appln_cntxt, db_session, mongo_client, control_params_dict):
    '''
    This method is used to work as Stage 2 for the control.
    Wherein this stage checks whether the exception collection is available in Mongo.
    And whether the needed indexes are there on the exception collection.
    '''

    logger.info(f' Inside the method {method_tfa02_ifa19_sc7_2.__name__} Executing the control library with args {control_params_dict} ')

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    # indexes needed by this control.- in the indexmodel syntax.
    # index1 = pymongo.IndexModel([""], unique=True)

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
        print('result_dict', result_dict)

        # Indexes only to be created if the INDEXES value exist
        indexes_as_per_control_metadata_dict = control_params_dict.get('INDEXES', {})

        if not result_dict.get(exception_collection_name, True):        # if the value coming is False
            # create the collection with the index
            mongo_db_specifics.create_collection_name(mongo_client, exception_collection_name)

            stg_op_response_obj.add_to_detail_section_dict(f'COLLECTION_CREATED',
                                                           exception_collection_name
                                                           ,
                                                           f'This collection was not existing and is created')
            # here it will only proceed further if no error comes out since its under try block, so index is created iff
            # the collection exists.

            # In order to create the indexes, we need to create the Index Models;
            # pass in the INDEXES dict from control_params_dict

            if len(indexes_as_per_control_metadata_dict) != 0:

                indexmodel_per_collection_dict = mongo_db_specifics.create_indexmodels_for_mongodb(indexes_as_per_control_metadata_dict)

                for collection_name, list_of_index_models_dict in indexmodel_per_collection_dict.items():
                    # Take only the INDEX_MODEL key's value from the dicts in the list.
                    list_of_index_models = [k['INDEX_MODEL'] for k in list_of_index_models_dict]

                    # Take only the ENGLISH_REPR key's value from the dicts in the list - for logging
                    list_of_indexes_english_repr = [k['ENGLISH_REPR'] for k in list_of_index_models_dict]

                    mongo_db_specifics.create_indexes(mongo_client, list_of_index_models, collection_name)

                    # For Final Recording.
                    stg_op_response_obj.add_to_detail_section_dict(f'INDEXES_CREATED_{collection_name}',
                                                                   list_of_indexes_english_repr
                                                                   , f'These indexes created on the collection name {collection_name}')
        else:
            # Here, collection is existing but check for index and create needed index on the collection.

            indexes_status_dict = mongo_db_specifics.check_which_indexes_exist(mongo_client, indexes_as_per_control_metadata_dict)
            print('indexes_as_per_control_metadata_dict', indexes_as_per_control_metadata_dict)
            print('indexes_status_dict', indexes_status_dict)

            list_of_keys_already_existing = [key_name for key_name, to_be_created in indexes_status_dict.items() if to_be_created== False]

            print('list_of_keys_already_existing', list_of_keys_already_existing)
            # only those indexes need be created , which do not exist already.

            indexes_to_be_created_dict = {key_name: index_specs for key_name, index_specs in indexes_as_per_control_metadata_dict.items()
                                          if key_name not in list_of_keys_already_existing}

            stg_op_response_obj.add_to_detail_section_dict(f'INDEXES_TO_BE_CREATED',
                                                           indexes_to_be_created_dict,
                                                           f'These indexes are missing after checking with DB')

            if len(indexes_to_be_created_dict) != 0:

                indexmodel_per_collection_dict = mongo_db_specifics.create_indexmodels_for_mongodb(indexes_to_be_created_dict)

                for collection_name, list_of_index_models_dict in indexmodel_per_collection_dict.items():
                    # Take only the INDEX_MODEL key's value from the dicts in the list.
                    # list_of_index_models_dict is a list
                    list_of_index_models = [k['INDEX_MODEL'] for k in list_of_index_models_dict]

                    # Take only the ENGLISH_REPR key's value from the dicts in the list - for logging
                    list_of_indexes_english_repr = [k['ENGLISH_REPR'] for k in list_of_index_models_dict]

                    mongo_db_specifics.create_indexes(mongo_client, list_of_index_models, collection_name)

                    # For Final Recording.
                    stg_op_response_obj.add_to_detail_section_dict(f'INDEXES_CREATED_{collection_name}',
                                                                   list_of_indexes_english_repr
                                                                   , f'These indexes created on the collection name {collection_name}')

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


def method_tfa02_ifa19_sc7_3(appln_cntxt, db_session, mongo_client, control_params_dict):
    ''' This method is used to work as Stage 3 for the control.
     3rd - Mark the ones to be handled in this run - Not to overflow Mongo memory - also acts as a floodgate mechanism'''

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    try:
    # this will get the database from the URI string
        db_from_uri_string = mongo_client.get_database()

        # Getting the limit of the records to be processed in this run.
        limit_for_recs_processing_in_one_iteration = control_params_dict['LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION']
        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION', limit_for_recs_processing_in_one_iteration
                                                       , 'This is the limit of records to be processed for this control, in one run' )


        # getting the function collection to operate upon
        function_id = control_params_dict["FUNCTION_ID"]

        stg_op_response_obj.add_to_detail_section_dict('FUNCTION_ID', function_id
                                                       , 'This is the Mongo DB collection operated upon for this stage')

        # getting the RunId
        run_id = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', run_id
                                                       , 'This is the runID passed for this stage execution')

        # Execute the Pipeline Code

        mongo_pipeline_code_str = list_of_mongo_statements.AG_PIPELINE_TFA02_IFA19_SC7_1_3


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

def method_tfa02_ifa19_sc7_4(appln_cntxt, db_session, mongo_client, control_params_dict):
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

        mongo_pipeline_code_str = list_of_mongo_statements.AG_PIPELINE_TFA02_IFA19_SC7_1_4


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

def method_tfa02_ifa19_sc7_5(appln_cntxt, db_session, mongo_client, control_params_dict):
    ''' This method is used to work as Stage 5 for the control.
     Additional Processing over the prepared Exceptions '''

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

        # getting the function collection to operate upon, not needed for this stage
        # function_id = control_params_dict["FUNCTION_ID"]
        # stg_op_response_obj.add_to_detail_section_dict('FUNCTION_COLLECTION', function_id
        #                                              , 'This is the Mongo DB collection operated upon for this stage')

        # Getting the exception collection name
        exception_collection_name = control_params_dict['EXCEPTION_COLLECTION_NAME']
        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('EXCEPTION_COLLECTION_NAME', exception_collection_name
                                                       ,
                                                       'This is the exception_collection_name for this control, in one run')
        # getting the RunId
        run_id = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', run_id
                                                       , 'This is the runID passed for this stage execution')

        start_time = time.time()
        # Execute the Pipeline Code
        result_from_update = db_from_uri_string[exception_collection_name]\
            .update_many({"runID": {"$eq": run_id}, "exceptionID": ""},
                         [{"$set": {"exceptionID": {"$toString": "$_id"}}}]
                         )

        stg_op_response_obj.add_to_detail_section_dict('RESULT_FROM_OPERATION', 'UPDATE_MANY'
                                                       , f'modified count = {result_from_update.modified_count} '
                                                         f'matched count =  {result_from_update.matched_count} '
                                                       )
        end_time = time.time()

        mongo_code_execution_time = end_time - start_time
        stg_op_response_obj.add_to_detail_section_dict('MONGO_CODE_EXECUTION_TIME', mongo_code_execution_time
                                                       , 'This is the mongo code execution time')

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


def method_tfa02_ifa19_sc7_6(appln_cntxt, db_session, mongo_client, control_params_dict):
    ''' This method is used to work as Stage 6 for the control.
     Close the Loop - Flag the posts conquered!!! '''

    logger.info(f' Executing the control library with args {control_params_dict} ')

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    # with db_session.begin():    #Not needed in the methods now taken care at the pipeline execution side to put in additional details for committing to DB if needed

    try:
        # this will get the database from the URI string
        db_from_uri_string = mongo_client.get_database()

        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('DATABASE_NAME', db_from_uri_string.name
                                                       , 'This is the Mongo DB connected for this stage' )

        # getting the function collection to operate upon, not needed for this stage
        function_id = control_params_dict["FUNCTION_ID"]
        stg_op_response_obj.add_to_detail_section_dict('FUNCTION_COLLECTION', function_id
                                                       , 'This is the Mongo DB collection operated upon for this stage')

        # # Getting the exception collection name
        # exception_collection_name = control_params_dict['EXCEPTION_COLLECTION_NAME']
        # # Add informatives to the DETAIL_SECTION
        # stg_op_response_obj.add_to_detail_section_dict('EXCEPTION_COLLECTION_NAME', exception_collection_name
        #                                                ,
        #                                         'This is the exception_collection_name for this control, in one run')
        # getting the RunId
        run_id = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', run_id
                                                       , 'This is the runID passed for this stage execution')

        start_time = time.time()
        # Execute the Pipeline Code
        result_from_update = db_from_uri_string[function_id]\
            .update_many({"runID": {"$eq": run_id}, "GLT_is_this_realized": "IN-PROCESS"},
                         {"$set": {"GLT_is_this_realized": "DONE"}}
                         )

        stg_op_response_obj.add_to_detail_section_dict('RESULT_FROM_OPERATION', 'UPDATE_MANY'
                                                       , f'modified count = {result_from_update.modified_count} '
                                                         f'matched count =  {result_from_update.matched_count} '
                                                       )
        end_time = time.time()

        mongo_code_execution_time = end_time - start_time
        stg_op_response_obj.add_to_detail_section_dict('MONGO_CODE_EXECUTION_TIME', mongo_code_execution_time
                                                       , 'This is the mongo code execution time')

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

def method_tfa02_ifa19_sc7_7(appln_cntxt, db_session, mongo_client, control_params_dict):
    ''' This method is used to work as Stage 7 for the control.WHETHER_ALL_DONE
     This is check to know whether we need to further continue or proceed to exit .
     This is a decision stage so here , 1 will denote proceed to yesID and no to denote proceed to noID .

     IMP: this is the decision node and can only emit out status as yesID, noID or 0 (i.e FAILURE);
     It cannot have a status as SUCCESS emitted out! --- V.Imp to remember else if it emits SUCCESS then yesID wil be considered
     '''

    logger.info(f' Executing the control library with args {control_params_dict} ')

    stg_op_response_obj = structured_response.StageOutputResponseDict()

    # with db_session.begin():    #Not needed in the methods now taken care at the pipeline execution side to put in additional details for committing to DB if needed

    try:
        # this will get the database from the URI string
        db_from_uri_string = mongo_client.get_database()

        # Add informatives to the DETAIL_SECTION
        stg_op_response_obj.add_to_detail_section_dict('DATABASE_NAME', db_from_uri_string.name
                                                       , 'This is the Mongo DB connected for this stage' )

        # getting the function collection to operate upon, not needed for this stage
        function_id = control_params_dict["FUNCTION_ID"]
        stg_op_response_obj.add_to_detail_section_dict('FUNCTION_COLLECTION', function_id
                                                       , 'This is the Mongo DB collection operated upon for this stage')

        # # Getting the exception collection name
        # exception_collection_name = control_params_dict['EXCEPTION_COLLECTION_NAME']
        # # Add informatives to the DETAIL_SECTION
        # stg_op_response_obj.add_to_detail_section_dict('EXCEPTION_COLLECTION_NAME', exception_collection_name
        #                                                ,
        #                                         'This is the exception_collection_name for this control, in one run')
        # getting the RunId
        run_id = control_params_dict["RUN_ID"]
        stg_op_response_obj.add_to_detail_section_dict('RUN_ID', run_id
                                                       , 'This is the runID passed for this stage execution')

        start_time = time.time()
        # Execute the Pipeline Code
        # Please note here update_one is used.
        # it is important that the check for marking IN_PROCESS (in stage 3) is done using not equal to operator to DONE
        # (and ='IN-PROCESS' to be avoided) so that
        # CHECK_IF_NEED_TO_CONTINUE value is also included.

        result_from_update = db_from_uri_string[function_id]\
            .update_one({"runID": {"$eq": run_id}, "GLT_is_this_realized": {"$ne": "DONE"}},
                        {"$set": {"GLT_is_this_realized": "CHECK_IF_NEED_TO_CONTINUE"}}
                        )

        modified_count = result_from_update.modified_count
        matched_count = result_from_update.matched_count
        stg_op_response_obj.add_to_detail_section_dict('RESULT_FROM_OPERATION', 'UPDATE_MANY'
                                                       , f'modified count = {modified_count} '
                                                         f'matched count =  {matched_count} '
                                                       )
        end_time = time.time()

        mongo_code_execution_time = end_time - start_time
        stg_op_response_obj.add_to_detail_section_dict('MONGO_CODE_EXECUTION_TIME', mongo_code_execution_time
                                                       , 'This is the mongo code execution time')

        if modified_count == 0 and matched_count == 0:  # all work done so move to next stage
            # noID to emitted which signals proceed to further stage and Not loop in
            stg_op_response_obj.add_to_status('yesID')
            stg_op_response_obj.add_to_status_comments('yesID signalled hence all completed!')

        else:  # loop back to process rest of the records.
            # yesID to emitted which signals proceed to further stage and Not loop in
            stg_op_response_obj.add_to_status('noID')
            stg_op_response_obj.add_to_status_comments('noID signalled starting from the initial !')

        logger.debug(f' As a result of the operation of the method on the input parameters {control_params_dict} '
                     f'following is the response output {stg_op_response_obj.method_op_dict}')

    except Exception as error:
        logger.error(f'Error encountered while executing method having input params as {control_params_dict} '
                     f'error being {error}', exc_info=True)

        stg_op_response_obj.add_to_status(0)    # 0 denotes Failure
        stg_op_response_obj.add_to_status_comments(str(error))

    finally:
        return stg_op_response_obj.method_op_dict
