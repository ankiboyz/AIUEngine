#benevolent_methods.py
'''
Author: Ankur Saxena
Objective : As the name suggests ,this module contains methods those are benevolent in nature i.e. can be used
          across different controls and execute the common functionality.

'''

import logging
import commons.structured_response as structured_response
logger = logging.getLogger(__name__)
import commons.external_app_specifics.mongo_db as mongo_db_specifics
import pymongo

def make_collection_with_indexes_available(appln_cntxt, db_session, mongo_client, control_params_dict):
    '''
    This method is used to work as Stage 2 for the control.
    Wherein this stage checks whether the exception collection is available in Mongo.
    And whether the needed indexes are there on the exception collection.
    If not available this method creates the needed indexes and collection.
    '''

    logger.info(f' Inside the method {make_collection_with_indexes_available.__name__} Executing the control library with args {control_params_dict} ')

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