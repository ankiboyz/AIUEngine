# mongo_db.py
'''
Author: Ankur Saxena
Objective: The objective of this module is to provide methods/ decorators for the connection handling to
            mongo db database.
            Also various mongo related operations
'''
import logging
from pymongo import MongoClient, errors as pymonerrors, IndexModel

logger = logging.getLogger(__name__)


def create_mongo_client(mongodb_uri):
    ''' Objective of the method is to get the mongo connection and return it to the caller.
    Input : The URI of the mongo client.
    Output : MongoClient in case of success
             Boolean False in case of an error
             along with the error ; that is its a tuple.

    In case of situation corresponding error was thrown:
    timeout:
    when IP cannot be resolved eg VPN not connected, it wont be able to make out whether ip valid or cant reach the server.
    pymongo.errors.ServerSelectionTimeoutError: 192.168.2.193:27018: timed out, Timeout: 30s, Topology Description: <TopologyDescription id: 60c075e1624cc85993e7be64, topology_type: Single, servers: [<ServerDescription ('192.168.2.193', 27018) server_type: Unknown, rtt: None, error=NetworkTimeout('192.168.2.193:27018: timed out',)>]>

    configuration error : username pwd incorrect
    pymongo.errors.OperationFailure: Authentication failed., full error: {'ok': 0.0, 'errmsg': 'Authentication failed.', 'code': 18, 'codeName': 'AuthenticationFailed'}

    if wrong ip is given:
    when IP can be reached but no MOngo on it , it will refuse.
    192.168.2.194:27018: [WinError 10061] No connection could be made because the target machine actively refused it, Timeout: 30s, Topology Description: <TopologyDescription id: 60c0779672223a713647f6a0, topology_type: Single, servers: [<ServerDescription ('192.168.2.194', 27018) server_type: Unknown, rtt: None, error=AutoReconnect('192.168.2.194:27018: [WinError 10061] No connection could be made because the target machine actively refused it',)>]>
    pymongo.errors.ServerSelectionTimeoutError: 192.168.2.194:27018: [WinError 10061] No connection could be made because the target machine actively refused it, Timeout: 30s, Topology Description: <TopologyDescription id: 60c07836ac8800f77c8b9084, topology_type: Single, servers: [<ServerDescription ('192.168.2.194', 27018) server_type: Unknown, rtt: None, error=AutoReconnect('192.168.2.194:27018: [WinError 10061] No connection could be made because the target machine actively refused it',)>]>

    '''

    mc_or_False = False
    error_recd = ''
    mc = MongoClient(mongodb_uri)

    try:
        # light weight command used to check for the Client credentials , connectivity etc.
        mc.admin.command('ismaster')
        logger.info('Mongo client is able to Connect')
        mc_or_False = mc
        error_recd = ''

    except Exception as error:
        error_recd = error      # in order to take care of unbound local variable a variable need to be assigned within the except block.
        logger.error(f'ERROR encountered {error_recd}', exc_info=True)
        mc_or_False = False

    return mc_or_False, error_recd   # Passing as a tuple.

# For testing purposes
# d = create_mongo_client("mongodb://ankur:green123@192.168.2.193:27018/PHILIPS_BCM"
#                                         "?authSource=PHILIPS_BCM"
#                                         "&authMechanism=SCRAM-SHA-256&readPreference=primary"
#                                         "&appname=GLT_BCM_AIUENGINE&ssl=false")
# print(d)
# This method will be used as a decorator for methods executing mongo commands
# def execute_mongo_command(func)


def get_collection_existence(mongo_client, collection_name):
    ''' With this method we will be able to know whether the collection exists.
        accepts the list of the collection names/ as well as single collection name
        and returns the list with True False against those as per their
        existence.
        True if the collection is there else false.

        Pls note the exception_collection_name can be a list as well.
    '''

    # gets the mentioned db in the uri and connects to that.
    db = mongo_client.get_database()

    # as per Mongo docs associating nameOnly helps in not attaching any locks to the collection.

    with mongo_client.start_session() as session:
        # with session.start_transaction(): Since its not a transaction of DML kind hence commenting
        # with nameOnly flag - Returning just the name and type (view or collection) does not take
        # collection-level locks whereas returning full collection information locks each collection in the database.
        return_dict = dict()

        if isinstance(collection_name, list):
            exception_collection_name_list = collection_name # since this is a list so putting in proper name
            for excpt_coll_name in exception_collection_name_list:
                result_list = db.list_collection_names(filter={'name': excpt_coll_name}, nameOnly=True)
                if len(result_list) == 0:
                    return_dict[excpt_coll_name] = False
                else:
                    return_dict[excpt_coll_name] = True

        if isinstance(collection_name, str):
            excpt_coll_name = collection_name
            result_list = db.list_collection_names(filter={'name': excpt_coll_name}, nameOnly=True)
            if len(result_list) == 0:
                return_dict[excpt_coll_name] = False
            else:
                return_dict[excpt_coll_name] = True
    # returns the dictionary as created above
    return return_dict


def create_collection_name(mongo_client, collection_name):
    ''' Here, also the mongo's exception collection name can be a string or multiple collections in a list '''

    db = mongo_client.get_database()

    with mongo_client.start_session() as session:
        return_dict = dict()

        if isinstance(collection_name, list):
            exception_collection_name_list = collection_name  # since this is a list so putting in proper name
            for excpt_coll_name in exception_collection_name_list:
                try:
                    result_list = db.create_collection(excpt_coll_name)
                    return_dict[excpt_coll_name] = True

                except pymonerrors.CollectionInvalid as error:
                    logger.info(f'Collection Invalid exception been raised while creating '
                                f'the collection {excpt_coll_name}')
                    return_dict[excpt_coll_name] = True    # setting in the dictionary as True since it exists

                except Exception as error:     # except all others
                    logger.error(f'Error encountered while creating the exception , error as {error}', exc_info=True)
                    return_dict[excpt_coll_name] = False
                    raise

        if isinstance(collection_name, str):
            excpt_coll_name = collection_name
            try:
                result_list = db.create_collection(excpt_coll_name)
                return_dict[excpt_coll_name] = True

            except pymonerrors.CollectionInvalid as error:  # this will come if the collection already exists
                logger.info(f'Collection Invalid exception been raised while creating '
                            f'the collection {excpt_coll_name}')
                return_dict[excpt_coll_name] = True  # setting in the dictionary as True since it exists

            except Exception as error:  # except all others
                logger.error(f'Error encountered while creating the exception , error as {error}', exc_info=True)
                return_dict[excpt_coll_name] = False
                raise


def create_indexes(mongo_client, list_of_index_models, collection_name):
    ''' This method can be used to create different indexes on the collection.
    Here, this is a single collection.
    IndexModel([("hello", DESCENDING),
                ("world", ASCENDING)], name="hello_world")
    IndexModel([("goodbye", DESCENDING)])
    above are examples of indexModels.

    These methods will raise exceptions to be caught by the caller program.
    '''

    db = mongo_client.get_database()

    with mongo_client.start_session() as session:
        return_dict = dict()
        try:
            # Here, in loop in for every indexM
            db[collection_name].create_indexes(list_of_index_models)
            return True
        except Exception as error:
            logger.error(f' Encountered error while creating indexes for the '
                         f'collection {collection_name}, error being {error}', exc_info=True)
            raise


def create_indexmodels_for_mongodb(index_dict):
    ''' Here , the index dict is presented , that is maintained in the control_metadata.py file
    The structure that is maintained in the form as below (the dict braces starting from KEY1 will be input):
    'INDEXES':
    {'KEY1': IndexKeys([("COMPOSITEKEY", 'ASCENDING')], 'TFA02', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'})
    , 'KEY2': IndexKeys([("runID", 'ASCENDING')], 'TFA02', {'unique': False, 'name': 'GLT_BCM_runID'})
    , 'KEY3': IndexKeys([("exceptionID", 'ASCENDING')], 'TFA02', {'unique': False, 'name': 'GLT_BCM_exceptionID'})}

    The value of KEY1 is the NamedTuple IndexKeys with the structure as defined in control_metadata.py file:
    IndexKeys = typing.NamedTuple('IndexKeys', [('KEY_COMPONENT', list), ('COLLECTION_NAME', str), ('KWARGS', dict)])
    so values can be accessed as named indices.
    And the index model for above will be :
    IndexModel([("COMPOSITEKEY",pymongo.ASCENDING)],name="GLT_BCM_COMPOSITEKEY",unique=True)
    IndexModel([("runID",pymongo.ASCENDING)],name="GLT_BCM_runID",unique=False)
    IndexModel([("exceptionID",pymongo.ASCENDING)],name="GLT_BCM_exceptionID",unique=False)

    Here, we will be forming the index models per collection name as
    {'TFA02' : [{INDEX_MODEL : indexModel1, ENGLISH_REPR : 'Key Component as [("exceptionID",pymongo.ASCENDING)], KWARGS as {name:GLT_BCM_COMPOSITEKEY,unique:True}' }
             , {INDEX_MODEL : indexModel2, ENGLISH_REPR : 'Key Component as [("exceptionID",pymongo.ASCENDING)], KWARGS as {name:GLT_BCM_COMPOSITEKEY,unique:True}' }
             , {INDEX_MODEL : indexModel3, ENGLISH_REPR : 'Key Component as [("exceptionID",pymongo.ASCENDING)], KWARGS as {name:GLT_BCM_COMPOSITEKEY,unique:True}' }
              ],
     'TRE07' : [{INDEX_MODEL : indexModel1, ENGLISH_REPR : 'Key Component as [("exceptionID",pymongo.ASCENDING)], KWARGS as {name:GLT_BCM_COMPOSITEKEY,unique:True}' }
             , {INDEX_MODEL : indexModel2, ENGLISH_REPR : 'Key Component as [("exceptionID",pymongo.ASCENDING)], KWARGS as {name:GLT_BCM_COMPOSITEKEY,unique:True}' }
             ]
     }

    '''

    # First of all we will take index_dict's INDEXES key as follows:
    # index_dict['INDEXES']. OK NO. INDEXES value will be passed. So starting point is dict with keys as items.
    per_collection_dict = {}
    if len(index_dict) != 0:
        for key, value in index_dict.items():
            # below will form the dict with key as collection and against it ,
            # we will have the list of pymongo indexModel objects
            per_collection_dict[value.COLLECTION_NAME] = per_collection_dict.get(value.COLLECTION_NAME, []) \
                                                             + [{'INDEX_MODEL': IndexModel(value.KEY_COMPONENT, **value.KWARGS)
                                                                 , 'ENGLISH_REPR': f'KEY Component as {value.KEY_COMPONENT}'
                                                                                   f', KWARGS as {value.KWARGS}'}]

        # Above will return the IndexModels created against the keys as collection names.
        logger.debug(f' INdex MOdel created for Mongo , following is the dictionary : {per_collection_dict}')

    return per_collection_dict


def check_which_indexes_exist(mongo_client, index_dict):
    ''' This method returns information as to which all indexes exist.

        Input:
        Index dict is passed as below.
        {'KEY1': IndexKeys([("COMPOSITEKEY", 'ASCENDING')], 'EXCEPTION_TFA02_IFA19_1', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'})
        , 'KEY2': IndexKeys([("runID", 'ASCENDING')], 'EXCEPTION_TFA02_IFA19_1', {'unique': False, 'name': 'GLT_BCM_runID'})
        , 'KEY3': IndexKeys([("exceptionID", 'ASCENDING')], 'EXCEPTION_TFA02_IFA19_1', {'unique': False, 'name': 'GLT_BCM_exceptionID'})}

        Now, we formulate the index dict as following:
        {'EXCEPTION_TFA02_IFA19_1': {'KEY1': ['COMPOSITEKEY'], 'KEY2': ['runID'], 'KEY3': ['exceptionID']}} -- i.e. list of the keys needed for every collection.
        {'TFA02': {'KEY1': ['COMPOSITEKEY', 'TESTING'], 'KEY2': ['runID'], 'KEY3': ['exceptionID']}}

        We get information over the keys from the Mongo via index_information() call to it for the respective collections,
        which returns info as :
        {'_id_': {'v': 2, 'key': [('_id', 1)]}, 'COMPOSITEKEY_1': {'v': 2, 'unique': True, 'key': [('COMPOSITEKEY', 1)]}
        , 'exceptionID_1': {'v': 2, 'key': [('exceptionID', 1)]}, 'exception_Id_1': {'v': 2, 'key': [('exception_Id', 1)]}}

        and formulate the results as follows:
        {'EXCEPTION_TFA02_IFA19_1': [['_id'], ['COMPOSITEKEY'], ['exceptionID'], ['exception_Id']]}

        Now, we compare for every KEY1, KEY2 .. whether the keys are matching with the list items of the list.
        i.e. 'KEY1': ['COMPOSITEKEY'] compared with each of the list items : ['_id'], ['COMPOSITEKEY'], ['exceptionID'], ['exception_Id'].
        these to be matched converting to SET.

    '''
    passed_index_dict = index_dict
    index_keys_to_created_dict = dict()

    # Structuring the index_dict to the needed one.
    # {'EXCEPTION_TFA02_IFA19_1': {'KEY1': ['COMPOSITEKEY'], 'KEY2': ['runID'], 'KEY3': ['exceptionID']}}
    if len(passed_index_dict) != 0:
        db = mongo_client.get_database()

        with mongo_client.start_session() as session:
            structured_passed_index_dict = dict()

            for key_name, value in passed_index_dict.items():
                collection_name = value.COLLECTION_NAME
                structured_passed_index_dict[collection_name] = structured_passed_index_dict.get(collection_name, {})
                structured_passed_index_dict[collection_name][key_name] = [item[0] for item in value.KEY_COMPONENT]

            logger.debug(f'The structured passed index dict is {structured_passed_index_dict} ')

            # Now, for each of the collection getting the index_information from the database.

            structured_existing_indexes_dict = dict()  # initializing

            try:
                # Here, in loop in for every collection
                for coll_name in structured_passed_index_dict.keys():
                    # returned_from_mongo_dict = dict()   # initializing, not needed as its populated with return value below.

                    # Call To Mongo
                    returned_from_mongo_dict = db[coll_name].index_information()

                    pre_structured_existing_indexes_dict = {k: v['key'] for k, v in returned_from_mongo_dict.items()}
                    # above does the formatting as :
                    # {'_id_': [('_id', 1)], 'COMPOSITEKEY_1': [('COMPOSITEKEY', 1)], 'exceptionID_1': [('exceptionID', 1)], 'exception_Id_1': [('exception_Id', 1)]}
                    logger.debug(f'The pre_structured_existing_indexes_dict  is {pre_structured_existing_indexes_dict} ')

                    for existing_index_key_name, field_direction_tuple_list in pre_structured_existing_indexes_dict.items():
                        structured_existing_indexes_dict[coll_name] = structured_existing_indexes_dict.get(coll_name,[]) \
                                                                      + [[field_direction_tuple[0] for field_direction_tuple in field_direction_tuple_list]]

                    # In the above the list comprehension is being used, to traverse though the field_direction_tuple_list
                    # Above returns as the following, see it close ,it's a list in a list and hence above has list_comprehension in a list
                    # {'EXCEPTION_TFA02_IFA19_1': [['_id'], ['COMPOSITEKEY'], ['exceptionID'], ['exception_Id']]}

                    logger.debug(f'The structured_existing_indexes_dict  is {structured_existing_indexes_dict} ')

                # Now having the structured_existing_indexes_dict and structured_passed_index_dict
                # we need to compare which indexes are existing and which are not.
                # E.g. structured_passed_index_dict = {'EXCEPTION_TFA02_IFA19_1': {'KEY1': ['COMPOSITEKEY'], 'KEY2': ['runID'], 'KEY3': ['exceptionID']}}
                # E.g. structured_existing_indexes_dict = {'EXCEPTION_TFA02_IFA19_1': [['_id'], ['COMPOSITEKEY'], ['exceptionID'], ['exception_Id']]}

                for coll_name, needed_keys_info_dict in structured_passed_index_dict.items():
                    if structured_existing_indexes_dict.get(coll_name,0) ==0:
                        # no index exist for this collection , all keys to be created.

                        index_keys_to_created_dict.update({key_name: True for key_name, key_fields in needed_keys_info_dict.items()})

                    else:
                        # Making it set for comparison - sorted and de-duplicated.
                        print('structured_existing_indexes_dict', structured_existing_indexes_dict, structured_existing_indexes_dict[coll_name])
                        set_converted_existing_indexes_4_this_coll = [set(key_list) for key_list in structured_existing_indexes_dict[coll_name]]

                        print('set_converted_existing_indexes_4_this_coll', set_converted_existing_indexes_4_this_coll)
                        # set_converted_existing_indexes_4_this_coll [{'d', '_', 'i'}, {'M', 'C', 'I', 'E', 'K', 'P', 'T', 'Y', 'O', 'S'}]

                        for key_name, key_fields in needed_keys_info_dict.items():

                            # comparing sets on both sides.
                            if set(key_fields) in set_converted_existing_indexes_4_this_coll:
                                index_keys_to_created_dict.update({key_name: False})
                            else:
                                index_keys_to_created_dict.update({key_name: True})

            except Exception as error:
                logger.error(f' Error encountered in the above code block; while error being {error}')
                raise
    print('index_keys_to_created_dict', index_keys_to_created_dict)
    return index_keys_to_created_dict




