# mongo_db.py
'''
Author: Ankur Saxena
Objective: The objective of this module is to provide methods/ decorators for the connection handling to
            mongo db database.
            Also various mongo related operations
'''
import logging
from pymongo import MongoClient, errors as pymonerrors

logger = logging.getLogger(__name__)


def create_mongo_client(mongodb_uri):
    ''' Objective of the method is to get the mongo connection and return it to the caller.
    Input : The URI of the mongo client.
    Output : MongoClient in case of success
             Boolean False in case of an error

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

    mc = MongoClient(mongodb_uri)
    mc_or_False = False

    try:
        # light weight command used to check for the Client credentials , connectivity etc.
        mc.admin.command('ismaster')
        logger.info('Mongo client is able to Connect')
        mc_or_False = mc

    except Exception as error:
        logger.error(f'ERROR encountered {error}', exc_info=True)
        mc_or_False = False

    return mc_or_False

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
            db[collection_name].create_indexes(list_of_index_models)
            return True
        except Exception as error:
            logger.error(f' Encountered error while creating indexes for the '
                         f'collection {collection_name}, error being {error}', exc_info=True)
            raise


def create_indexmodels(index_dict):
    ''' Here , the index dict is presented , that is maintained in the control_metadata.py file
    The structure that is maintained in the form as below:
    'INDEXES':
    {
    'KEY1': {
             'KEY_COMPONENT': [("COMPOSITEKEY", 'ASCENDING')]
             , 'FUNCTION_COLLECTION': 'TFA02'
             , 'KWARGS': {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'}
              },
    'KEY2': {
             'KEY_COMPONENT': [("runID", 'ASCENDING')]
             , 'FUNCTION_COLLECTION': 'TFA02'
             , 'KWARGS': {'unique': False, 'name': 'GLT_BCM_runID'}
              },
    'KEY3': {
             'KEY_COMPONENT': [("exceptionID", 'ASCENDING')]
             , 'FUNCTION_COLLECTION': 'TFA02'
             , 'KWARGS': {'unique': False, 'name': 'GLT_BCM_exceptionID'}
              }
    }
    And the index model for above will be :
    IndexModel([("COMPOSITEKEY",pymongo.ASCENDING)],name="GLT_BCM_COMPOSITEKEY",unique=True)
    IndexModel([("runID",pymongo.ASCENDING)],name="GLT_BCM_runID",unique=False)
    IndexModel([("exceptionID",pymongo.ASCENDING)],name="GLT_BCM_exceptionID",unique=False)
    '''

    # First of all we will take index_dict's INDEXES key as follows:
    # index_dict['INDEXES']. OK NO. INDEXES value will be passed. So starting point is dict with keys as items.


