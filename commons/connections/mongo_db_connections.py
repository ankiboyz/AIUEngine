# mongo_db_connections.py
'''
Author: Ankur Saxena
Objective: The objective of this module is to provide methods/ decorators for the connection handling to
            mongo db database.
'''
import logging
from pymongo import MongoClient

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