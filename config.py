# config.py
""""This file contains the configurations for the application - DB, Logging , """
import os


'''Having the DB file at the Current Working Directory'''
_cwd = os.path.dirname(os.path.abspath(__file__))

"""These specific configurations which need to be available even before the application context is made available.
These are for all the APPs. NO overrides available inside each app for these settings."""
LOG_LEVEL = 'DEBUG'   # not gets applicable as the app starts the previous loggers are disabled.
# Below is the value of the control type for BCM that needs to be made available.
# This section can also store some attributes which might be needed across an app, not necessarily from app_context but
# as similar to global variables.
OPERATION_TYPE = 'BCM'

# Main daemon thread for handling jobs in seconds (ideally to be kept as 40 or so)
JOB_HANDLER_THREAD_POLL_FREQUENCY = 40

# Engine related Settings
# Engine ID can be used to refer to specific instance of the Engine running, in case multiple are running in d
# cluster; example as in load balancer scenario. If any specific operations need be done via this engine the
# ENGINE_ID identifier will be helpful. As far as the IDs are uniquely identifiable any name can be provided to 'em.
ENGINE_ID = 'EINSTEIN'

# The host ip the service will be visible as running on, 0.0.0.0 ensures the service running on all IPs of the server
# where the service is deployed.
HOST_IP = '0.0.0.0'
# Ports 49152-65535
PORT = '50008'

# This determine which set of the configurations as defined below per class will be taken up.
# for dev keep it as 'config.DevelopmentConfig'
# for prod keep it as 'config.ProductionConfig'
# 'config.TestingConfig'
# For current testing keeping it as 'config.StagingConfig'
APP_CONFIG_MODE = 'config.DevelopmentConfig'

'''All Configurations here DB as well as Logging based on the environment'''

class Config(object):
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True

    '''SECRET_KEY A secret key that will be used for securely signing the session cookie and 
    can be used for any other security related needs by extensions or your application. 
    It should be a long random string of bytes, although unicode is accepted too. For example, 
    copy the output of this to your config:
    '''
    @property
    def SECRET_KEY(self):   # Note: it has to be all UPPERCASE to work with config load via from_object(object)
        secretkey = os.urandom(16)
        print(secretkey)
        return secretkey

    # list of controls to be undertaken by this APP.
    # WORD OF CAUTION: Kindly ensure the list of controls entered are all Unique!
    # At every restart of the APP the list of controls is inserted into the DB for the specific EngineID
    LIST_OF_CONTROLS = ['PAY05', 'TFA02_IFA19_1', 'TFA02_IFA19_SC7_1', 'TRE07_1', 'TFA02_IFA19_1', 'FIN08_FA_1', 'FIN08_AP_AR_1'
        , 'FIN08_INVENTORY_1']

    # This is the maximum limit of the records to be processed in one iteration in MONGO for a control.
    # This is maintained in the control metadata now.
    # LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION = 10

    # Out of the list of controls (as specified in LIST_OF_CONTROLS) below lists which all controls need to have
    # multiprocessing mode i.e. wherein the control processing would require CPU of engine to do some intermediary
    # processing and does not entirely depend on the aggregation pipeline @mongo.
    LIST_OF_MULTI_PROC_CONTROLS = ['TRE07']

    '''SQLALCHEMY_ECHO If set to True SQLAlchemy will log all the statements issued to stderr 
    which can be useful for debugging.
    '''
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(_cwd, 'AIUEngine.db')
    # For ORACLE , DEFAULT would suffice.
    # idea is to have 'DEFAULT' for all Dbs where the queries specified in list_of_sql_stmnts.py are compatible.
    DATABASE_VENDOR = 'DEFAULT'
    # SQLALCHEMY_DATABASE_URI = 'oracle://SIDDHANT:green123@192.168.2.18:2020/platform'
    SQLALCHEMY_DATABASE_URI = 'oracle://EBCPDEV2:green123@192.168.2.217:2020/IRMRTA'
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False  # enabled True for now
    # https://flask-sqlalchemy.palletsprojects.com/en/2.x/api/#models
    # In debug mode Flask-SQLAlchemy will log all the SQL queries sent to the database. This information is
    # available until the end of request which makes it possible to easily ensure that the SQL generated is the
    # one expected on errors or in unittesting. If you don’t want to enable the DEBUG mode for your unittests
    # you can also enable the query recording by setting the 'SQLALCHEMY_RECORD_QUERIES'
    # config variable to True. This is automatically enabled if Flask is in testing mode.
    SQLALCHEMY_RECORD_QUERIES = True

    # SQLALCHEMY_POOL_SIZE: The size of the database pool.Defaults to the engine's default (usually 5).
    # Controls the number of connections that can be created after the pool reached its maximum size.
    # SQLALCHEMY_MAX_OVERFLOW: When those additional connections are returned to the pool, they are disconnected and discarded.
    # Deprecated as of v2.4 and will be removed in v3.0.
    SQLALCHEMY_POOL_SIZE = 15
    SQLALCHEMY_MAX_OVERFLOW = 20
    # This oracle client path to the oracle instant client only works for windows server.
    # If the application is installed on Linux then the instant client need to be INSTALLED on the machine,
    # and as in windows simply unzipping the zipped archive of instant client
    # does not work on Linux.(verified on Oracle Linux 7). For Linux it should be installed.
    ORACLE_CLIENT_PATH = r"C:\ORACLEINSTANTCLIENT\instantclient_19_10"

    # This has to be default path to provide log config details,in absence of an override in APP,this should prevail.
    # Currently this file is not filled with the logging configuration values.
    # IN case this file is within the project path then use relative path else mention the Absolute path.
    # In below example it is relative path since the file is within the project and is part of the executables.
    # Unless until some special need is encountered this path is not to be changed.
    LOG_CNFG_PATH = './log_cnfg_dflt.yaml'

    # Kafka Related Settings
    # This stores the information related to the Kafka cluster, all the brokers in the cluster to be used by the engine.

    # This setting would determine whether the message will be submitted to Kafka (if yes), else the processing will
    # proceed by spawning new threads whenever the call arrives. Value is True/ False (BEWARE: boolean values in
    # Python is case sensitive . correct values with right case are True / False).
    # Temp-comm: need to code in for newer thread spawning code.
    WHETHER_SUBMIT_TO_KAFKA = True

    # Below are thread related settings.
    # This setting defines the maximum number of threads to be spawned at one time if WHETHER_SUBMIT_TO_KAFKA is False.
    MAX_NUM_OF_THREADS_AT_ONE_TIME = 2
    # This setting defines the sleep time between polling to find out the next job id to execute within a control id lookout thread.
    # This is in Milli Seconds.
    INTRA_THREAD_POLL_FREQUENCY_MS = 5000
    # This is number of consecutive attempts to get some processing, if not found then gracefully exits to make way for other.
    INTRA_THREAD_CONSECUTIVE_NO_RECS_TO_SIGNAL_EXIT = 2


    # Below are Kafka consumer related settings
    # This setting defines the maximum number of consumers to be spawned at one time.
    MAX_NUM_OF_CONSUMERS_AT_ONE_TIME = 5
    # this is the list of bootstrap-servers, currently kafka-python provides to connect via broker urls
    # and not by directly connecting to Zookeeper , which the .sh utilities provide.
    KAFKA_BROKER_URLS = ['localhost:9092', ]

    # Kafka Consumer properties
    KAFKA_CONSUMER_AUTO_OFFSET_RESET = 'latest'   # values possible earliest or latest
    KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = True
    # Kafka Consumer poll timeout in Milliseconds
    KAFKA_CONSUMER_POLL_TIMEOUT_MS = 10000
    # This is the number of count if gathered by the consumer during consecutive polls as to not having any message
    # then the consumer will come down to give way to other consumers.
    KAFKA_CONSUMER_CONSECUTIVE_NO_RECS_TO_SIGNAL_EXIT = 2

    # Collection Storage Engine - currently its MongoDB

    # Future(NotInCurrentRelease) These IDs will be utilized in the pipelines for the execution of the individual stages.
    # Type and Subtype to be used for further distinguishing in case more sophisticated connectivity mechanism need be
    # devised. Later releases Mongo DB name also to be made part of the pipeline.
    # For Current Release , below:
    # Please NOTE NO Whitespace in the URI!!!
    # Please mention the database in the URI itself , code will be connecting to this DB.

    MONGO_DB_CONN_URI = "mongodb://ankur:green123@192.168.2.193:27018/PHILIPS_BCM"\
                        "?authSource=PHILIPS_BCM"\
                        "&authMechanism=SCRAM-SHA-256&readPreference=primary"\
                        "&appname=GLT_BCM_AIUENGINE&ssl=false"

    # This is the EBCP Call Back URL to post the status of the JOB.
    EBCP_CALL_BACK_URL = 'http://127.0.0.1:5001/test'
    # EBCP_CALL_BACK_URL = 'https://10.0.2.214:7073/ebcpplatform/exceptionGenerator/acknowledgeBCMExceptionGeneration'
    # if True then the authentic certificate to be present under the folder commons/external_app_specifics/HTTPSCertificates
    # if False certificate verification is by passed.
    EBCP_VERIFY_CERT = True

    # this is the path to the cert and key file of EBCP , inclusive of the filename.
    EBCP_PATH_TO_CERT = "C:\\Users\\ASaxena\\PycharmProjects\\AIUEngine\\commons\\external_app_specifics\\HTTPSCertificates\\example.crt"
    EBCP_PATH_TO_CERT_KEY = "C:\\Users\\ASaxena\\PycharmProjects\\AIUEngine\\commons\\external_app_specifics\\HTTPSCertificates\\example.key"

class ProductionConfig(Config):
    DEBUG = False
    # SQLALCHEMY_DATABASE_URI = 'oracle://REALIZATION:green123@192.168.2.217:2020/IRMRTA'
    SQLALCHEMY_DATABASE_URI = 'oracle://IRM_TM_DEV:green123@sapsr3db.c6djbyal1avz.us-west-2.rds.amazonaws.com:1521/RCMDB'
    KAFKA_BROKER_URLS = ['192.168.2.239:9092', ]
    EBCP_CALL_BACK_URL = 'https://10.0.2.214:7073/ebcpplatform/exceptionGenerator/acknowledgeBCMExceptionGeneration'
    EBCP_VERIFY_CERT = False
    MONGO_DB_CONN_URI = "mongodb://ankur:green123@10.0.2.184:27017/PHILIPS_BCM" \
                        "?authSource=PHILIPS_BCM" \
                        "&authMechanism=SCRAM-SHA-256&readPreference=primary" \
                        "&appname=GLT_BCM_AIUENGINE&ssl=false"


class DevelopmentConfig(Config):
    # DEVELOPMENT = True
    # DEBUG = True
    # SQLALCHEMY_ECHO = True
    DEVELOPMENT = False
    DEBUG = False  # Only put DEBUG = True only when needed else should be False in regular running else issues may occur.
    SQLALCHEMY_ECHO = True

    WHETHER_SUBMIT_TO_KAFKA = False
    MAX_NUM_OF_CONSUMERS_AT_ONE_TIME = 5
    MAX_NUM_OF_THREADS_AT_ONE_TIME = 5

    # For Testing:
    # SQLALCHEMY_POOL_SIZE = 1
    # SQLALCHEMY_MAX_OVERFLOW = 1
    # SQLALCHEMY_DATABASE_URI = 'oracle://EBCPDEV2:green123@192.168.2.217:2020/REALIZATION'

class StagingConfig(Config):
    DEVELOPMENT = True
    DEBUG = False   # Only put DEBUG = True only when needed else should be False in regular running else issues may occur.

    SQLALCHEMY_DATABASE_URI = 'oracle://BCMDEV:green123@sapsr3db.c6djbyal1avz.us-west-2.rds.amazonaws.com:1521/RCMDB'
    KAFKA_BROKER_URLS = ['10.150.75.36:9092', ]
    # EBCP_CALL_BACK_URL = 'https://philips.gltcloud.com/ebcpplatform-qa/exceptionGenerator/acknowledgeBCMExceptionGeneration'
    EBCP_VERIFY_CERT = False

    MONGO_DB_CONN_URI = "mongodb://BCMDEVUSER:green123@10.0.2.184:27017/BCMDEV" \
                        "?authSource=BCMDEV" \
                        "&authMechanism=SCRAM-SHA-1&readPreference=primary" \
                        "&appname=GLT_BCM_AIUENGINE&ssl=false"

    WHETHER_SUBMIT_TO_KAFKA = False
    MAX_NUM_OF_THREADS_AT_ONE_TIME = 5

    MAX_NUM_OF_CONSUMERS_AT_ONE_TIME = 4


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'oracle://EBCPQA4:green123@192.168.2.240:2020/EBCPQA'
    KAFKA_BROKER_URLS = ['192.168.2.239:9092', ]
    EBCP_CALL_BACK_URL = 'http://192.168.2.44:7080/ebcpplatform/exceptionGenerator/acknowledgeBCMExceptionGeneration'
    EBCP_VERIFY_CERT = False

    MONGO_DB_CONN_URI = "mongodb://gluser:green123@10.0.2.184:27017/GLT_AVM_HENKEL_QA" \
                        "?authSource=GLT_AVM_HENKEL_QA" \
                        "&authMechanism=SCRAM-SHA-1&readPreference=primary" \
                        "&appname=GLT_BCM_AIUENGINE&ssl=false"

    WHETHER_SUBMIT_TO_KAFKA = False
    MAX_NUM_OF_THREADS_AT_ONE_TIME = 5

    MAX_NUM_OF_CONSUMERS_AT_ONE_TIME = 5



