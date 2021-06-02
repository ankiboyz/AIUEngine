# config.py
""""This file contains the configurations for the application - DB, Logging , """
import os


'''Having the DB file at the Current Working Directory'''
_cwd = os.path.dirname(os.path.abspath(__file__))

"""These specific configurations which need to be available even before the application context is made available.
These are for all the APPs. NO overrides available inside each app for these settings."""
LOG_LEVEL = 'DEBUG'   # not gets applicable as the app starts the previous loggers are disabled.
# Below is the value of the control type for CCM that needs to be made available.
# This section can also store some attributes which might be needed across an app, not necessarily from app_context but
# as similar to global variables.
OPERATION_TYPE = 'BCM'

# Main daemon thread for handling jobs in seconds
JOB_HANDLER_THREAD_POLL_FREQUENCY = 40

# Engine related Settings
# Engine ID can be used to refer to specific instance of the Engine running, in case multiple are running in d
# cluster; example as in load balancer scenario. If any specific operations need be done via this engine the
# ENGINE_ID identifier will be helpful. As far as the IDs are uniquely identifiable any name can be provided to 'em.
ENGINE_ID = 'EINSTEIN'

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
    LIST_OF_CONTROLS = ['PAY05', 'TFA02_IFA19', 'TFA02_IFA19_SC7', 'TRE07', 'TFA02_IFA19']

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
    SQLALCHEMY_DATABASE_URI = 'oracle://SIDDHANT:green123@192.168.2.18:2020/platform'
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False  # enabled True for now
    # https://flask-sqlalchemy.palletsprojects.com/en/2.x/api/#models
    # In debug mode Flask-SQLAlchemy will log all the SQL queries sent to the database. This information is
    # available until the end of request which makes it possible to easily ensure that the SQL generated is the
    # one expected on errors or in unittesting. If you don’t want to enable the DEBUG mode for your unittests
    # you can also enable the query recording by setting the 'SQLALCHEMY_RECORD_QUERIES'
    # config variable to True. This is automatically enabled if Flask is in testing mode.
    SQLALCHEMY_RECORD_QUERIES = True
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
    # Python is case sensitive ).
    # Temp-comm: need to code in for newer thread spawning code.
    WHETHER_SUBMIT_TO_KAFKA = True
    # This setting defines the maximum number of consumers to be spawned at one time.
    MAX_NUM_OF_CONSUMERS_AT_ONE_TIME = 2

    # this is the list of bootstrap-servers, currently kafka-python provides to connect via broker urls
    # and not by directly connecting to Zookeeper , which the .sh utilities provide.
    KAFKA_BROKER_URLS = ['localhost:9092', ]

    # Kafka Consumer properties
    KAFKA_CONSUMER_AUTO_OFFSET_RESET = 'earliest'   # values possible earliest or latest
    KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = True
    # Kafka Consumer poll timeout in Milliseconds
    KAFKA_CONSUMER_POLL_TIMEOUT_MS = 10000
    # This is the number of count if gathered by the consumer during consecutive polls as to not having any message
    # then the consumer will come down to give way to other consumers.
    KAFKA_CONSUMER_CONSECUTIVE_NO_RECS_TO_SIGNAL_EXIT = 2


class ProductionConfig(Config):
    DEBUG = False


class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True
    SQLALCHEMY_ECHO = True


class StagingConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class TestingConfig(Config):
    TESTING = True

