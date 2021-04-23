# config.py
""""This file contains the configurations for the application - DB, Logging , """
import os

'''Having the DB file at the Current Working Directory'''
_cwd = os.path.dirname(os.path.abspath(__file__))

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

    '''SQLALCHEMY_ECHO If set to True SQLAlchemy will log all the statements issued to stderr 
    which can be useful for debugging.
    '''
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(_cwd, 'AIUEngine.db')
    SQLALCHEMY_DATABASE_URI = 'oracle://SIDDHANT:green123@192.168.2.18:2020/platform'
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ORACLE_CLIENT_PATH = r"C:\ORACLEINSTANTCLIENT\instantclient_19_10"
    # This has to be default path to provide log config details,in absence of an override in APP,this should prevail.
    # Currently this file is not filled with the logging configuration values.
    # IN case this file is within the project path then use relative path else mention the Absolute path.
    # In below example it is relative path since the file is within the project and is part of the executables.
    # Unless until some special need is encountered this path is not to be changed.
    LOG_CNFG_PATH = './log_cnfg_dflt.yaml'


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

