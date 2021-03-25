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
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(_cwd, 'AIUEngine.db')
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    CFG_LOG_PATH_DFLT = 'log_cfg_dflt.yaml'  # This has to be default log config in absence of an override in APP this should prevail.


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

