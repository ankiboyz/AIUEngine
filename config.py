# config.py

from os.path import abspath, dirname, join
import os

_cwd = dirname(abspath(__file__))

'''SECRET_KEY A secret key that will be used for securely signing the session cookie and 
can be used for any other security related needs by extensions or your application. 
It should be a long random string of bytes, although unicode is accepted too. For example, 
copy the output of this to your config:

'''
secretkey = os.urandom(16)
print(secretkey)
SECRET_KEY = secretkey

''' SQLALCHEMY_ECHO If set to True SQLAlchemy will log all the statements issued to stderr 
which can be useful for debugging.
'''

SQLALCHEMY_DATABASE_URI = 'sqlite:///' + join(_cwd, 'AIUEngine.db')
SQLALCHEMY_ECHO = True