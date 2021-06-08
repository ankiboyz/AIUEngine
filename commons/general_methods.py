#general_methods.py
import logging
import datetime
import random
from BCM import models
from BCM.app_scope_methods import list_of_sql_stmnts
import config

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

''' This module provide general methods. '''


def generate_passport():
    ''' This method generates a passport number '''

    now = datetime.datetime.now()
    date_time_concat = now.strftime("%Y%m%d%H%M%S%f")
    random_num = random.randint(1000, 9999)      # Generating a 4 digit random number
    now_an_id = str(date_time_concat)+str(random_num)

    print('Passport Number', now_an_id)
    return now_an_id


def df_for_models_table(cls, list_of_obj):

    ''' This method returns the dataframe for a table that is in the models class.
    input: the class and the list of objects is passed; each obj is an instance of the passed class.

    !!! Use of this is commented for the time being

    '''

    # i.e. extract attrib if its first 2 chars are not __
    # to find out the class properties following is the way:
    # class.__dict__.keys() gives o/p as dict_keys(['__module__', '__tablename__', 'engine_id', 'control_id', 'status'
    # , '__doc__', '_sa_class_manager', '__table__', '__init__', '__mapper__'])
    # for class in models without __ will be the column names.
    # list_of_columns = [keyname for keyname in cls.__dict__.keys() if (keyname[:2] != '__' and keyname[:1] != '_')]

    # we can also get the list of columns from this without putting in the custom logic
    # print(models.CCMControlEngineAssoc.__dict__["__mapper__"].column_attrs.keys())
    list_of_columns = cls.__dict__["__mapper__"].column_attrs.keys()
    print(list_of_columns)
    print(list_of_obj)
    for row in list_of_obj: # with dict comprehension we can create the dict of series of
        for cols in list_of_columns:
            # print(row.@cols)
            print(getattr(row, cols))


def get_the_sql_str_for_db(sql_str_id, db_vendor):

    sql_id = sql_str_id + '_' + db_vendor
    sql_str = getattr(list_of_sql_stmnts, sql_id)
    logger.debug(f' SQL string gathered :{sql_str} ')

    return sql_str


# test
# df_for_models_table(models.CCMControlEngineAssoc, 'abc')
