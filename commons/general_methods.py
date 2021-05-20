#general_methods.py
import datetime
import random
from CCM import models

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
    '''

    # i.e. extract attrib if its first 2 chars are not __
    # to find out the class properties following is the way:
    # class.__dict__.keys() gives o/p as dict_keys(['__module__', '__tablename__', 'engine_id', 'control_id', 'status'
    # , '__doc__', '_sa_class_manager', '__table__', '__init__', '__mapper__'])
    # for class in models without __ will be the column names.
    list_of_columns = [keyname for keyname in cls.__dict__.keys() if (keyname[:2] != '__' and keyname[:1] != '_')]
    print(list_of_columns)
    print(list_of_obj)
    for row in list_of_obj: # with dict comprehension we can create the dict of series of
        for cols in list_of_columns:
            # print(row.@cols)
            print(getattr(row, cols))



# test
# df_for_models_table(models.CCMControlEngineAssoc, 'abc')
