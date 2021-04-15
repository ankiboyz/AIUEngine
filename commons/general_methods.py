#general_methods.py
import datetime
import random

''' This module provide general methods. '''


def generate_passport():
    ''' This method generates a passport number '''

    now = datetime.datetime.now()
    date_time_concat = now.strftime("%Y%m%d%H%M%S%f")
    random_num = random.randint(1000, 9999)      # Generating a 4 digit random number
    now_an_id = str(date_time_concat)+str(random_num)

    print('Passport Number', now_an_id)
    return now_an_id