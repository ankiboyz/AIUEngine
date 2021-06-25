# control_metadata.py
'''
Here, we will keep the metadata for the control.
Here, there are two segments of metadata , one is global that is for all controls,
and is available to be Overridden in CONTROL_METADATA for the specific control.

For indexes the structure is such that it can accommodate the compound indexes as well.

For indexes the case of the column also should be matching.
This is to be taken care of else the command to create index executes without throwing any error, and is created on a
non-existing field. Mongo being a document store can allow that as fields can be added later as well.
The above needs to doubly sure of. Make sure the names of the indexes are unique for each of the index.
'''

from collections import namedtuple
import typing
# IndexKeys = namedtuple('IndexKeys', ['KEY_COMPONENT', 'FUNCTION_COLLECTION', 'KWARGS'])

# IN order to enforce (its only suggestive, will show while editing) data types to the named tuple fields'
# following can be done.
IndexKeys = typing.NamedTuple('IndexKeys', [('KEY_COMPONENT', list), ('FUNCTION_COLLECTION', str), ('KWARGS', dict)])

# test = {'KEY1': IndexKeys([("COMPOSITEKEY", 'ASCENDING')],'TFA02', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'})}
# print(test, test['KEY1'].KEY_COMPONENT)

GLOBAL_CONTROLS_METADATA = {'LIMIT_OF_RECS_PROCESSED': 10000}

# SPECIFIC_CONTROLS_METADATA = {
#                     'TFA02_IFA19_1': {'LIMIT_OF_RECS_PROCESSED': 10,
#                                       # This is a tuple - first tuple being list of tuples
#                                       'INDEXES': {'KEY1': {
#                                                          'KEY_COMPONENT': [("COMPOSITEKEY", 'ASCENDING')]
#                                                          , 'FUNCTION_COLLECTION': 'TFA02'
#                                                          , 'KWARGS': {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'}
#                                                           },
#                                                   'KEY2': {
#                                                          'KEY_COMPONENT': [("runID", 'ASCENDING')]
#                                                          , 'FUNCTION_COLLECTION': 'TFA02'
#                                                          , 'KWARGS': {'unique': False, 'name': 'GLT_BCM_runID'}
#                                                           },
#                                                   'KEY3': {
#                                                          'KEY_COMPONENT': [("exceptionID", 'ASCENDING')]
#                                                          , 'FUNCTION_COLLECTION': 'TFA02'
#                                                          , 'KWARGS': {'unique': False, 'name': 'GLT_BCM_exceptionID'}
#                                                           }
#                                                   }
#
#
#                                       },
#                     'TRE07': {'LIMIT_OF_RECS_PROCESSED': 10,
#                                       # This is a tuple - first tuple being list of tuples
#                                       'INDEXES': {'KEY1': {
#                                                          'KEY_COMPONENT': [("COMPOSITEKEY", 'ASCENDING')],
#                                                          'KWARGS': {'unique': True, 'name': 'GLT_BCM_COMPOSITE_KEY'}
#                                                           }
#                                                   }
#
#
#                                       }
#
#                     }
SPECIFIC_CONTROLS_METADATA = {
                    'TFA02_IFA19_1': {'LIMIT_OF_RECS_PROCESSED': 10,
                                      # This is a tuple - first tuple being list of tuples
                                      'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", 'ASCENDING')], 'TFA02', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'})
                                                  , 'KEY2': IndexKeys([("runID", 'ASCENDING')], 'TFA02', {'unique': False, 'name': 'GLT_BCM_runID'})
                                                  , 'KEY3': IndexKeys([("exceptionID", 'ASCENDING')], 'TFA02', {'unique': False, 'name': 'GLT_BCM_exceptionID'})}
                                      },
                    'TRE07': {'LIMIT_OF_RECS_PROCESSED': 10,
                                      # This is a tuple - first tuple being list of tuples
                                      'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", 'ASCENDING')], 'TRE07', {'unique': True, 'name': 'GLT_BCM_COMPOSITE_KEY'})}
                              }
                              }
