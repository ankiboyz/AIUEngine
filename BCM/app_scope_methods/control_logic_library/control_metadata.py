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
Also the KWARGS have been put here with key names those are supported by Mongo index creation.
'''

from collections import namedtuple
import typing
import pymongo
# IndexKeys = namedtuple('IndexKeys', ['KEY_COMPONENT', 'FUNCTION_COLLECTION', 'KWARGS'])

# IN order to enforce (its only suggestive, will show while editing) data types to the named tuple fields'
# following can be done.
IndexKeys = typing.NamedTuple('IndexKeys', [('KEY_COMPONENT', list), ('COLLECTION_NAME', str), ('KWARGS', dict)])

# test = {'KEY1': IndexKeys([("COMPOSITEKEY", 'ASCENDING')],'TFA02', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'})}
# print(test, test['KEY1'].KEY_COMPONENT)

GLOBAL_CONTROLS_METADATA = {'LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION': 100000}

SPECIFIC_CONTROLS_METADATA = {
                    'TFA02_IFA19_1': {# 'LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION': 13,
                                      # This is a tuple - first tuple being list of tuples
                                      'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING)], 'EXCEPTION_TFA02_IFA19_1', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'})
                                                  , 'KEY2': IndexKeys([("runID", pymongo.ASCENDING)], 'EXCEPTION_TFA02_IFA19_1', {'unique': False, 'name': 'GLT_BCM_runID'})
                                                  , 'KEY3': IndexKeys([("exceptionID", pymongo.ASCENDING)], 'EXCEPTION_TFA02_IFA19_1', {'unique': False, 'name': 'GLT_BCM_exceptionID'})
                                                  , 'KEY4': IndexKeys([("BUKRS", pymongo.ASCENDING), ("ANLN1", pymongo.ASCENDING), ("ANLN2", pymongo.ASCENDING), ("FNAME", pymongo.ASCENDING), ("EXCEPTION", pymongo.ASCENDING),
                                                                       ("SYSTEM", pymongo.ASCENDING), ("status", pymongo.ASCENDING)], 'EXCEPTION_TFA02_IFA19_1', {'unique': False, 'name': 'GLT_BCM_BUKRS_ANLN1_ANLN2_FNAME_EXCEPTION_SYSTEM_status'})
                                                  }
                                      },
                    'TFA02_IFA19_SC7_1': {# 'LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION': 13,
                                          # This is a tuple - first tuple being list of tuples
                                          'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING)], 'EXCEPTION_TFA02_IFA19_SC7_1', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY'})
                                                      , 'KEY2': IndexKeys([("runID", pymongo.ASCENDING)], 'EXCEPTION_TFA02_IFA19_SC7_1', {'unique': False, 'name': 'GLT_BCM_runID'})
                                                      , 'KEY3': IndexKeys([("exceptionID", pymongo.ASCENDING)], 'EXCEPTION_TFA02_IFA19_SC7_1', {'unique': False, 'name': 'GLT_BCM_exceptionID'})
                                                      }
                                           },
                    'TRE07_1': {  # 'LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION': 10,
                                      # This is a tuple - first tuple being list of tuples
                                      'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING)], 'EXCEPTION_TRE07_1', {'unique': True, 'name': 'GLT_BCM_COMPOSITE_KEY'})
                                                  , 'KEY2': IndexKeys([("runID", pymongo.ASCENDING)], 'EXCEPTION_TRE07_1', {'unique': False, 'name': 'GLT_BCM_runID'})
                                                  , 'KEY3': IndexKeys([("exceptionID", pymongo.ASCENDING)], 'EXCEPTION_TRE07_1', {'unique': False, 'name': 'GLT_BCM_exceptionID'})
                                                  , 'KEY4': IndexKeys([("AUGBL", pymongo.ASCENDING), ("GJAHR", pymongo.ASCENDING), ("BUKRS", pymongo.ASCENDING)
                                                                      , ("HKONT", pymongo.ASCENDING), ("SAP", pymongo.ASCENDING), ("EXP_STATUS", pymongo.ASCENDING)]
                                                                      , 'EXCEPTION_TRE07_1', {'unique': False, 'name': 'GLT_BCM_AUGBL_GJAHR_BUKRS_HKONT_SAP_EXP_STATUS'})
                                                  }
                                },
                    'FIN08_FA_1': {  # 'LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION': 10,
                                      # This is a tuple - first tuple being list of tuples
                                      'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING),("GLT_incremental_number",pymongo.DESCENDING)], 'EXCEPTION_FIN08_FA_1', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY_incremental_number'})
                                                  , 'KEY2': IndexKeys([("runID", pymongo.ASCENDING)], 'EXCEPTION_FIN08_FA_1', {'unique': False, 'name': 'GLT_BCM_runID'})
                                                  , 'KEY3': IndexKeys([("exceptionID", pymongo.ASCENDING)], 'EXCEPTION_FIN08_FA_1', {'unique': False, 'name': 'GLT_BCM_exceptionID'})
                                                  , 'KEY4': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING)], 'EXCEPTION_FIN08_FA_1', {'unique': False, 'name': 'GLT_BCM_COMPOSITEKEY'})
                                                  }
                                   },
                    'FIN08_AP_AR_1': {  # 'LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION': 10,
                                      # This is a tuple - first tuple being list of tuples
                                      'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING),("GLT_incremental_number",pymongo.DESCENDING)], 'EXCEPTION_FIN08_AP_AR_1', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY_incremental_number'})
                                                  , 'KEY2': IndexKeys([("runID", pymongo.ASCENDING)], 'EXCEPTION_FIN08_AP_AR_1', {'unique': False, 'name': 'GLT_BCM_runID'})
                                                  , 'KEY3': IndexKeys([("exceptionID", pymongo.ASCENDING)], 'EXCEPTION_FIN08_AP_AR_1', {'unique': False, 'name': 'GLT_BCM_exceptionID'})
                                                  , 'KEY4': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING)], 'EXCEPTION_FIN08_AP_AR_1', {'unique': False, 'name': 'GLT_BCM_COMPOSITEKEY'})
                                                  }
                                   },
                    'FIN08_INVENTORY_1': {  # 'LIMIT_FOR_RECS_PROCESSING_IN_ONE_ITERATION': 10,
                                      # This is a tuple - first tuple being list of tuples
                                      'INDEXES': {'KEY1': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING),("GLT_incremental_number",pymongo.DESCENDING)], 'EXCEPTION_FIN08_INVENTORY_1', {'unique': True, 'name': 'GLT_BCM_COMPOSITEKEY_incremental_number'})
                                                  , 'KEY2': IndexKeys([("runID", pymongo.ASCENDING)], 'EXCEPTION_FIN08_INVENTORY_1', {'unique': False, 'name': 'GLT_BCM_runID'})
                                                  , 'KEY3': IndexKeys([("exceptionID", pymongo.ASCENDING)], 'EXCEPTION_FIN08_INVENTORY_1', {'unique': False, 'name': 'GLT_BCM_exceptionID'})
                                                  , 'KEY4': IndexKeys([("COMPOSITEKEY", pymongo.ASCENDING)], 'EXCEPTION_FIN08_INVENTORY_1', {'unique': False, 'name': 'GLT_BCM_COMPOSITEKEY'})
                                                  }
                                   }
                            }
