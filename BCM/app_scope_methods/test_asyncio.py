import asyncio
from datetime import datetime
from kafka import KafkaConsumer, BrokerConnection

import asyncio
from datetime import datetime

import asyncio
from concurrent.futures import ThreadPoolExecutor

#
# # Define a coroutine that takes in a future
#
# _executor = ThreadPoolExecutor(1)
#
# async def myCoroutine(future, consumer1):
#     # simulate some 'work'
#     # await asyncio.sleep(1)
#     # # set the result of our future object
#     # future.set_result("My Coroutine-turned-future has completed")
#     r = await consumer1.poll(timeout_ms=10, max_records=1, update_offsets=True)
#     return r
#
#
# async def main():
#     # define a future object
#     future = asyncio.Future()
#
#     consumer1 = KafkaConsumer("REALIZER", bootstrap_servers=["localhost:9092"], auto_offset_reset="earliest"
#                               , enable_auto_commit=True, group_id="REALIZER")
#
#     # wait for the completion of our coroutine that we've
#     # turned into a future object using the ensure_future() function
#
#     # await asyncio.ensure_future(myCoroutine(future))
#
#     await loop.run_in_executor(_executor, myCoroutine(future,consumer1))
#
#     # await asyncio.ensure_future(myCoroutine(future, consumer1))
#
#
#     # Print the result of our future
#     print(future.result())
#
#
# # Spin up a quick and simple event loop
# # and run until completed
# loop = asyncio.get_event_loop()
# try:
#     loop.run_until_complete(main())
# finally:
#     loop.close()
#
###########################
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

#
# _executor = ThreadPoolExecutor(1)
# consumer1 = KafkaConsumer("REALIZER", bootstrap_servers=["localhost:9092"], auto_offset_reset="earliest"
#                               , enable_auto_commit=True, group_id="REALIZER")
#
#
# def sync_blocking():
#     time.sleep(10)
#     r = consumer1.poll(timeout_ms=1000, max_records=1, update_offsets=True)
#     # time.sleep(2)
#     return r


# async def hello_world():
#     # run blocking function in another thread,
#     # and wait for it's result:
#     # consumer1 = KafkaConsumer("REALIZER", bootstrap_servers=["localhost:9092"], auto_offset_reset="earliest"
#     #                           , enable_auto_commit=True, group_id="REALIZER")
#
#     r = await loop.run_in_executor(_executor, sync_blocking)
#     print(r)
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(hello_world())
# loop.close()
# import BCM.app_scope_methods.control_logic_library.list_of_mongo_statements as list_of_mongo_statements
AG_PIPELINE_TFA02_IFA19_1_3 = '[{{"$match": {{"runID": {{"$eq": "{run_id}" }}\
                                                                 ,"GLT_is_this_realized": {{"$ne": "DONE"}} \
                                                                }}\
                                                     }}, # refer all  those where neq DONE\
                              {{"$limit": {limit_for_recs_processing_in_one_iteration}}}\
                              # {{"$project": {{"_id": 0}}}}, # comment this bcoz on the Function Collection no unique key on COMPOSITEKEY\
                             ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()\
                                             , "GLT_is_this_realized": "IN-PROCESS"	# \
                                              }}\
                              }}\
                             ,{{"$merge" : {{ "into": "{function_id}"\
                                          , "on": "_id" # to be on the _id\
                                          , "whenMatched":[ # not merging here, coz there could have been some other fields updated by workflow or other engine\
                                                         {{"$addFields":\
                                                                    {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                    ,"GLT_is_this_realized": "$$new.GLT_is_this_realized"\
                                                                    }}\
                                                         }}\
                                                        ]\
                                          , "whenNotMatched": "discard" }} # this should not be the case since the doc is picked from this source only\
                              }},\
                              ]'
function_id = 'TFA02_COPY'
run_id = 123
limit_for_recs_processing_in_one_iteration = 10
mongo_pipeline_code_str = AG_PIPELINE_TFA02_IFA19_1_3
call_to_be_executed_pre_str = 'db_from_uri_string.{function_id}.aggregate(' \
                                      + mongo_pipeline_code_str \
                                      + ',allowDiskUse=True)'
returned_op_str = 'AA'
start_time = time.time()
call_to_get_execution_str = 'returned_op_str=' + 'f\'' + call_to_be_executed_pre_str + '\''
dynamic_execution_to_stringify_call = exec(call_to_get_execution_str)
print(returned_op_str, dynamic_execution_to_stringify_call)