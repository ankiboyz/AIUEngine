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
#     consumer1 = KafkaConsumer("PAY05", bootstrap_servers=["localhost:9092"], auto_offset_reset="earliest"
#                               , enable_auto_commit=True, group_id="PAY05")
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


_executor = ThreadPoolExecutor(1)
consumer1 = KafkaConsumer("PAY05", bootstrap_servers=["localhost:9092"], auto_offset_reset="earliest"
                              , enable_auto_commit=True, group_id="PAY05")


def sync_blocking():
    time.sleep(10)
    r = consumer1.poll(timeout_ms=1000, max_records=1, update_offsets=True)
    # time.sleep(2)
    return r


async def hello_world():
    # run blocking function in another thread,
    # and wait for it's result:
    # consumer1 = KafkaConsumer("PAY05", bootstrap_servers=["localhost:9092"], auto_offset_reset="earliest"
    #                           , enable_auto_commit=True, group_id="PAY05")

    r = await loop.run_in_executor(_executor, sync_blocking)
    print(r)

loop = asyncio.get_event_loop()
loop.run_until_complete(hello_world())
loop.close()