# kafka_consumers.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods related to the kafka_consumers; eg starting up and checking
                whether the consumers are up or not.
"""

import inspect
import logging, json
from kafka import KafkaConsumer, BrokerConnection
from commons import general_methods
import pandas as pd
import threading
import config
# import asyncio
from BCM.app_scope_methods import control_processing

from BCM import models, app

logger = logging.getLogger(__name__)
db = models.db


def kafka_consumer_algo():
    ''' This algo will uncover the number of consumers need to be spawned '''

    with app.app_context():
        # cntrl_monitor_hdr = models.CCMMonitorHDR()
        # query = cntrl_monitor_hdr.query.filter_by(status=models.StatusEnum.SUBMITTED).all()
        # logger.debug(f'There are these many jobs to be handled {len(query)}')

        # Finding the consumers those are UP
        cntrl_monitor_ngn_assoc = models.CCMControlEngineAssoc()
        query_4_consumers_up = cntrl_monitor_ngn_assoc.query.filter_by(status=models.KafkaConsumerEnum.UP
                                                                      , engine_id=app.config["ENGINE_ID"]).all()
        consumers_that_can_be_awakened = app.config["MAX_NUM_OF_CONSUMERS_AT_ONE_TIME"] - len(query_4_consumers_up)

        logger.info(f'Consumers that can be Awakened for this Engine_ID {app.config["ENGINE_ID"]} are '
                     f'{consumers_that_can_be_awakened}')

        logger.debug(db.get_engine())  # this gets the engine
        flask_sqlalchemy_ngn = db.get_engine()

        # general_methods.df_for_models_table(models.CCMControlEngineAssoc, query_for_consumers)

        # we will take the number of configured kafka consumers to be run at one time.
        # We will sort the jobs those need be handled in the descending order of duration from submission.
        # Start making Consumers up those are needed to cater to the job as per priority gathered above.
        # Stop once the maximum no. of consumers to be up at one time is reached.
        # The consumer who has completed its job  and has not received any new message , need to be come down by itself.

        # This query will list out the consumers those need to be awakened via this engine: taking into context -
        # 1. The jobs those are submitted and still to be tackled in order of submission date
        # 2. List only those jobs whose consumers are DOWN and were supposed to be tackled by this Engine_ID.

        sql_str_for_consumers_to_be_awakened = general_methods.get_the_sql_str_for_db('SQL_ID_1', app.config["DATABASE_VENDOR"])

        df_consumers_to_be_awakened = pd.read_sql(sql_str_for_consumers_to_be_awakened, flask_sqlalchemy_ngn)
        logger.debug(f'consumers to be awakened {df_consumers_to_be_awakened.to_string(index=False)}')
        logger.debug(df_consumers_to_be_awakened.dtypes)

        # After sorting out the df needs to be again equated to the changed df.
        # df_consumers_to_be_awakened = df_consumers_to_be_awakened.sort_values(by="min_start_date", ascending=True)
        # print(df_consumers_to_be_awakened)
        if consumers_that_can_be_awakened > 0:
            # if the consumers to be awakened are n , then awake the top n consumers.
            # For each consumer there will be a thread invoked with a specific function for that consumer.

            # finding out the number of rows of df to be worked upon i.e. whichever is minimum
            # , the num of rows in df for consumers to be awakened or the consumers that can be awakened (which is
            # based on the settings of the max num of consumers those can be awakened.
            # we will take then these top n rows.

            wrk_set_num_rows = min(len(df_consumers_to_be_awakened.index), consumers_that_can_be_awakened)
            logger.debug(f'consumers to be awakened {df_consumers_to_be_awakened.iloc[:wrk_set_num_rows].to_string(index=False)}')

            # Here we get a series object of the consumers to be awakened:
            consumers_to_be_awakened_series = df_consumers_to_be_awakened.iloc[:wrk_set_num_rows]["control_id"]
            logger.debug(f'consumers to be awakened series {consumers_to_be_awakened_series}')

            threads_spawned_list = list()

            for items in consumers_to_be_awakened_series.iteritems():
                print(items[1])     # gather the second index of the tuple to get the control id.
                kfk_control_id = items[1]   # topic id and consumer group id is same as that of the control_id

                # Invoking a thread per consumer given the upper limit of the threads those can be spawned.
                x = threading.Thread(target=making_consumer_up, args=(kfk_control_id, kfk_control_id, app,), daemon=True)
                try:
                    x.start()
                    logger.debug(f'started new thread by main thread {threading.current_thread().name} for {kfk_control_id}')
                    threads_spawned_list.append(x)

                except Exception as error:
                    logger.error(error, exc_info=True)

            # making_consumer_up('REALIZER','REALIZER',app)
            logger.info(f'List of threads for the consumers spawned is {threads_spawned_list}')
            pass


def making_consumer_up(topic_id, group_id, appln_cntxt):
    ''' This method is used to bring up the consumer '''

    # import happening here , since it can be called within the thread.
    from kafka import KafkaConsumer, BrokerConnection
    is_consumer_set = 'N'
    engine_id = appln_cntxt.config["ENGINE_ID"]

    try:
        consumer = KafkaConsumer(topic_id, bootstrap_servers=appln_cntxt.config["KAFKA_BROKER_URLS"]
                                 , auto_offset_reset=appln_cntxt.config["KAFKA_CONSUMER_AUTO_OFFSET_RESET"]
                                 , enable_auto_commit=appln_cntxt.config["KAFKA_CONSUMER_ENABLE_AUTO_COMMIT"]
                                 , group_id=group_id)

        logger.debug(f'Hiii I am in the consumer for {topic_id} for the thread {threading.get_ident()} '
                     f'{threading.current_thread().name}')
        is_consumer_set = 'Y'

    except Exception as error:
        logger.error(error, exc_info=True)
        # consumer.close()

    if is_consumer_set == 'Y':
        # set the status of consumer UP in DB
        models.consumer_status_update_per_control_engine(topic_id, 'UP', appln_cntxt)

        # code shifted to the method
        # with appln_cntxt.app_context():
        #     cntrl_monitor_ngn_assoc = models.CCMControlEngineAssoc()
        #
        #     kfk_control_id = topic_id
        #     db_row_4_status_upd = cntrl_monitor_ngn_assoc.query.filter_by(control_id=kfk_control_id
        #                                                                   , engine_id=appln_cntxt.config["ENGINE_ID"]).all()
        #     for row in db_row_4_status_upd:
        #         logger.info(f' Kafka Consumer control Id being updated for status is  {row.control_id}')
        #         row.status = models.KafkaConsumerEnum.UP        # make it UP
        #         print(f' row modified is {row}')
        #
        #         db.session.commit()

        try:
            # for message in consumer:
            timeout_in_ms = appln_cntxt.config["KAFKA_CONSUMER_POLL_TIMEOUT_MS"]
            consecutive_no_recs_to_signal_exit = appln_cntxt.config["KAFKA_CONSUMER_CONSECUTIVE_NO_RECS_TO_SIGNAL_EXIT"]
            count_consec_empty = 0

            while True:
                message = consumer.poll(timeout_ms=timeout_in_ms, max_records=1, update_offsets=True)

                logger.info(f'Hiii this is the poll output message {message} for {topic_id}   '
                            f'from the thread {threading.current_thread().name}')
                # Only checks for consecutive values been empty
                if not message or len(message) == 0:
                    count_consec_empty = count_consec_empty + 1

                else:
                    count_consec_empty = 0
                    logger.info(f'Hiii this is the NON-Empty message from Kafka for consumer {topic_id} : '
                                f'message is {message} '
                                f'from the thread {threading.current_thread().name}')

                    # Need to process the message here -- call a method and that method will invoke the processing of
                    # the job ID : message will only have the ID and the Control_ID : will interface with the
                    # detail table and also update status of the header table.

                    # .values() method takes the values in a dict and puts them as tuples:
                    # dict_values([[ConsumerRecord(topic='TFA02_IFA19', partition=0, offset=12, timestamp=1622633711601,
                    #                              timestamp_type=0, key=None,
                    #                              value=b'{"RUNID": 2222, "RISKID": "GLTTRE07", "JOBID": "1234",'
                    #                                    b' "JOBHDRID": "0123", "CCMRISKNAME": "REALIZER",'
                    #                                    b' "SYSTEMID": "ABC"}',
                    #                              headers=[], checksum=None, serialized_key_size=-1,
                    #                              serialized_value_size=117, serialized_header_size=-1)]]
                    #             )

                    message_values_list = [consumer_rec_list for consumer_rec_list in message.values()]

                    print(message_values_list)
                    # consumer_recs = [consumer_rec for consumer_rec in message_values_list]
                    # since our poll takes  in only one message at a time so we have one consumer message at one time
                    # to deal with - taking the value property which is a dictionary
                    message_dict = json.loads(message_values_list[0][0].value) # see abv eg its a double list

                    logger.info(f' method called to process {topic_id} with the message as {message_dict}')

                    control_processing.delegator(topic_id, appln_cntxt, message=message_dict)  # as apart from 1st other are keyword args

                if count_consec_empty >= consecutive_no_recs_to_signal_exit:
                    logger.info(f' The consecutive consumer polls, counted as {count_consec_empty}, '
                                f' for the consumer {topic_id} '
                                f' from the thread {threading.current_thread().name} have resulted in no messages been '
                                f' fetched , hence gracefully exiting to make way for other consumers'
                                )
                    break

                # Below code did not help to resolve for the case when the Kafka server down while consumer listening
                # for messages.
                # important thing to note here is even if the Kafka server goes down no exception is caught here
                # # so we need to check for the Kafka connection here as follows.
                # if consumer.bootstrap_connected():
                #
                #     print(f'Hiii this is the message from Kafka for consumer {topic_id} and this is {message} '
                #           f'from the thread {threading.current_thread().name}')
                # else:
                #     raise Exception(f'Kafka Server not reachable from the consumer {topic_id} and engine {engine_id}')

        except Exception as error:
            logger.error(error, exc_info=True)
            # consumer.close()

        # in case the consumer abruptly/cleanly  goes down -- this block should always be executed to make consumer
        # status back to DOWN
        finally:

            consumer.close()
            logger.debug(f'Hiii  I am closed -- this is the message from Kafka for consumer {topic_id} '
                         f'from the thread {threading.current_thread().name}')

            # update the status to DOWN for this engine's control_Id
            models.consumer_status_update_per_control_engine(topic_id, 'DOWN', appln_cntxt)
            # code shifted to a method
            # with appln_cntxt.app_context():
            #     cntrl_monitor_ngn_assoc = models.CCMControlEngineAssoc()
            #
            #     print('finally block called')
            #     kfk_control_id = topic_id
            #     db_row_4_status_upd = cntrl_monitor_ngn_assoc.query.filter_by(control_id=kfk_control_id
            #        , engine_id=appln_cntxt.config["ENGINE_ID"]).all() # here in finally the app_context was not alive
            #     for row in db_row_4_status_upd:
            #         logger.info(f' Kafka Consumer control Id being updated for status is  {row.control_id}')
            #         row.status = models.KafkaConsumerEnum.DOWN  # make it down
            #         print(f' row modified is {row}')
            #         db.session.commit()
