# kafka_consumers.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods related to the kafka_consumers; eg starting up and checking
                whether the consumers are up or not.
"""

import inspect
import logging
from kafka import KafkaConsumer
from commons import general_methods
import pandas as pd
import threading
import config

from CCM import models, app

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

            for items in consumers_to_be_awakened_series.iteritems():
                print(items[1])     # gather the second index of the tuple to get the control id.
                kfk_control_id = items[1]   # topic id and consumer group id is same as that of the control_id

                x = threading.Thread(target=making_consumer_up, args=(kfk_control_id, kfk_control_id, app,), daemon=True)
                try:
                    x.start()
                    logger.debug(f'started new thread by main thread {threading.current_thread().name} for {kfk_control_id}')

                except Exception as error:
                    logger.error(error, exc_info=True)

            # making_consumer_up('PAY05','PAY05',app)
            pass


def making_consumer_up(topic_id, group_id, appln_cntxt):
    ''' This method is used to bring up the consumer '''

    from kafka import KafkaConsumer
    is_consumer_set = 'N'
    try:
        consumer = KafkaConsumer(topic_id, bootstrap_servers=["localhost:9092"], auto_offset_reset=appln_cntxt.config["KAFKA_CONSUMER_AUTO_OFFSET_RESET"]
                                 , enable_auto_commit=appln_cntxt.config["KAFKA_CONSUMER_ENABLE_AUTO_COMMIT"], group_id=group_id)

        logger.debug(f'Hiii I am in the consumer for {topic_id} for the thread {threading.get_ident()} {threading.current_thread().name}')
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
            for message in consumer:
                print(f'Hiii this is the message from Kafka for consumer {topic_id} and this is {message} '
                      f'from the thread {threading.current_thread().name}')

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
            #                                                                   , engine_id=appln_cntxt.config["ENGINE_ID"]).all() # here in finally the app_context was not alive
            #     for row in db_row_4_status_upd:
            #         logger.info(f' Kafka Consumer control Id being updated for status is  {row.control_id}')
            #         row.status = models.KafkaConsumerEnum.DOWN  # make it down
            #         print(f' row modified is {row}')
            #
            #         db.session.commit()