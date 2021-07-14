# thread_based_job_execution.py
"""
    Author: Ankur Saxena

    Objective: This module will provide various methods related to the thread based job execution;
               Each thread will execute one job ID and exit.
               Whenever the method is invoked it will do the following:
               a. Check whether the in - process (PROCESSING) jobs are less than the maximum no. of threads those can be
               invoked at one time;ie there are jobs in SUBMITTED state take from them the control IDs - per control id
               execution has to be sequential.

               b. If those are less than number of threads then the difference no. of jobs to be triggered.
               c. find out out as per algo which jobs can be put up.
               d. start out those jobs.
               e. Execution to happen in sequence for a control id.
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
import time

# Here we get both the app and a db that has been initialized with app (i.e db.init method has been called onto it).
# And on these we can execute the DB transactions .. and db.session from flask-sqlachemy seems to
# return the same SqlAlchemy Session object that can be workd upon.
from BCM import models, app

from flask import current_app

logger = logging.getLogger(__name__)
db = models.db


def thread_job_algo():
    ''' This algo will uncover which jobs to be picked up for execution and spwan threads for their execution.
    The execution can be parallel control wise but not within a control; within a control it need be sequential.
    So, we make it simple spawn threads control wise and then loop in for every control to execute job ID for that
    control.

    So, it could also be modelled similar to the consumer way; so utilizing the similar pieces here.
    '''

    with app.app_context():

        cntrl_monitor_ngn_assoc = models.BCMControlEngineAssoc()
        query_4_consumers_up = cntrl_monitor_ngn_assoc.query.filter_by(status=models.KafkaConsumerEnum.UP
                                                                       , engine_id=app.config["ENGINE_ID"]).all()
        threads_that_can_be_awakened = app.config["MAX_NUM_OF_THREADS_AT_ONE_TIME"] - len(query_4_consumers_up)

        logger.info(f'Threads those can be Awakened for this Engine_ID {app.config["ENGINE_ID"]} are '
                     f'{threads_that_can_be_awakened}')

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
        if threads_that_can_be_awakened > 0:
            # if the consumers to be awakened are n , then awake the top n consumers.
            # For each consumer there will be a thread invoked with a specific function for that consumer.

            # finding out the number of rows of df to be worked upon i.e. whichever is minimum
            # , the num of rows in df for consumers to be awakened or the consumers that can be awakened (which is
            # based on the settings of the max num of consumers those can be awakened.
            # we will take then these top n rows.

            wrk_set_num_rows = min(len(df_consumers_to_be_awakened.index), threads_that_can_be_awakened)
            logger.debug(f'threads to be awakened {df_consumers_to_be_awakened.iloc[:wrk_set_num_rows].to_string(index=False)}')

            # Here we get a series object of the consumers to be awakened:
            consumers_to_be_awakened_series = df_consumers_to_be_awakened.iloc[:wrk_set_num_rows]["control_id"]
            logger.debug(f'consumers to be awakened series {consumers_to_be_awakened_series}')

            threads_spawned_list = list()

            for items in consumers_to_be_awakened_series.iteritems():
                print(items[1])     # gather the second index of the tuple to get the control id.
                kfk_control_id = items[1]   # topic id and consumer group id is same as that of the control_id

                # Invoking a thread per consumer given the upper limit of the threads those can be spawned.
                x = threading.Thread(target=execution_within_threads, args=(kfk_control_id, app,), daemon=True)
                try:
                    x.start()
                    logger.debug(f'started new thread by main thread {threading.current_thread().name} for {kfk_control_id}')
                    threads_spawned_list.append(x)

                except Exception as error:
                    logger.error(error, exc_info=True)

            # making_consumer_up('REALIZER','REALIZER',app)
            logger.info(f'List of threads for the consumers spawned is {threads_spawned_list}')
            pass


def execution_within_threads(control_id, appln_cntxt):
    ''' since the thread is up , here we need to now sequentially execute the Header Ids in the job. '''

    response = False
    engine_id = appln_cntxt.config["ENGINE_ID"]
    with appln_cntxt.app_context():

        try:
            # make the status as UP
            models.consumer_status_update_per_control_engine(control_id, 'UP', appln_cntxt)

        except Exception as error:
            logger.error(error, exc_info=True)
            # consumer.close()

        try:
            # for message in consumer:
            poll_interval_ms = appln_cntxt.config["INTRA_THREAD_POLL_FREQUENCY_MS"]
            poll_interval_in_secs = poll_interval_ms/1000   # divide by 1000 to get seconds.
            consecutive_no_recs_to_signal_exit = appln_cntxt.config["INTRA_THREAD_CONSECUTIVE_NO_RECS_TO_SIGNAL_EXIT"]
            count_consec_empty = 0

            while True:

                #  Here, we select the Header Id for this control id thread.
                #  Also, verify if the ID is PROCESSING then skip else execute
                #  upon skipping sleep for configured time (INTRA_THREAD_POLL_FREQUENCY_SECS)
                #  and re-check , if consecutively skipped for n (consecutive_no_recs_to_signal_exit)
                #  times then gracefully exit.
                cntrl_monitor_hdr = models.BCMMonitorHDR()
                db_job_header_row = cntrl_monitor_hdr.query.filter_by(status=models.StatusEnum.PROCESSING
                                                                               , control_id=control_id).first()

                if db_job_header_row is not None:
                    db_job_header_row_id = db_job_header_row.id
                else:
                    db_job_header_row_id = 0

                # that is some processing is going on, so no additional processing should be done.

                if db_job_header_row_id != 0:
                    count_consec_empty = count_consec_empty + 1

                else:
                    # so that its a fresh call and not cached
                    db_job_header_row = cntrl_monitor_hdr.query.filter_by(status=models.StatusEnum.SUBMITTED
                                                                          , control_id=control_id).order_by(cntrl_monitor_hdr.start_date)\
                        .first()

                    print('db_job_header_row', db_job_header_row)

                    if db_job_header_row is not None:
                        db_job_header_row_id = db_job_header_row.id
                    else:
                        db_job_header_row_id = 0

                    if db_job_header_row_id == 0:   # if there is no job that is SUBMITTED
                        count_consec_empty = count_consec_empty + 1
                    else:
                        # Do processing here.
                        count_consec_empty = 0

                        logger.info(f'Hiii this is the execution to be done for the control_id {control_id} : '
                                    f'and the job header id is {db_job_header_row_id} '
                                    f'from the thread {threading.current_thread().name}')

                        # Need to process the message here -- call a method and that method will invoke the processing of
                        # the job ID : message will only have the ID and the Control_ID : will interface with the
                        # detail table and also update status of the header table.

                        # Here, create a dictionary for the ID and CONTROL_ID as follows.
                        message_dict = {'ID': db_job_header_row_id, 'CONTROL_ID': control_id}

                        logger.debug(f'The NON-Empty message after unwrapping out of gathered by Consumer is {message_dict}')

                        logger.info(f'Delegator called to process {control_id} with the message as {message_dict}')

                        response = control_processing.delegator(control_id, dict_input=message_dict)  # as apart from 1st other are keyword args

                        print('response from delegator', response)
                # Putting up a sleep counter here.
                logger.debug(f'Thread sleeping for {poll_interval_in_secs} seconds for the control {control_id}')
                time.sleep(poll_interval_in_secs)

                if count_consec_empty >= consecutive_no_recs_to_signal_exit:
                    logger.info(f' The consecutive THREAD\'s polls, counted as {count_consec_empty}, '
                                f' for the consumer {control_id} '
                                f' from the thread {threading.current_thread().name} have resulted in no messages been '
                                f' fetched , hence gracefully exiting to make way for other consumers'
                                )
                    break

        except Exception as error:
                logger.error(error, exc_info=True)

            # in case the consumer abruptly/cleanly  goes down -- this block should always be executed to make consumer
            # status back to DOWN (for the daemon based thread ,
            # the finally block would not be called when abrupt stoppage happens)
        finally:

            # consumer.close()
            logger.debug(f'Hiii  I am closed -- this is the message from Thread for control {control_id} '
                         f'from the thread {threading.current_thread().name}')

            # update the status to DOWN for this engine's control_Id
            models.consumer_status_update_per_control_engine(control_id, 'DOWN', appln_cntxt)
            print(f'finally block called for THREAD\'s graceful exit {control_id}')
            logger.info(f'Finally block called for THREAD\'s graceful exit {control_id}')