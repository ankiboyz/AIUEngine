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
        # query_for_consumers = cntrl_monitor_ngn_assoc.query(cntrl_monitor_ngn_assoc.engine_id)

        # print(query_for_consumers, query_for_consumers[0].__dict__, inspect.getmembers(models.CCMControlEngineAssoc()))#  getattr(query_for_consumers,'control_id'),
        # print(models.CCMControlEngineAssoc.__dict__.keys())
        # print(dir(query_for_consumers), 'length', len(query_for_consumers))
        # print(models.CCMControlEngineAssoc.__dict__["__table__"])
        # print(models.CCMControlEngineAssoc.__dict__["__doc__"])
        # print(models.CCMControlEngineAssoc.__dict__["_sa_class_manager"])
        # print(models.CCMControlEngineAssoc.__dict__["__mapper__"])   # mapped class CCMControlEngineAssoc->glt_ccm_xtnd_cntrl_ngn_assoc

        # This below was helpful -->
        # print(models.CCMControlEngineAssoc.__dict__["__mapper__"].column_attrs.keys())  # gives the column names
        # ['Comparator', '__class__', '__clause_element__', '__delattr__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__
        # ', '__getattr__', '__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__',
        # '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__slots__',
        # '__str__', '__subclasshook__', '_all_strategies', '_cache_key_traversal', '_configure_finished', '_configure_started',
        # '_creation_order', '_default_path_loader_key', '_deferred_column_loader', '_fallback_getattr', '_gen_cache_key',
        # '_generate_cache_attrs', '_generate_cache_key', '_generate_cache_key_for_object', '_get_context_loader',
        # '_get_strategy', '_getcommitted', '_is_internal_proxy', '_is_polymorphic_discriminator', '_mapped_by_synonym',
        # '_memoized_attr__default_path_loader_key', '_memoized_attr__deferred_column_loader', '_memoized_attr__raise_column_loader',
        # '_memoized_attr__wildcard_token', '_memoized_attr_info', '_orig_columns', '_raise_column_loader', '_should_log_debug',
        # '_should_log_info', '_strategies', '_strategy_lookup', '_wildcard_token', 'active_history', 'cascade', 'cascade_iterator',
        # 'class_attribute', 'columns', 'comparator_factory', 'copy', 'create_row_processor', 'deferred', 'descriptor', 'do_init',
        # 'doc', 'expire_on_flush', 'expression', 'extension_type', 'group', 'info', 'inherit_cache', 'init', 'instrument',
        # 'instrument_class', 'is_aliased_class', 'is_attribute', 'is_bundle', 'is_clause_element', 'is_instance', 'is_mapper',
        # 'is_property', 'is_selectable', 'key', 'logger', 'merge', 'parent', 'post_instrument_class', 'raiseload', 'set_parent',
        # 'setup', 'strategy', 'strategy_for', 'strategy_key', 'strategy_wildcard_key']
        # print(dir(models.CCMControlEngineAssoc.__dict__["__mapper__"].column_attrs['engine_id']))
        # print(models.CCMControlEngineAssoc.__dict__["__mapper__"].column_attrs['engine_id'].__getattribute__)

        logger.debug(db.get_engine())  # this gets the engine
        flask_sqlalchemy_ngn = db.get_engine()
        # SQL_str = f'SELECT * FROM {models.CCMControlEngineAssoc.__dict__["__table__"]}'
        # print(SQL_str)
        #
        # df = pd.read_sql(SQL_str, flask_sqlalchemy_ngn)
        # print(df.to_string(index=False))
        # print(df.dtypes)

        # SQL_str1 = f'SELECT * FROM {models.CCMControlEngineAssoc.__dict__["__table__"]}'

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
            print('Hiii consumers to be awakened')
            print(df_consumers_to_be_awakened.iloc[:wrk_set_num_rows])

            making_consumer_up('PAY05','PAY05',app)
            pass


def making_consumer_up(topic_id, group_id, appln_cntxt):
    ''' This method is used to bring up the consumer '''

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(topic_id, bootstrap_servers=["localhost:9092"], auto_offset_reset=appln_cntxt.config["KAFKA_CONSUMER_AUTO_OFFSET_RESET"]
                             , enable_auto_commit=appln_cntxt.config["KAFKA_CONSUMER_ENABLE_AUTO_COMMIT"], group_id=group_id)

    print('Hiii I am in the consumer')
    for message in consumer:
        print('Hiii this is the message from Kafka',message)