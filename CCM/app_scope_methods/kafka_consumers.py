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


from CCM import models, app

logger = logging.getLogger(__name__)
db = models.db


def kafka_consumer_algo():
    ''' This algo will uncover the number of consumers need to be spawned '''

    with app.app_context():
        cntrl_monitor_hdr = models.CCMMonitorHDR()
        query = cntrl_monitor_hdr.query.filter_by(status=models.StatusEnum.SUBMITTED).all()
        logger.debug(f'There are these many jobs to be handled {len(query)}')

        cntrl_monitor_ngn_assoc = models.CCMControlEngineAssoc()
        query_for_consumers = cntrl_monitor_ngn_assoc.query.all()

        print(query_for_consumers, query_for_consumers[0].__dict__, inspect.getmembers(models.CCMControlEngineAssoc()))#  getattr(query_for_consumers,'control_id'),
        print(models.CCMControlEngineAssoc.__dict__.keys())
        print(dir(query_for_consumers))
        print(models.CCMControlEngineAssoc.__dict__["__table__"])
        print(models.CCMControlEngineAssoc.__dict__["__doc__"])
        print(models.CCMControlEngineAssoc.__dict__["_sa_class_manager"])
        print(models.CCMControlEngineAssoc.__dict__["__mapper__"])   # mapped class CCMControlEngineAssoc->glt_ccm_xtnd_cntrl_ngn_assoc

        print(models.CCMControlEngineAssoc.__dict__["__mapper__"].column_attrs.keys())  # gives the column names
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
        print(dir(models.CCMControlEngineAssoc.__dict__["__mapper__"].column_attrs['engine_id']))
        print(models.CCMControlEngineAssoc.__dict__["__mapper__"].column_attrs['engine_id'].__getattribute__)

        print(db.get_engine())  # this gets the engine
        general_methods.df_for_models_table(models.CCMControlEngineAssoc, query_for_consumers)
        # we will take the number of configured kafka consumers to be run at one time.
        # We will sort the jobs those need be handled in the descending order of duration from submission.
        # Start making Consumers up those are needed to cater to the job as per priority gathered above.
        # Stop once the maximum no. of consumers to be up at one time is reached.
        # The consumer who has completed its job  and has not received any new message , need to be come down by itself.


