#pipeline_config.py
'''

Author: Ankur Saxena
Objective: This module stores the pipeline configuration for the controls.

'''

from collections import namedtuple
# PIPELINE is a dictionary keyed in by the Control_ID and the value as the LIST of stages needed to be executed for that
# control. Essentially the pipeline is the flowchart representation of the lifecycle for the control execution.
# Every list element is keyed in by Unique Stage Identifier; stage id has a named identifier as its value.
# currently supported stage types as processing, decision.

# Every flow to have atleast one proceed_to as EXIT.
# EXIT is a special stageID to denote Exit, it is to be used in the proceed_to to denote if here then Exit.


Stage = namedtuple('Stage', ['name', 'description', 'stage_type', 'proceed_to'])
StageProcessor = namedtuple('StageProcessor', ['path_to_module', 'method_name'])
PIPELINE = {'TFA02_IFA19_1':
                    [
                        {"ID": "STAGE1"
                         , "STAGE": Stage(name="PREPARE_PLAYGROUND"
                                          , description="This stage UN-marks the records in the function collection "
                                                        " those might have remained stale as in-process due to some "
                                                        "errors. "
                                          , stage_type='processing'
                                          , proceed_to='STAGE2')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_1'
                                                             )
                                                            # , arguments={'args': (1, 2, 3), 'kwargs': {'d': 4, }})
                                                            # currently not handling arguments reserved for
                                                            # next dev cycle
                         },
                        {"ID": "STAGE2"
                         , "STAGE": Stage(name="BRING_TO_LIFE_THE_CONTROL_METADATA_IF_NOT_ALREADY"
                                          , description="This stage checks for the existence of Exception Collection "
                                                        "and needed indexes, as per the control_metadata; "
                                                        "creates if not available"
                                          , stage_type='processing'
                                          , proceed_to='STAGE3')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_2')
                         },
                        {"ID": "STAGE3"
                         , "STAGE": Stage(name="MARK_THE_ONES_TO_BE_PROCESSED"
                                          , description="This stage processes marks the records to be processed. "
                                          , stage_type='processing'
                                          , proceed_to='STAGE4')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_3')
                         },
                        {"ID": "STAGE4"
                         , "STAGE": Stage(name="PROCESS_AND_MERGE"
                                          , description="This stage processes and merges the records "
                                                        "in the exception collection."
                                          , stage_type='processing'
                                          , proceed_to='STAGE5')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_4')
                         },
                        {"ID": "STAGE5"
                         , "STAGE": Stage(name="ADDITIONAL_PROCESSING"
                                          , description="This stage does additional processing on the records in the"
                                                        "Exception collection eg updating the exceptionID for the index"
                                          , stage_type='processing'
                                          , proceed_to='STAGE6')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_5')
                         },
                        {"ID": "STAGE6"
                         , "STAGE": Stage(name="FLAG_POSTS_CONQUERED"
                                          , description="This stage marks the recs in function collection, those have "
                                                        "been processed."
                                          , stage_type='processing'
                                          , proceed_to='STAGE7')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_6')
                         },
                        {"ID": "STAGE7"
                         , "STAGE": Stage(name="WHETHER_ALL_DONE"
                                               , description="This stage checks whether all the records in "
                                                             "the function collection have been processed"
                                               , stage_type='decision'
                                               # Probably all being done fine so go to STAGE2 instead of STAGE1;
                                               # On 2nd thoughts make it go to STAGE1 only as False could have been
                                               # as a result of Failure in logic execution.
                                               , proceed_to={'yes_ID': 'STAGE8', 'no_ID': 'STAGE1'})
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_7')
                         },
                        {"ID": "STAGE8"
                         , "STAGE": Stage(name="SIGNALING_EXIT"
                                          , description="This stage of this pipeline is to signal the EXIT  "
                                          , stage_type='processing'
                                          , proceed_to='EXIT')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='')
                         },
                        # {"ID": "STAGE9"
                        #  , "STAGE": Stage(name="UPDATE_AGING"
                        #                   , description="This stage of this pipeline updates the Aging "
                        #                                 "column of the Exception Collection "
                        #                   , stage_type='processing'
                        #                   # A special stage to denote Exit!
                        #                   , proceed_to='EXIT')
                        #  , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                        #                                                     '.control_logic_library.control_TFA02_IFA19'
                        #                                      , method_name='')
                        #  }
                    ],
            'TFA02_IFA19_SC7_1':
                    [
                        {"ID": "STAGE1"
                         , "STAGE": Stage(name="PREPARE_PLAYGROUND"
                                          , description="This stage UN-marks the records in the function collection "
                                                        " those might have remained stale as in-process due to some "
                                                        "errors. "
                                          , stage_type='processing'
                                          , proceed_to='STAGE2')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='method_tfa02_ifa19_sc7_1'
                                                             )
                                                            # , arguments={'args': (1, 2, 3), 'kwargs': {'d': 4, }})
                                                            # currently not handling arguments reserved for
                                                            # next dev cycle
                         },
                        {"ID": "STAGE2"
                         , "STAGE": Stage(name="BRING_TO_LIFE_THE_CONTROL_METADATA_IF_NOT_ALREADY"
                                          , description="This stage checks for the existence of Exception Collection "
                                                        "and needed indexes, as per the control_metadata; "
                                                        "creates if not available"
                                          , stage_type='processing'
                                          , proceed_to='STAGE3')
                         # Here, the generic method to create the indexes and exception collection is referred.
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.benevolent_methods'
                                                             , method_name='make_collection_with_indexes_available')
                         },
                        {"ID": "STAGE3"
                         , "STAGE": Stage(name="MARK_THE_ONES_TO_BE_PROCESSED"
                                          , description="This stage processes marks the records to be processed. "
                                          , stage_type='processing'
                                          , proceed_to='STAGE4')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='method_tfa02_ifa19_sc7_3')
                         },
                        {"ID": "STAGE4"
                         , "STAGE": Stage(name="PROCESS_AND_MERGE"
                                          , description="This stage processes and merges the records "
                                                        "in the exception collection."
                                          , stage_type='processing'
                                          , proceed_to='STAGE5')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='method_tfa02_ifa19_sc7_4')
                         },
                        {"ID": "STAGE5"
                         , "STAGE": Stage(name="ADDITIONAL_PROCESSING"
                                          , description="This stage does additional processing on the records in the"
                                                        "Exception collection eg updating the exceptionID for the index"
                                          , stage_type='processing'
                                          , proceed_to='STAGE6')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='method_tfa02_ifa19_sc7_5')
                         },
                        {"ID": "STAGE6"
                         , "STAGE": Stage(name="FLAG_POSTS_CONQUERED"
                                          , description="This stage marks the recs in function collection, those have "
                                                        "been processed."
                                          , stage_type='processing'
                                          , proceed_to='STAGE7')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='method_tfa02_ifa19_sc7_6')
                         },
                        {"ID": "STAGE7"
                         , "STAGE": Stage(name="WHETHER_ALL_DONE"
                                               , description="This stage checks whether all the records in "
                                                             "the function collection have been processed"
                                               , stage_type='decision'
                                               # Probably all being done fine so go to STAGE2 instead of STAGE1;
                                               # On 2nd thoughts make it go to STAGE1 only as False could have been
                                               # as a result of Failure in logic execution.
                                               , proceed_to={'yes_ID': 'STAGE8', 'no_ID': 'STAGE1'})
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='method_tfa02_ifa19_sc7_7')
                         },
                        {"ID": "STAGE8"
                         , "STAGE": Stage(name="SIGNALING_EXIT"
                                          , description="This stage of this pipeline is to signal the EXIT"
                                          , stage_type='processing'
                                          , proceed_to='EXIT')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='')
                         },
                        # {"ID": "STAGE9"
                        #  , "STAGE": Stage(name="UPDATE_AGING"
                        #                   , description="This stage of this pipeline updates the Aging "
                        #                                 "column of the Exception Collection "
                        #                   , stage_type='processing'
                        #                   # A special stage to denote Exit!
                        #                   , proceed_to='EXIT')
                        #  , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                        #                                                     '.control_logic_library.control_TFA02_IFA19_SC7'
                        #                                      , method_name='')
                        #  }
                    ],
            'TRE07_1':
                    [
                        {"ID": "STAGE1"
                         , "STAGE": Stage(name="PREPARE_PLAYGROUND"
                                          , description="This stage UN-marks the records in the function collection "
                                                        " those might have remained stale as in-process due to some "
                                                        "errors. "
                                          , stage_type='processing'
                                          , proceed_to='STAGE2')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.benevolent_methods'
                                                             , method_name='prepare_playground_clear_vestige'
                                                             )
                                                            # , arguments={'args': (1, 2, 3), 'kwargs': {'d': 4, }})
                                                            # currently not handling arguments reserved for
                                                            # next dev cycle
                         },
                        {"ID": "STAGE2"
                         , "STAGE": Stage(name="BRING_TO_LIFE_THE_CONTROL_METADATA_IF_NOT_ALREADY"
                                          , description="This stage checks for the existence of Exception Collection "
                                                        "and needed indexes, as per the control_metadata; "
                                                        "creates if not available"
                                          , stage_type='processing'
                                          , proceed_to='STAGE3')
                         # Here, the generic method to create the indexes and exception collection is referred.
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.benevolent_methods'
                                                             , method_name='make_collection_with_indexes_available')
                         },
                        {"ID": "STAGE3"
                         , "STAGE": Stage(name="MARK_THE_ONES_TO_BE_PROCESSED"
                                          , description="This stage processes marks the records to be processed. "
                                          , stage_type='processing'
                                          , proceed_to='STAGE4')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.benevolent_methods'
                                                             , method_name='mark_the_ones_to_be_handled')
                         },
                        {"ID": "STAGE4"
                         , "STAGE": Stage(name="PROCESS_AND_MERGE"
                                          , description="This stage processes and merges the records "
                                                        "in the exception collection."
                                          , stage_type='processing'
                                          , proceed_to='STAGE5')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TRE07'
                                                             , method_name='method_tre07_4')
                         },
                        {"ID": "STAGE5"
                         , "STAGE": Stage(name="ADDITIONAL_PROCESSING"
                                          , description="This stage does additional processing on the records in the"
                                                        "Exception collection eg updating the exceptionID for the index"
                                          , stage_type='processing'
                                          , proceed_to='STAGE6')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.benevolent_methods'
                                                             , method_name='filling_up_exceptionId_in_exception_collection')
                         },
                        {"ID": "STAGE6"
                         , "STAGE": Stage(name="FLAG_POSTS_CONQUERED"
                                          , description="This stage marks the recs in function collection, those have "
                                                        "been processed."
                                          , stage_type='processing'
                                          , proceed_to='STAGE7')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.benevolent_methods'
                                                             , method_name='flag_recs_in_function_collection_as_processed')
                         },
                        {"ID": "STAGE7"
                         , "STAGE": Stage(name="WHETHER_ALL_DONE"
                                               , description="This stage checks whether all the records in "
                                                             "the function collection have been processed"
                                               , stage_type='decision'
                                               # Probably all being done fine so go to STAGE2 instead of STAGE1;
                                               # On 2nd thoughts make it go to STAGE1 only as False could have been
                                               # as a result of Failure in logic execution.
                                               , proceed_to={'yes_ID': 'STAGE8', 'no_ID': 'STAGE1'})
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.benevolent_methods'
                                                             , method_name='whether_all_function_recs_done_decisional_node')
                         },
                        {"ID": "STAGE8"
                         , "STAGE": Stage(name="SIGNALING_EXIT"
                                          , description="This stage of this pipeline is to signal the EXIT"
                                          , stage_type='processing'
                                          , proceed_to='EXIT')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19_SC7'
                                                             , method_name='')
                         },
                        # {"ID": "STAGE9"
                        #  , "STAGE": Stage(name="UPDATE_AGING"
                        #                   , description="This stage of this pipeline updates the Aging "
                        #                                 "column of the Exception Collection "
                        #                   , stage_type='processing'
                        #                   # A special stage to denote Exit!
                        #                   , proceed_to='EXIT')
                        #  , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                        #                                                     '.control_logic_library.control_TFA02_IFA19_SC7'
                        #                                      , method_name='')
                        #  }
                    ],
            'FIN08_FA_1':
                    [
                        {"ID": "STAGE1"
                            , "STAGE": Stage(name="PREPARE_PLAYGROUND"
                                             , description="This stage UN-marks the records in the function collection "
                                                           " those might have remained stale as in-process due to some "
                                                           "errors. "
                                             , stage_type='processing'
                                             , proceed_to='STAGE2')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='prepare_playground_clear_vestige'
                                                                )
                         # , arguments={'args': (1, 2, 3), 'kwargs': {'d': 4, }})
                         # currently not handling arguments reserved for
                         # next dev cycle
                         },
                        {"ID": "STAGE2"
                            , "STAGE": Stage(name="BRING_TO_LIFE_THE_CONTROL_METADATA_IF_NOT_ALREADY"
                                             , description="This stage checks for the existence of Exception Collection "
                                                           "and needed indexes, as per the control_metadata; "
                                                           "creates if not available"
                                             , stage_type='processing'
                                             , proceed_to='STAGE3')
                         # Here, the generic method to create the indexes and exception collection is referred.
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='make_collection_with_indexes_available')
                         },
                        {"ID": "STAGE3"
                            , "STAGE": Stage(name="MARK_THE_ONES_TO_BE_PROCESSED"
                                             , description="This stage processes marks the records to be processed. "
                                             , stage_type='processing'
                                             , proceed_to='STAGE4')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='mark_the_ones_to_be_handled')
                         },
                        {"ID": "STAGE4"
                            , "STAGE": Stage(name="PROCESS_AND_MERGE"
                                             , description="This stage processes and merges the records "
                                                           "in the exception collection."
                                             , stage_type='processing'
                                             , proceed_to='STAGE5')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.control_FIN08_FA'
                                                                , method_name='method_fin08_fa_4')
                         },
                        {"ID": "STAGE5"
                            , "STAGE": Stage(name="ADDITIONAL_PROCESSING"
                                             , description="This stage does additional processing on the records in the"
                                                           "Exception collection eg updating the exceptionID for the index"
                                             , stage_type='processing'
                                             , proceed_to='STAGE6')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='filling_up_exceptionId_in_exception_collection')
                         },
                        {"ID": "STAGE6"
                            , "STAGE": Stage(name="FLAG_POSTS_CONQUERED"
                                             , description="This stage marks the recs in function collection, those have "
                                                           "been processed."
                                             , stage_type='processing'
                                             , proceed_to='STAGE7')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='flag_recs_in_function_collection_as_processed')
                         },
                        {"ID": "STAGE7"
                            , "STAGE": Stage(name="WHETHER_ALL_DONE"
                                             , description="This stage checks whether all the records in "
                                                           "the function collection have been processed"
                                             , stage_type='decision'
                                             # Probably all being done fine so go to STAGE2 instead of STAGE1;
                                             # On 2nd thoughts make it go to STAGE1 only as False could have been
                                             # as a result of Failure in logic execution.
                                             , proceed_to={'yes_ID': 'STAGE8', 'no_ID': 'STAGE1'})
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='whether_all_function_recs_done_decisional_node')
                         },
                        {"ID": "STAGE8"
                            , "STAGE": Stage(name="SIGNALING_EXIT"
                                             , description="This stage of this pipeline is to signal the EXIT"
                                             , stage_type='processing'
                                             , proceed_to='EXIT')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.control_FIN08_FA'
                                                                , method_name='')
                         },
                    ],
            'FIN08_AP_AR_1':
                    [
                        {"ID": "STAGE1"
                            , "STAGE": Stage(name="PREPARE_PLAYGROUND"
                                             , description="This stage UN-marks the records in the function collection "
                                                           " those might have remained stale as in-process due to some "
                                                           "errors. "
                                             , stage_type='processing'
                                             , proceed_to='STAGE2')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.control_FIN08_AP_AR'
                                                                , method_name='method_fin08_ap_ar_1'
                                                                )
                         # , arguments={'args': (1, 2, 3), 'kwargs': {'d': 4, }})
                         # currently not handling arguments reserved for
                         # next dev cycle
                         },
                        {"ID": "STAGE2"
                            , "STAGE": Stage(name="BRING_TO_LIFE_THE_CONTROL_METADATA_IF_NOT_ALREADY"
                                             , description="This stage checks for the existence of Exception Collection "
                                                           "and needed indexes, as per the control_metadata; "
                                                           "creates if not available"
                                             , stage_type='processing'
                                             , proceed_to='STAGE3')
                         # Here, the generic method to create the indexes and exception collection is referred.
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='make_collection_with_indexes_available')
                         },
                        {"ID": "STAGE3"
                            , "STAGE": Stage(name="MARK_THE_ONES_TO_BE_PROCESSED"
                                             , description="This stage processes marks the records to be processed. "
                                             , stage_type='processing'
                                             , proceed_to='STAGE4')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.control_FIN08_AP_AR'
                                                                , method_name='method_fin08_ap_ar_3')
                         },
                        {"ID": "STAGE4"
                            , "STAGE": Stage(name="PROCESS_AND_MERGE"
                                             , description="This stage processes and merges the records "
                                                           "in the exception collection."
                                             , stage_type='processing'
                                             , proceed_to='STAGE5')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.control_FIN08_AP_AR'
                                                                , method_name='method_fin08_ap_ar_4')
                         },
                        {"ID": "STAGE5"
                            , "STAGE": Stage(name="ADDITIONAL_PROCESSING"
                                             , description="This stage does additional processing on the records in the"
                                                           "Exception collection eg updating the exceptionID for the index"
                                             , stage_type='processing'
                                             , proceed_to='STAGE6')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='filling_up_exceptionId_in_exception_collection')
                         },
                        {"ID": "STAGE6"
                            , "STAGE": Stage(name="FLAG_POSTS_CONQUERED"
                                             , description="This stage marks the recs in function collection, those have "
                                                           "been processed."
                                             , stage_type='processing'
                                             , proceed_to='STAGE7')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='flag_recs_in_function_collection_as_processed')
                         },
                        {"ID": "STAGE7"
                            , "STAGE": Stage(name="WHETHER_ALL_DONE"
                                             , description="This stage checks whether all the records in "
                                                           "the function collection have been processed"
                                             , stage_type='decision'
                                             # Probably all being done fine so go to STAGE2 instead of STAGE1;
                                             # On 2nd thoughts make it go to STAGE1 only as False could have been
                                             # as a result of Failure in logic execution.
                                             , proceed_to={'yes_ID': 'STAGE8', 'no_ID': 'STAGE1'})
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.benevolent_methods'
                                                                , method_name='whether_all_function_recs_done_decisional_node')
                         },
                        {"ID": "STAGE8"
                            , "STAGE": Stage(name="SIGNALING_EXIT"
                                             , description="This stage of this pipeline is to signal the EXIT"
                                             , stage_type='processing'
                                             , proceed_to='EXIT')
                            , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                               '.control_logic_library.control_FIN08_FA'
                                                                , method_name='')
                         },
                    ]
            }   # closure of overall Pipeline dictionary
