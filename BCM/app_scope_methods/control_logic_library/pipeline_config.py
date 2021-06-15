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
                                                             # , arguments={'args': (1, 2, 3), 'kwargs': {'d': 4, }}) #currently not handling arguments reserved for next dev cycle
                         },
                        {"ID": "STAGE2"
                         , "STAGE": Stage(name="MARK_RECS_TO_PROCESS"
                                          , description="This stage marks the records in the function collection "
                                                        " those need to be processed"
                                          , stage_type='processing'
                                          , proceed_to='STAGE3')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='method_tfa02_ifa19_1')
                         },
                        {"ID": "STAGE3"
                         , "STAGE": Stage(name="PROCESS_AND_MERGE"
                                          , description="This stage processes the selected records and merges into the "
                                                        "final exception collection"
                                          , stage_type='processing'
                                          , proceed_to='STAGE4')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='')
                         },
                        {"ID": "STAGE4"
                         , "STAGE": Stage(name="MARK_PROCESSED_RECS"
                                          , description="This stage marks the records in the function collection "
                                                        "those have been processed"
                                          , stage_type='processing'
                                          , proceed_to='STAGE5')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                           , method_name='')
                         },
                        {"ID": "STAGE5"
                         , "STAGE": Stage(name="WHETHER_ALL_DONE"
                                               , description="This stage checks whether all the records in "
                                                             "the function collection have been processed"
                                               , stage_type='decision'
                                               # Probably all being done fine so go to STAGE2 instead of STAGE1;
                                               # On 2nd thoughts make it go to STAGE1 only as False could have been
                                               # as a result of Failure in logic execution.
                                               , proceed_to={'yes_ID': 'STAGE6', 'no_ID': 'STAGE1'})
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='')
                         },
                        {"ID": "STAGE6"
                         , "STAGE": Stage(name="UPDATE_AUTO_CLOSE"
                                          , description="This stage of this pipeline updates the auto close flags in "
                                                        "the control exception collection "
                                          , stage_type='processing'
                                          , proceed_to='STAGE7')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='')
                         },
                        {"ID": "STAGE7"
                         , "STAGE": Stage(name="UPDATE_AGING"
                                          , description="This stage of this pipeline updates the Aging "
                                                        "column of the Exception Collection "
                                          , stage_type='processing'
                                          # A special stage to denote Exit!
                                          , proceed_to='EXIT')
                         , "STAGE_PROCESSOR": StageProcessor(path_to_module='BCM.app_scope_methods'
                                                                            '.control_logic_library.control_TFA02_IFA19'
                                                             , method_name='')
                         }
                    ]

            }