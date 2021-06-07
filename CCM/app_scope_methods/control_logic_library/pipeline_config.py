#pipeline_config.py
'''

Author: Ankur Saxena
Objective: This module stores the pipeline configuration for the controls.

'''

from collections import namedtuple
# PIPELINE is a dictionary keyed in by the Control_ID and the value as the LIST of stages needed to be executed for that
# control. Essentially the pipeline is the flowchart representation of the lifecycle for the control execution.
# Every list element is keyed in by Unique Stage Identifier; stage id has a named identifier as its value.
# currently supported stage types as processing, decision

Stage = namedtuple('Stage', ['name', 'description', 'stage_type', 'proceed_to'])
StageProcessor = namedtuple('StageProcessor', ['path_to_module', 'method_name'])
PIPELINE = {'TFA02_IFA19':
                    [
                        {"STAGE1": Stage(name="MARK_RECS_TO_PROCESS"
                                         , description="This stage marks the records in the function collection "
                                                       " those need to be processed"
                                         , stage_type='processing'
                                         , proceed_to='STAGE2'),
                         "STAGE_PROCESSOR": StageProcessor(path_to_module='CCM.app_scope_methods.control_logic_library'
                                                                          '.control_TFA02_IFA19'
                                                           , method_name='method_TFA02_IFA19')
                         },
                        {"STAGE2": Stage(name="PROCESS_AND_MERGE"
                                         , description="This stage processes the selected records and merges into the "
                                                       "final exception collection"
                                         , stage_type='processing'
                                         , proceed_to='STAGE3'),
                         "STAGE_PROCESSOR": StageProcessor(path_to_module='CCM.app_scope_methods.control_logic_library'
                                                                          '.control_TFA02_IFA19'
                                                           , method_name='')
                         },
                        {"STAGE3": Stage(name="MARK_PROCESSED_RECS"
                                         , description="This stage marks the records in the function collection "
                                                       "those have been processed"
                                         , stage_type='processing'
                                         , proceed_to='STAGE4'),
                         "STAGE_PROCESSOR": StageProcessor(path_to_module='CCM.app_scope_methods.control_logic_library'
                                                                          '.control_TFA02_IFA19'
                                                           , method_name='')
                         },
                        {"STAGE4": Stage(name="MARK_RECS_TO_PROCESS"
                                         , description="This stage marks the records in the function collection"
                                                       " those need to be processed"
                                         , stage_type='decision'
                                         , proceed_to={'yes': 'STAGE1', 'no': 'END'}),
                         "STAGE_PROCESSOR": StageProcessor(path_to_module='CCM.app_scope_methods.control_logic_library'
                                                                          '.control_TFA02_IFA19'
                                                           , method_name='')
                         },
                        {"END": Stage(name="END"
                                      , description="This the End stage of this pipeline. Any clean up or "
                                                    "leftover bookkeeping can be done in this stage. "
                                      , stage_type='processing'
                                      , proceed_to=''),
                         "STAGE_PROCESSOR": StageProcessor(path_to_module='CCM.app_scope_methods.control_logic_library'
                                                                          '.control_TFA02_IFA19'
                                                           , method_name='')
                         },

                    ]

            }