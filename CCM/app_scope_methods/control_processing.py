# control_processing.py
'''
Author: Ankur Saxena

Objective : This module consists of methods those will be providing
            the processing logic needed to be worked out on the specific controls.
'''
import logging

logger = logging.getLogger(__name__)

# Each dictionary item is keyed in by the control_id.
# Each control_id key has value as the list of the dictionaries.
# Each dictionary item in the value denote a path of code to be executed.
# Items in the List denote the sequence to be followed.
# each of these methods would be presented with keyword arguments.
# here if the path of the code is changed , then it also has to be changed here.
control_logic_dict = {'TFA02_IFA19':
                          [{'path_to_module': 'CCM.app_scope_methods.control_logic_library.control_TFA02_IFA19'
                            ,'method_name': 'method_TFA02_IFA19'},]
                      ,
                    }

def delegator(control_id, appln_cntxt, **kwargs):

    ''' This method delegates to the specific method where in the processing block for specific control will be
    executed.
    This executes all the logic for a specific control and returns the final status to the caller (via Kafka_consumers or
    via threads ).
    '''

    # Based on the configuration above finding the method to be executed.

    # Here we will get the value of the key ie the sequence of code dicts to be executed for that control.

    code_units_list = control_logic_dict.get(control_id, list())
    if len(code_units_list) == 0:
        logger.error(f'NO code units found to be executed for the control {control_id}.. Kindly check the configuration')

    else:
        for item in code_units_list:
            try:
                import importlib
                imprt_module = importlib.import_module(item["path_to_module"])
                func_to_be_called = item["method_name"]

                logger.info(f' Method {func_to_be_called} called to process the control {control_id}')
                result_frm_func_called = getattr(imprt_module, func_to_be_called)(appln_cntxt, **kwargs)

            except Exception as error:
                logger.error(f' Error encountered {error}', exc_info=True)
                # make entries into the detail table for the execution trace for that.
                # Fail the job for the ID
                break

    return True     # returns just boolean over the execution have been done irrespective of whether it was pass / fail.


class Stage:

    def __init__(self, name, description, stage_type):
        self.name = name
        self.description = description
        self.stage_type = stage_type


class ControlLifecycleFlowchart:
    ''' This class will run the entire pipeline logic on its own.
       Only thing needed is to initiate it and pass in the control parameters as dict.
       all the parameters that are needed by the methods in the pipeline can be
       gathered from the object level dictionary i.e. control_params_dict '''

    def __init__(self, control_id, control_params_dict):
        # to invoke the lifecycle flowchart pass in the control_id and the control_params_dict.
        self.control_id = control_id
        self.control_params_dict = control_params_dict

        # These attributes will be set by the class methods internally looking at the config.
        self.stage = 'initial'
        self.pipeline = 'pipeline'  # pipeline is a simple list of the stages

    def set_stage(self, stage):
        self.stage = stage

    def set_pipeline(self):
        ''' This method should be able to set the pipeline based on the configuration '''

        # self.pipeline = pipeline

    def get_pipeline(self):
        ''' Based on the control Id we should be able to get the pipeline. '''
        pass




