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


def delegator(control_id, **kwargs):

    ''' This method delegates to the specific method where in the processing block for specific control will be
    executed.
    This executes all the logic for a specific control and returns the final status to the caller (via Kafka_consumers or
    via threads ).
    '''

    # Based on the configuration above finding the method to be executed.

    # Here we will get the value of the key ie the sequence of code dicts to be executed for that control.

    code_units_list = control_logic_dict["control_id"]
    if len(code_units_list) == 0:
        logger.error(f'NO code units found to be executed for the control {control_id}.. Kindly check the configuration')

    else:
        for item in code_units_list:
            try:
                import importlib
                imprt_module = importlib.import_module(item["path_to_module"])
                func_to_be_called = item["method_name"]

                result_frm_func_called = getattr(imprt_module, func_to_be_called)(**kwargs)

            except Exception as error:
                logger.error(f' Error encountered {error}', exc_info=True)
                # make entries into the detail table for the execution trace for that.
                # Fail the job for the ID
                break

    return True # returns just boolean over the execution have been done irrespective of whether it was pass / fail.

