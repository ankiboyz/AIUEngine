# control_processing.py
'''
Author: Ankur Saxena

Objective : This module consists of methods those will be providing
            the processing logic needed to be worked out on the specific controls.
'''
import logging
import CCM.app_scope_methods.control_logic_library.pipeline_config as pipeline_config

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

    # For testing purposes - START
    # flwchart = ControlLifecycleFlowchart(control_id, appln_cntxt, {'RUN_ID': 111, 'FUNCTION_ID': "TFA02_IFA19"
    #     , 'EXCEPTION_COLLECTION_NAME': "EXCEPTION_TFA02_IFA19"
    #     , 'CONTROL_ID': "TFA02_IFA19"})

    flwchart = ControlLifecycleFlowchart(control_id, appln_cntxt, kwargs)

    flwchart.execute_pipeline()
    # a = {'RUN_ID': 1111, 'CONTROL_ID': "TFA02_IFA19", 'EXCEPTION_COLLECTION_NAME': "EXCEPTION_TFA02_IFA19"
    #     , 'FUNCTION_ID': "TFA02_IFA19"}
    # For testing purposes - END

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


# class Stage:
#
#     def __init__(self, name, description, stage_type):
#         self.name = name
#         self.description = description
#         self.stage_type = stage_type


class ControlLifecycleFlowchart:
    ''' This class will run the entire pipeline logic on its own.
       Only thing needed is to initiate it and pass in the control parameters as dict.
       all the parameters that are needed by the methods in the pipeline can be
       gathered from the object level dictionary i.e. control_params_dict.
        1. First, instantiate the object of the ControlLifeCycleFlowChart class,
           passing i/ps as control_id, dictionary of input parameters.
        2. Second, set the pipeline for the control ID calling in the set_pipeline method.
           execute_pipeline method does it now.
        3. Third, call the execute_pipeline method.
        '''

    def __init__(self, control_id, appln, control_params_dict):
        # to invoke the lifecycle flowchart pass in the control_id and the control_params_dict and the app (to use the
        # app context for Flask-SQLAlchemy etc.).
        self.control_id = control_id
        self.control_params_dict = control_params_dict
        self.appln = appln

        # These attributes will be set by the class methods internally looking at the config.
        # Here, just assigning some initial values
        self.current_stage_ID = 'START'

        # here the pipeline_config.Stage becomes the object with () and keyword-ed ip params
        self.current_stage = pipeline_config.Stage(name='Initial', description='Let''s Start', stage_type='START'
                                                   , proceed_to='')
        self.current_stage_processor = pipeline_config.StageProcessor(path_to_module='', method_name='')
        self.pipeline = []  # pipeline is a simple list of the stages
        self.flag_exit = False  # flag to exit

    def set_current_stage(self, stage_id, stage, stage_processor):
        self.current_stage_ID = stage_id
        self.current_stage = stage
        self.current_stage_processor = stage_processor

    def set_pipeline(self):
        ''' This method should be able to set the pipeline based on the configuration.
            This will also set the current stage as initial one.

            Returns True or False; if returned True then only the caller should begin further processing.
         '''

        pipeline_list_of_stages = pipeline_config.PIPELINE.get(self.control_id, 'NOTFOUND')

        if pipeline_list_of_stages == 'NOTFOUND':
            logger.error(f' Pipeline for the control {self.control_id} is not configured. ')
            return False
        else:
            self.pipeline = pipeline_list_of_stages
            logger.info(f' Pipeline for the control {self.control_id} is set up. ')

            # sets the first stage of the pipeline
            self.set_current_stage(pipeline_list_of_stages[0]["ID"], pipeline_list_of_stages[0]["STAGE"]
                                   , pipeline_list_of_stages[0]["STAGE_PROCESSOR"])
            return True

    def get_pipeline(self):
        ''' Based on the control Id we should be able to get the pipeline. '''
        return self.pipeline

    def goto_next_stage(self, bool_op_current_executed_stage):
        '''
        Also, gets the boolean output of the previous stage i.e. True / False.
        This method will yield the next stage to go to.
        Sets the current stage as the next stage;
        so once this method is called the next stage is set as the current stage.
        Also sets the flag_exit if it is the end of the pipeline or the current just executed stage
        signalled false (ie some error might have come).
        '''
        current_stage_type = self.current_stage.stage_type.strip()
        # proceed_to can be a dict in case of decision node else it will stage ID
        current_stage_proceed_to = self.current_stage.proceed_to

        # if the proceed_to is specified as EXIT or left as blank or previous stage encountered some error
        # hence o/p False.
        # If other than decision stage resulted in False (ie in the stage execution there would have been some issue)
        if current_stage_type == 'decision':
            # In case of a decision node value is: proceed_to={'yes_ID': 'END', 'no_ID': 'STAGE2'}
            # There would be a need for an error placeholder in decision node as well.
            fork_dict = {(True if k in ['yes_ID'] else False): v for k,v in current_stage_proceed_to.items()}
            # above will yield dict : {True: 'END', False: 'STAGE2'}
            # Now, based on output the stage need be determined:
            next_stage_id = fork_dict[bool_op_current_executed_stage]
        else:
            # if not decision node
            next_stage_id = self.current_stage.proceed_to

        if (next_stage_id.strip() == '') or (next_stage_id.strip() == 'EXIT') \
                or ((current_stage_type != 'decision') and (bool_op_current_executed_stage is False)):   # current stage was not a decision node and it returned op as False
            logger.info(f' Setting the exit flag for the pipeline for {self.control_id} with input params as '
                        f'{self.control_params_dict}')
            self.flag_exit = True

        else:
            pipeline_list_of_stages = self.get_pipeline()
            list_matching_next_stage_id = [item_dict for item_dict in pipeline_list_of_stages
                                           if item_dict["ID"] == next_stage_id]

            length_of_list_matching_next_stage_id = len(list_matching_next_stage_id)

            if length_of_list_matching_next_stage_id != 1:
                logger.error(f' There are more than one stage with the same ID {next_stage_id} '
                             f'in the pipeline for control {self.control_id} with the input params '
                             f'as {self.control_params_dict}')

                # exit the pipeline
                self.flag_exit = True

            else:
                # Since there should be only one matching item in the list
                self.set_current_stage(list_matching_next_stage_id[0]["ID"], list_matching_next_stage_id[0]["STAGE"]
                                       , list_matching_next_stage_id[0]["STAGE_PROCESSOR"])

    def execute_current_stage_processor(self):
        ''' This will execute the logic for the current stage.
            Method also ensures the output it receives from invocation of a method is boolean.
        '''

        bool_op = False

        current_stage_processor_path_to_module = self.current_stage_processor.path_to_module.strip()
        current_stage_processor_method_name = self.current_stage_processor.method_name.strip()

        len_current_stage_processor_path_to_module = len(current_stage_processor_path_to_module)
        len_current_stage_processor_method_name = len(current_stage_processor_method_name)

        # initialize
        way_to_go = 'nothing_to_execute'

        if (len_current_stage_processor_path_to_module == 0) and (len_current_stage_processor_method_name == 0):
            # If neither module nor method is mentioned then return True as nothing was supposed to be called.
            way_to_go = 'nothing_to_execute'
        if (len_current_stage_processor_path_to_module != 0) and (len_current_stage_processor_method_name == 0):
            way_to_go = 'only_module_import'
        if (len_current_stage_processor_path_to_module == 0) and (len_current_stage_processor_method_name != 0):
            way_to_go = 'only_method_execute'
        if (len_current_stage_processor_path_to_module != 0) and (len_current_stage_processor_method_name != 0):
            way_to_go = 'both_import_and_execute'

        # Execution block based on above inference
        continue_eval = True

        if continue_eval and (way_to_go == 'nothing_to_execute'):

            continue_eval = False
            logger.info(f' Nothing found to execute in the Stage Processor of the stage {self.current_stage_ID} '
                        f'for the control {self.control_id} hence passing execution as TRUE')
            return True

        if continue_eval and (way_to_go == 'only_module_import'):

            continue_eval = False
            try:
                import importlib
                imprt_module = importlib.import_module(current_stage_processor_path_to_module)
                logger.info(f' Module {current_stage_processor_path_to_module} imported to '
                            f'process the control {self.control_id} with input params '
                            f' {self.control_params_dict} ')
                return True
            except Exception as error:
                logger.error(f' Error as {error} encountered for the stage {self.current_stage_ID} '
                             f' process the control {self.control_id} with input params '
                             f' {self.control_params_dict} ', exc_info=True)

                # return the execution of this stage as False
                return False

                # record the exception in the table for this execution

        if continue_eval and (way_to_go == 'only_method_execute'):
            continue_eval = False

            # Write code here to execute the method; currently only lambda functions are supported.
            # as every stage_processor should return Boolean only.
            # can also be used to set some value in the params_dict , but eventually return bool.
            # examples of method only are:
            # current_stage_processor_method_name = 'lambda: all((1,2,3))'
            # exec('out='+current_stage_processor_method_name)
            # out()
            try:
                dyn_result = None   # initialize later will be resolved to a boolean output
                exec('out_dyn_execute=' + current_stage_processor_method_name)
                exec('dyn_result=out_dyn_execute()')

                # output to be boolean
                # Currently the Stage_processor methods should only return Boolean

                if isinstance(dyn_result, bool):
                    return dyn_result
                else:
                    raise Exception(f' The method {current_stage_processor_method_name} called outputted '
                                    f'a NON Boolean output while processing the control '
                                    f'{self.control_id} in the stage {self.current_stage_ID} for the input params '
                                    f'{self.control_params_dict}')

            except Exception as error:
                logger.error(f' Error as {error} encountered for the stage {self.current_stage_ID} '
                             f' process the control {self.control_id} with input params '
                             f' {self.control_params_dict} ', exc_info=True)

                # if error-ed return the execution of this stage as False
                return False

                # record the exception in the table for this execution

        if continue_eval and (way_to_go == 'both_import_and_execute'):

            continue_eval = False

            try:
                # import module
                import importlib
                imprt_module = importlib.import_module(current_stage_processor_path_to_module)
                logger.info(f' Module {current_stage_processor_path_to_module} imported to '
                            f'process the control {self.control_id} with input params '
                            f' {self.control_params_dict} ')

                # execute the module's method
                logger.info(f' Method {current_stage_processor_method_name} called to process the control '
                            f'{self.control_id} in the stage {self.current_stage_ID} for the input params '
                            f'{self.control_params_dict}')

                result_frm_method_called = getattr(imprt_module, current_stage_processor_method_name)(self.appln, self.control_params_dict)

                # Currently the Stage_processor methods should only return Boolean
                if isinstance(result_frm_method_called, bool):
                    return result_frm_method_called
                else:
                    raise Exception(f' The method {current_stage_processor_method_name} called outputted '
                                    f'a NON Boolean output while processing the control '
                                    f'{self.control_id} in the stage {self.current_stage_ID} for the input params '
                                    f'{self.control_params_dict}')

            except Exception as error:
                logger.error(f' Error as {error} encountered for the stage {self.current_stage_ID} '
                             f' process the control {self.control_id} with input params '
                             f' {self.control_params_dict} ', exc_info=True)

                # if error-ed return the execution of this stage as False
                return False

                # record the exception in the table for this execution
        # if nothing is evaluated from any of above code units, which should not be the case, return True
        return bool_op

    def execute_pipeline(self):
        ''' This method once called will execute the entire pipeline. '''

        # set_pipeline sets up the pipeline as well as the initial stage.
        if self.set_pipeline():

            pipeline_list_of_stages = self.get_pipeline()

            # loop in till the exit is NOT flagged as True
            logger.info(f' Pipeline execution for the control {self.control_id} with input params as '
                        f'{self.control_params_dict} '
                        f'started ...')

            while not self.flag_exit:
                # for stage in pipeline_list_of_stages:

                # set the current stage
                # self.set_current_stage(stage["ID"], stage["STAGE"])
                logger.info(f' Pipeline execution for the control {self.control_id} with input params as '
                            f'{self.control_params_dict} moved to the stage {self.current_stage_ID} '
                           )
                print(self.current_stage, self.current_stage_ID, self.current_stage.name)

                # Do processing of the current_stage.
                bool_op_current_stage = False
                bool_op_current_stage = self.execute_current_stage_processor()

                # Once done , go to the next stage and pass to it the status of the previous stage
                # (i.e. the current stage that was) executed
                self.goto_next_stage(bool_op_current_stage)



