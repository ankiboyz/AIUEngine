# control_processing.py
'''
Author: Ankur Saxena

Objective : This module consists of methods those will be providing
            the processing logic needed to be worked out on the specific controls.
'''
import logging, json
import BCM.app_scope_methods.control_logic_library.pipeline_config as pipeline_config
import commons.connections.mongo_db_connections as mongo_db_conn
import run    # this was causing issue as run was getting re-executed and the oracle client library was getting re-initialized.
import BCM.models as models

logger = logging.getLogger(__name__)

# Each dictionary item is keyed in by the control_id.
# Each control_id key has value as the list of the dictionaries.
# Each dictionary item in the value denote a path of code to be executed.
# Items in the List denote the sequence to be followed.
# each of these methods would be presented with keyword arguments.
# here if the path of the code is changed , then it also has to be changed here.
control_logic_dict = {'TFA02_IFA19':
                          [{'path_to_module': 'BCM.app_scope_methods.control_logic_library.control_TFA02_IFA19'
                            ,'method_name': 'method_TFA02_IFA19'},]
                      ,
                    }


def delegator(control_id, dict_input):

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

    # Here , we will call the method to create an app object and also the db.
    # a tuple is returned by it - application and also initialized db without any connection yet been created.

    # so now this happens independently, not passed by the parent thread.
    # Hence we can now make use of it in forking as well as threading.

    application, database = run.create_ccm_app()

    # Here, let's gather the mongo DB connection and pass it to the control execution flowchart.
    mongo_client = mongo_db_conn.create_mongo_client(application.config["MONGO_DB_CONN_URI"])

    # Here it means that the Mongo Client has been gathered else in case of error it would have returned False
    if mongo_client:
        flwchart = ControlLifecycleFlowchart(control_id, application, database, mongo_client, dict_input)
        flwchart.execute_pipeline()
    else:
        pass
        # We need to log the job execution and stop it from execution; signal error for the job


    # a = {'RUN_ID': 1111, 'CONTROL_ID': "TFA02_IFA19", 'EXCEPTION_COLLECTION_NAME': "EXCEPTION_TFA02_IFA19"
    #     , 'FUNCTION_ID': "TFA02_IFA19"}
    # For testing purposes - END

    # code_units_list = control_logic_dict.get(control_id, list())
    # if len(code_units_list) == 0:
    #     logger.error(f'NO code units found to be executed for the control {control_id}.. Kindly check the configuration')
    #
    # else:
    #     for item in code_units_list:
    #         try:
    #             import importlib
    #             imprt_module = importlib.import_module(item["path_to_module"])
    #             func_to_be_called = item["method_name"]
    #
    #             logger.info(f' Method {func_to_be_called} called to process the control {control_id}')
    #             result_frm_func_called = getattr(imprt_module, func_to_be_called)(appln_cntxt, **kwargs)
    #
    #         except Exception as error:
    #             logger.error(f' Error encountered {error}', exc_info=True)
    #             # make entries into the detail table for the execution trace for that.
    #             # Fail the job for the ID
    #             break

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

    def __init__(self, control_id, appln, database, mongo_client, control_params_dict):
        # to invoke the lifecycle flowchart pass in the control_id and the control_params_dict and the app (to use the
        # app context for Flask-SQLAlchemy etc.).
        self.control_id = control_id
        self.control_params_dict = control_params_dict
        self.appln = appln
        self.mongo_client = mongo_client
        self.db = database
        # initialize it with None , pls do not chane this val , its used later in closing procedures.
        # will be set later upon by the specific method
        self.db_session = None

        # These attributes will be set by the class methods internally looking at the config.
        # Here, just assigning some initial values
        self.current_stage_ID = 'START'

        # here the pipeline_config.Stage becomes the object with () and keyword-ed ip params
        self.current_stage = pipeline_config.Stage(name='Initial', description='Let''s Start', stage_type='START'
                                                   , proceed_to='')
        self.current_stage_processor = pipeline_config.StageProcessor(path_to_module='', method_name='')
        self.pipeline = []  # pipeline is a simple list of the stages
        self.flag_exit = False      # flag to exit
        self.flag_error = False     # flag to denote some error happened and hence needed to fail the job

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

    def pipeline_initialization_procedures(self):
        ''' This method will set up the initialization procedures for the pipeline.
        Eg: Setting the DB session.
            Setting up the control params dict i.e input to the methods of the pipeline.
            Setting up the pipeline.

        Returns boolean output to signal whether the initialization procedures have been accomplished successfully.
        '''
        bool_op = False

        try:
            bool_resp = self.set_pipeline() # method returns a boolean response
            # This needs to be done before to get the db connection for the control_params_dict to get the data.
            self.set_db_session()
            self.set_control_params_dict()

            # it has reached here so no exceptions encountered in set_db_session and set_control_params_dict().
            # also consider the bool_resp from set_pipeline method.
            if bool_resp:
                bool_op = True

        except Exception as error:
            logger.error(f' Error {error} encountered while initializing pipeline for {self.control_id} '
                         f'with input params as '
                         f'{self.control_params_dict}', exc_info=True)

            self.signal_error(True)
            self.signal_exit(True)
            bool_op = False

        return bool_op

    def set_control_params_dict(self):
        ''' The Kafka Producer only sets the control_params_dict as {ID:'',CONTROL_ID: ''}
            Setting additional keywords to the dictionary.
        '''

        # The first place we are looking for the additional parameters are the Request args those are stored in the
        # DB table. Once we get the DB session i.e we get the values from the PARAMETERS tab
        try:
            # with self.db_session.begin(): # it seems that DB_Session need not be passed since models creates a db in the begining
            op_row = models.select_from_CCMMonitorHDR(self.control_params_dict['ID'], self.appln)

            # Since it is supposed to return  only one row ; list of result will have only one row.
            # ths string returned is converted to a json and hence wud come out as a dict
            # "{\"RUN_ID\": \"1b90f9d5-094d-4816-a8a8-8ddf04247486-1622726323002\",
            # \"CONTROL_ID\":\"TFA02_IFA19_1\",
            # \"EXCEPTION_COLLECTION_NAME\": \"EXCEPTION_TFA02_IFA19_1\",
            # \"FUNCTION_ID\": \"TFA02_COPY\"}"

            list_of_params_dict = json.loads(op_row[0].parameters)

            logger.debug(f'list_of_params_dict modified, {type(list_of_params_dict)}, {list_of_params_dict}')

            # Here, we will merge the 2 dicts in place, 2nd argument will override the first one
            self.control_params_dict = {**self.control_params_dict, **list_of_params_dict}

            logger.debug(f' The modified parameters dictionary is as {self.control_params_dict}')

        except Exception as error:
            # First log error and then signal exit.
            logger.error(f' Error encountered while setting the control_params_dict for {self.control_id} '
                         f' with input params as '
                         f' {self.control_params_dict}, error being {error}', exc_info=True
                         )
            self.signal_error(True)
            self.signal_exit(True)

    def signal_exit(self, bool_val):
        ''' True to Exit; False to not Exit '''
        if bool_val:
            self.flag_exit = True
            logger.info(f' Setting the exit flag for the pipeline for {self.control_id} with input params as '
                        f'{self.control_params_dict}')
        else:
            self.flag_exit = False

    def signal_error(self, bool_val):
        ''' True to flag error encountered; False to reset the flag to False '''
        if bool_val:
            self.flag_error = True
            logger.info(f' Setting the Error flag for the pipeline for {self.control_id} with input params as '
                        f'{self.control_params_dict}')
        else:
            self.flag_error = False

    def do_closing_procedures(self):
        self.mongo_client.close()   # Close the client
        logger.info(f' Mongo Client is closed after executing the pipeline for the control {self.control_id} '
                    f' with input params as '
                    f'{self.control_params_dict}')
        # releases the proxy (Connection obj) of the actual connection (via DBAPI) to the pool
        # - then it can be handled by garbage collection
        # Here we need to make a check if there has been an error in getting session itself then ignore this step as
        # its a close call on NOne - with which it was initialized earlier.
        if self.db_session is not None:
            self.db_session.close()
            logger.info(f' DB Session is closed after executing the pipeline for the control {self.control_id} '
                        f' with input params as '
                        f'{self.control_params_dict}')

    def set_db_session(self):
        ''' This method will set the db session for the pipeline.
        Currently, the idea is to set the db session for a pipeline and let the entire pipeline use that session.
        Once done the end of the pipeline stage will close the session - in order to free up the resources.
        '''
        try:
            with self.appln.app_context():
                self.db_session = self.db.session()

        except Exception as error:
            logger.error(f' Error in getting the Database session in the pipeline for control {self.control_id} '
                         f' with the input parameters as {self.control_params_dict} , error being '
                         f'{error} ', exc_info=True)
            self.signal_error(True)     # an error been encountered
            self.signal_exit(True)  # signal the exit

        # finally:                          # this is handled in the execute pipeline finally the closing procedures.
        #     self.do_closing_procedures()

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

        if (next_stage_id.strip() == '') or (next_stage_id.strip() == 'EXIT'):
            # Graceful Exit , completed all the tasks.
            self.signal_exit(True)
            # logger.info(f' Setting the exit flag for the pipeline for {self.control_id} with input params as '
            #             f'{self.control_params_dict}')
            # self.flag_exit = True

        elif (current_stage_type != 'decision') and (bool_op_current_executed_stage is False):
            # current stage was not a decision node and it returned op as False
            # Here, we have to signal Error as well along with , Exit. Exit as a result of Error.
            self.signal_error(True)
            self.signal_exit(True)

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
                self.signal_error(True)
                self.signal_exit(True)

                # self.flag_exit = True

            else:
                # Since there should be only one matching item in the list
                self.set_current_stage(list_matching_next_stage_id[0]["ID"], list_matching_next_stage_id[0]["STAGE"]
                                       , list_matching_next_stage_id[0]["STAGE_PROCESSOR"])

    # @staticmethod
    def format_returned_output(self, result_frm_method_called):
        ''' This checks the output of the response and verifies if it is successful execution or NOT
            The input could either be boolean or it could be dictionary as per the format below:
            {'STATUS':, 'STATUS_COMMENTS':, 'DETAIL_SECTION':{KEY : {'value': value,'comment': comment text}}}
        '''
        if isinstance(result_frm_method_called, bool):
            return_val_bool = result_frm_method_called
            reason_text = ''
            return_val_dict = {}    # return blank dict

        elif isinstance(result_frm_method_called, dict):
            # if the response is a dict
            return_val_bool = True if result_frm_method_called.get('STATUS', False) == 'SUCCESS' else False

            # This is to accommodate the case when decision node runs into error, so only True / False is nt sufficient.
            is_error_encountered = True if result_frm_method_called.get('STATUS', False) == 'ERROR' else False
            if is_error_encountered:
                self.signal_error(True)

            # Boolean and String comparison does not throw an error.
            reason_text = 'SUCCESS' if result_frm_method_called.get('STATUS', False) == 'SUCCESS' else \
                result_frm_method_called.get('STATUS_COMMENTS', 'FAILURE')

            # copies the entire dictionary painstakingly created by the internal methods.
            return_val_dict = result_frm_method_called

        else:
            self.signal_error(True)
            raise Exception(f' The method called outputted '
                            f'a NON Boolean/ non dict output while processing the control '
                            f'in the pipeline for control {self.control_id} with the input params '
                            f'as {self.control_params_dict}')
        # returns the tuple (True, SUCCESS,
        # {'STATUS':, 'STATUS_COMMENTS':, 'DETAIL_SECTION':{KEY : {'value': value,'comment': comment text}}})
        return return_val_bool, reason_text, return_val_dict

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
                # Calling the method to decipher the returned values
                return_val_bool, reason_text, return_val_dict = self.format_returned_output(dyn_result)
                return return_val_bool

                # if isinstance(dyn_result, bool):
                #     return dyn_result
                # else:
                #     raise Exception(f' The method {current_stage_processor_method_name} called outputted '
                #                     f'a NON Boolean output while processing the control '
                #                     f'{self.control_id} in the stage {self.current_stage_ID} for the input params '
                #                     f'{self.control_params_dict}')

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

                # passed the input params as well appln, mongo_client, control_params_dict
                result_frm_method_called = getattr(imprt_module, current_stage_processor_method_name)\
                    (self.appln, self.db_session, self.mongo_client, self.control_params_dict)

                # Calling the method to decipher the returned values
                return_val_bool, reason_text, return_val_dict = self.format_returned_output(result_frm_method_called)
                return return_val_bool

                #
                # if isinstance(result_frm_method_called, bool):
                #     return result_frm_method_called
                # elif isinstance(result_frm_method_called, dict):
                #     # to code here
                #     if result_frm_method_called.get('SUCCESS', False)
                #     re = result_frm_method_called.get('SUCCESS', False)
                # else:
                #     raise Exception(f' The method {current_stage_processor_method_name} called outputted '
                #                     f'a NON Boolean output while processing the control '
                #                     f'{self.control_id} in the stage {self.current_stage_ID} for the input params '
                #                     f'{self.control_params_dict}')

            except Exception as error:
                logger.error(f' Error as {error} encountered for the stage {self.current_stage_ID} '
                             f' process the control {self.control_id} with input params '
                             f' {self.control_params_dict} ', exc_info=True)
                self.signal_error(True)
                # if error-ed return the execution of this stage as False

                return False

                # record the exception in the table for this execution
        # if nothing is evaluated from any of above code units, which should not be the case, return True
        return bool_op

    def execute_pipeline(self):
        ''' This method once called will execute the entire pipeline. '''

        # set_pipeline sets up the pipeline as well as the initial stage.
        # if self.set_pipeline():
        if self.pipeline_initialization_procedures():

            pipeline_list_of_stages = self.get_pipeline()

            # loop in till the exit is NOT flagged as True
            logger.info(f' Pipeline execution for the control {self.control_id} with input params as '
                        f'{self.control_params_dict} '
                        f'started ...')
            try:
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

            except Exception as error:
                self.signal_error(True)
                logger.error(f'Following error signalled while executing pipeline for the control {self.control_id} '
                             f'with input params as '
                             f'{self.control_params_dict} error being {error} ', exc_info=True)

            finally:
                if self.flag_error:
                    # here we need to mark the job in DB
                    logger.info(f'The job with ID is marked as FAILURE for the pipeline execution '
                                f'for the control {self.control_id} with input params as '
                                f'{self.control_params_dict}')

                # Error in the closing procedures will not account for the job's status as all the work related to job
                # has been accomplished.
                self.do_closing_procedures()
                logger.info(f' Closing procedures called - '
                            f' for the control {self.control_id} '
                            f' with input params as '
                            f'{self.control_params_dict}')

    def add_to_globals_dict(self, global_to_pipeline_item):
        # reserved for next dev cycle , here we will be able to add custom objects to the globals
        # eg appln context, param dict and connection client.
        # Here methods will be execute and output of those methods will be added to the Globals dictionary available to
        # all the stages of the pipeline.
        pass

