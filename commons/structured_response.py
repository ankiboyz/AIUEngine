# structured_response
''' This file is focused on creating a structured error message structure.'''

import logging

logger = logging.getLogger(__name__)

class StructuredResponse:

    '''
    Author: Ankur Saxena

    Objective: The instance of this class will return the structured error message conforming to the structure
               as prescribed by this class.

    Input: error object as returned and the type of the error; based on the error type the parsing methods can be
           enhanced.
           Example of an error object as gathered as error.__dict__
           {'code': 'e3q8'
           , 'statement': 'INSERT INTO glt_ccm_xtnd_monitor_header (control_id, operation_type, parameters, start_date, end_date, duration, status, comments, created_date, updated_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
           , 'params': ('REALIZER', 'BCM', None, None, None, None, None, None, None, None)
           , 'orig': OperationalError('no such table: glt_ccm_xtnd_monitor_header')
           , 'hide_parameters': False
           , 'detail': []
           , 'connection_invalidated': False}
            <class 'sqlalchemy.exc.OperationalError'>

    Output: returns the structured error object
    '''

    # def __init__(self, error_object, error_type): resp_object would be the response object (success/error);
    # type will be the type within that error obj or success obj if further handling need be segregated.
    def __init__(self, resp_object, resp_type):
        self.resp_object = resp_object
        self.resp_type = resp_type

    def structure_the_error(self):
        error_dict = dict()

        # covering for all currently therefore included 'GenericException' also here. Later on based on other types to
        # be accommodated, it can be put in other if condition
        if self.resp_type == 'SQLAlchemyException':
            # error_dict["status"] = 'ERROR'
            error_dict["status"] = 'FAILURE'
            # orig contains the original description of the error - actually it contains the object itself; so need to
            # stringify else it wont be possible to serialize it.
            error_dict["status_comments"] = str(self.resp_object.__dict__.get("orig", "orig key not found") )

            # error_dict["error_object_gathered"] = str(self.error_object.__dict__)  # put in the entire object gathered
            if not isinstance(self.resp_object, dict):  # detail section to be a dict always
                error_dict["detail_section"] = {'DETAILS': 'type of error object is ' + str(type(self.resp_object))
                                                + '.  '
                                                + str(self.resp_object.__dict__)}  # put in the entire object gathered
            else:   # if its a dict
                # error_dict["detail_section"] = str(self.resp_object) # Newly Fixed -- needed an object at EBCP side
                error_dict["detail_section"] = self.resp_object

        if self.resp_type == 'GenericException':  # basically here it seems the exceptions are simple strings
            # error_dict["status"] = 'ERROR'
            error_dict["status"] = 'FAILURE'
            # orig contains the original description of the error - actually it contains the object itself; so need to
            # stringify else it wont be possible to serialize it.
            error_dict["status_comments"] = str(self.resp_object)

            # error_dict["error_object_gathered"] = str(self.error_object.__dict__)  # put in the entire object gathered
            # error_dict["detail_section"] = 'type of error object is ' + str(type(self.resp_object)) + '.  ' \
            #                                + str(self.resp_object)  # put in the entire object gathered

            if not isinstance(self.resp_object, dict):  # detail section to be a dict always
                error_dict["detail_section"] = {'DETAILS': 'type of error object is ' + str(type(self.resp_object))
                                                           + '.  '
                                                           + str(self.resp_object.__dict__)}  # put in the entire object gathered
            else:  # if its a dict
                # error_dict["detail_section"] = str(self.resp_object)  # Newly Fixed -- needed an object at EBCP side
                error_dict["detail_section"] = self.resp_object

        return error_dict

    def structure_the_success(self):
        success_dict = dict()

        if self.resp_type == 'GenericSuccess':
            success_dict["status"] = 'SUCCESS'
            success_dict["status_comments"] = 'This has been successfully submitted! :)'
            # success_dict["detail_section"] = str(self.resp_object)  # object passed is a dict itself so no __dict__
            success_dict["detail_section"] = self.resp_object   # as needed by EBCP

        return success_dict


class StageOutputResponseDict:
    ''' Method to formulate the stage output response to the outer pipeline '''
    # {'STATUS': 'SUCCESS',
    # 'STATUS_COMMENTS': 'executed_successfully',
    # 'DETAIL_SECTION': {
    #     'DATABASE_NAME': {'value': 'PHILIPS_BCM', 'comment': 'This is the Mongo DB connected for this stage'},
    #     'FUNCTION_COLLECTION': {'value': 'TFA02_COPY',
    #                             'comment': 'This is the Mongo DB collection operated upon for this stage'},
    #     'RUN_ID': {'value': '1b90f9d5-094d-4816-a8a8-8ddf04247486-1622726323002',
    #                'comment': 'This is the runID passed for this stage execution'},
    #     'RESULT_FROM_OPERATION': {'value': 'UPDATE_MANY', 'comment': 'modified count = 0 matched count =  0 '}}}

    def __init__(self):
       self.method_op_dict = {'STATUS': '', 'STATUS_COMMENTS': '', 'DETAIL_SECTION': {}}

    def add_to_detail_section_dict(self, keyname, value, comment):
        ''' Method to add to the detail section of the response to be passed from the stage to the pipeline'''
        self.method_op_dict['DETAIL_SECTION'][keyname] = {'value': value, 'comment': comment}

    def add_to_status(self, status):
        ''' set to SUCCESS if 1 is passed else Failure for all other values'''
        # self.method_op_dict['STATUS'] = 'SUCCESS' if status == 1 else 'FAILURE'
        if status == 1:
            self.method_op_dict['STATUS'] = 'SUCCESS'
        elif status == 0:
            self.method_op_dict['STATUS'] = 'FAILURE'
        else:
            self.method_op_dict['STATUS'] = status      # for eg yesID, noID passed for decision type of nodes.


    def add_to_status_comments(self, status_comments):
        self.method_op_dict['STATUS_COMMENTS'] = status_comments
