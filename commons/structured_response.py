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
           , 'params': ('PAY05', 'CCM', None, None, None, None, None, None, None, None)
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
            error_dict["status"] = 'ERROR'
            # orig contains the original description of the error - actually it contains the object itself; so need to
            # stringify else it wont be possible to serialize it.
            error_dict["status_comments"] = str(self.resp_object.__dict__.get("orig", "orig key not found") )

            # error_dict["error_object_gathered"] = str(self.error_object.__dict__)  # put in the entire object gathered
            error_dict["detail_section"] = 'type of error object is ' + str(type(self.resp_object)) + '.  '  \
                                           + str(self.resp_object.__dict__)  # put in the entire object gathered

        if self.resp_type == 'GenericException':  # basically here it seems the exceptions are simple strings
            error_dict["status"] = 'ERROR'
            # orig contains the original description of the error - actually it contains the object itself; so need to
            # stringify else it wont be possible to serialize it.
            error_dict["status_comments"] = str(self.resp_object)

            # error_dict["error_object_gathered"] = str(self.error_object.__dict__)  # put in the entire object gathered
            error_dict["detail_section"] = 'type of error object is ' + str(type(self.resp_object)) + '.  ' \
                                           + str(self.resp_object)  # put in the entire object gathered
        return error_dict

    def structure_the_success(self):
        success_dict = dict()

        if self.resp_type == 'GenericSuccess':
            success_dict["status"] = 'SUCCESS'
            success_dict["status_comments"] = 'This has been successfully executed'
            success_dict["detail_section"] = str(self.resp_object)  # object passed is a dict itself so no __dict__

        return success_dict


