# ebcp_handshakes.py
''' This module deals with the methods related to handshakes with the EBCP application '''
import requests
import logging, json
from BCM import models
from commons import structured_response

logger = logging.getLogger(__name__)


def status_sync_with_ebcp(job_hdr_id, status, response_list, appln ):
    ''' This method is to sync up the status as observed in the AIU Engine back to EBCP '''

    ebcp_call_back_url = appln.config['EBCP_CALL_BACK_URL']
    logger.debug(f'This is the ebcp_call_back_url {ebcp_call_back_url}')

    # Since it is supposed to return  only one row ; list of result will have only one row.
    # ths string returned is converted to a json and hence wud come out as a dict
    # "{\"RUN_ID\": \"1b90f9d5-094d-4816-a8a8-8ddf04247486-1622726323002\",
    # \"CONTROL_ID\":\"TFA02_IFA19_1\",
    # \"EXCEPTION_COLLECTION_NAME\": \"EXCEPTION_TFA02_IFA19_1\",
    # \"FUNCTION_ID\": \"TFA02_COPY\"}"
    op_row = models.select_from_CCMMonitorHDR(job_hdr_id, appln)
    list_of_params_dict = json.loads(op_row[0].parameters)  # taking the PARAMETERS column value from Job Header table.
    run_id = op_row[0].run_id
    control_id_list = list_of_params_dict.get('CONTROL_ID_LIST', '')
    function_id = list_of_params_dict.get('FUNCTION_ID', '')

    # create response dict and pass it to the method
    # reponse_dict
    # based on status call either success or Failure message
    # std_err_resp_dict = structured_response.StructuredResponse(reponse_dict, 'GenericException') \
    #     .structure_the_error()


