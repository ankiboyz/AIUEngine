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

    # we will select the data, that is to be sent back, from the JOB HEADER table.
    op_row = models.select_from_CCMMonitorHDR(job_hdr_id, appln)
    list_of_params_dict = json.loads(op_row[0].parameters)  # taking the PARAMETERS column value from Job Header table.
    id = op_row[0].id
    run_id = op_row[0].run_id
    control_id_list = list_of_params_dict.get('CONTROL_ID_LIST', '')
    function_id = list_of_params_dict.get('FUNCTION_ID', '')
    comments = op_row[0].comments

    comments_truncated = comments[0:150]        # sending first 150 characters.

    print('call back response_dict comments', comments)
    print('call back response_dict response_list', response_list)

    # create response dict and pass it to the method
    # reponse_dict
    response_dict = dict()
    response_dict['RUN_ID'] = run_id
    response_dict['CONTROL_ID_LIST'] = control_id_list
    response_dict['FUNCTION_ID'] = function_id
    response_dict['ID'] = id
    response_dict['ADDINTIONAL_INFO'] = comments_truncated + '...'
    # based on status call either success or Failure message
    # std_err_resp_dict = structured_response.StructuredResponse(response_dict, 'GenericException') \
    #     .structure_the_error()

    if status == 1:     # denotes SUCCESS
        # pass in the response dict
        structured_response_dict = structured_response.StructuredResponse(response_dict, 'GenericSuccess')\
            .structure_the_success()

    if status == 0:     # denotes FAILURE
        # pass in the response dict
        structured_response_dict = structured_response.StructuredResponse(response_dict, 'GenericException')\
            .structure_the_error()

    verify = appln.config.get('EBCP_VERIFY_CERT', False)

    certificate_path = appln.config.get('EBCP_PATH_TO_CERT', 'PATH_TO_CERT_NOT_SPECIFIED')
    certificate_key_path = appln.config.get('EBCP_PATH_TO_CERT_KEY', 'PATH_TO_CERT_KEY_NOT_SPECIFIED')

    logger.debug(f'Path to the certificate for EBCP Call Back {certificate_path}')
    logger.debug(f'Path to the certificate for EBCP Call Back {certificate_key_path}')

    data_to_be_posted = str(structured_response_dict)
    try:
        if not verify:
            response = requests.post(ebcp_call_back_url, data_to_be_posted, verify=False)       # verify False added for https ignoring.
        else:
            response = requests.post(ebcp_call_back_url, data_to_be_posted,
                                     cert=(certificate_path, certificate_key_path))       # verify False added for https ignoring.
            # response = requests.post(ebcp_call_back_url, data_to_be_posted,
            #                          verify=certificate_path)

            # response = requests.post(ebcp_call_back_url, data_to_be_posted,
            #                          cert=("C:\\Users\\ASaxena\\PycharmProjects\\AIUEngine\\commons\\external_app_specifics\\HTTPSCertificates\\example.crt"
            #                                , "C:\\Users\\ASaxena\\PycharmProjects\\AIUEngine\\commons\\external_app_specifics\\HTTPSCertificates\\example.key"))



    except Exception as error:
        logger.error(f'Error encountered while making a call back for the ID {id} and error is {error}', exc_info=True)

    print('response from ebcp call back', response)
    logger.info(f'Response from EBCP Call back {response} for ID {id}')


