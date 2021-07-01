# realizer.py
"""Herein the Blueprint implementation for REALIZER control would go in.
This structure makes it easier for you to find the code and resources related to a given functionality.
For example, if you want to find the application logic about REALIZER control,
 then you can go to the REALIZER Blueprint in BCM/realizer.py instead of scrolling through a big app.py (in this specific case
  it is BCM/__init__.py )."""
import config
from flask import Blueprint, request, jsonify
from BCM.models import BCMMonitorHDR, BCMMonitorDTL, StatusEnum
import logging
import json
from BCM.REALIZER import services
from datetime import datetime

logger = logging.getLogger(__name__)

realizer_bp = Blueprint('realizer_bp', __name__,
                     template_folder='templates',
                     static_folder='static', static_url_path='pay05_static')


@realizer_bp.route('/')
def list_jobs():
    pay05_executions = BCMMonitorHDR.query.all()
    print(logger.parent, 'parent of logger of pay03 module')  # Here,the parent was coming as BCM.(chief of this APP)
    logger.info("I am in REALIZER with args as {0}".format(request.args))
    # logging.debug("I am in PA03 with args as {}", request.args)

    return str(pay05_executions)


@realizer_bp.route('/submitjob', methods=['GET', 'POST'])    # adding support for post
def submit_job():
    # Here, first of all lets gather the input args been passed.

    print(logger.parent, 'parent of logger of pay03 module')  # Here,the parent was coming as BCM.(chief of this APP)
    logger.info("I am in REALIZER with args as {0}".format(request.args))

    if request.method == 'GET':
        # currently only POST is supported
        # for get call an adidtional handling of list as str need to be handled:
        # http://127.0.0.1:50008/BCM/realizer/submitjob?RUN_ID=2bc36ea9-4865-4f39-8ebe-49f556acd566-1622621443070&CONTROL_ID_LIST=['TFA02_IFA19_1']&FUNCTION_ID=TFA02_COPY
        print('request.args', request.args)
        incoming_request_dict = request.args.to_dict()

        # passing request dict
        parameters_for_hdr_tbl_dict = from_incoming_request_prepare_dict_to_persist(incoming_request_dict)

        # Gathering the control ID from the request params
        control_id = parameters_for_hdr_tbl_dict['CONTROL_ID']
        print('request.args', request.args, jsonify(request.args), json.dumps(request.args))

    elif request.method == 'POST':

        print('request.data', request.data)
        decoded_data = request.data.decode('utf-8')

        incoming_data_dict = json.loads(decoded_data)
        logger.debug(f'incoming_data_dict {incoming_data_dict}')

        # passing data dict
        parameters_for_hdr_tbl_dict = from_incoming_request_prepare_dict_to_persist(incoming_data_dict)

        # Gathering the control ID from the form
        control_id = parameters_for_hdr_tbl_dict['CONTROL_ID']
        run_id = parameters_for_hdr_tbl_dict['RUN_ID']

    else:
        # 'Not Supported Operation - return message'
        # TBD
        pass

    CCMhdr_obj = services.CCMHeader()

    # created_date and updated_date will be filled up in the insert/update methods for the
    # particular obj eg CCMHeader and CCMDetail tables
    ret_op = CCMhdr_obj.insert(control_id=control_id, operation_type=config.OPERATION_TYPE
                               # , parameters=json.dumps(request.args)
                               , parameters=json.dumps(parameters_for_hdr_tbl_dict)
                               , start_date=datetime.now()
                               , status=StatusEnum.SUBMITTED
                               , run_id=run_id)

    # pay05_executions = CCMMonitorHDR.query.all()
    # print('pay05_executions', pay05_executions)

    # logging.debug("I am in PA03 with args as {}", request.args)

    # return str(pay05_executions)
    return jsonify(ret_op)

@realizer_bp.route('/test', methods=['GET', 'POST'])    # adding support for post
def test_job():
    # This is the test function.

    print(logger.parent, 'parent of logger of pay03 module')  # Here,the parent was coming as BCM.(chief of this APP)
    logger.info("I am in REALIZER with args as {0}".format(request.args))

    if request.method == 'GET':

        print('request.args', request.args)
        incoming_request_dict = request.args.to_dict()

        # passing request dict
        parameters_for_hdr_tbl_dict = from_incoming_request_prepare_dict_to_persist(incoming_request_dict)

        # Gathering the control ID from the request params
        control_id = parameters_for_hdr_tbl_dict['CONTROL_ID']
        print('request.args', request.args, jsonify(request.args), json.dumps(request.args))

    elif request.method == 'POST':

        print('request.form', request.form)
        request.get_data()
        print('request.form after get_data', request.form)
        # request.get_json()
        # print('request.form after get_json', request.form, request.get_json() )

        print('request.data', request.data)

        print('request.data.decode', request.data.decode('utf-8'))

        incoming_data_dict = json.loads(request.data.decode('utf-8'))
        print('incoming_data_dict', incoming_data_dict)

        # passing data dict
        parameters_for_hdr_tbl_dict = from_incoming_request_prepare_dict_to_persist(incoming_data_dict)

        # Gathering the control ID from the form
        control_id = parameters_for_hdr_tbl_dict['CONTROL_ID']
        print('request.form', request.form, jsonify(request.form), json.dumps(request.form))

    else:
        # 'Not Supported Operation - return message'
        # TBD
        pass

    return 'This is being tested'

def from_incoming_request_prepare_dict_to_persist(incoming_request_dict):
    # Here, we will massage the needed information for the downstream executions
    # like the application expects CONTROL_ID, RUN_ID, FUNCTION_ID, EXCEPTION_COLLECTION_NAME
    # If the values are not coming then populate as '' blanks will be verified.
    # Returns the well formatted dict to be persisted.

    # FOR request.args it is :
    #     request.args
    #     ImmutableMultiDict(
    #         [('RUN_ID', '2bc36ea9-4865-4f39-8ebe-49f556acd566-1622621443070'), ('CONTROL_ID_LIST', '[TFA02_IFA19_1]'),
    #          ('FUNCTION_ID', 'TFA02_COPY')])

    # Take all the parameters coming from the request.
    print('incoming_request_dict', incoming_request_dict)

    parameters_for_hdr_tbl_dict = incoming_request_dict

    # parameters_for_hdr_tbl_dict = {k[0]: k[1] for k in incoming_request_dict}
    # print('parameters_for_hdr_tbl_dict', parameters_for_hdr_tbl_dict)
    #
    # parameters_for_hdr_tbl_dict_test = {k[0]: k[1] for k in incoming_request_dict}
    # print('parameters_for_hdr_tbl_dict_test', parameters_for_hdr_tbl_dict_test)

    # parameters_for_hdr_tbl_dict = incoming_request_dict # if we do this then no additional
    # info can be added since it is immutable dict

    # And now add additional keys to the dictionary.
    # Currently only one control ID coming is supported.
    # control_id_list_str = parameters_for_hdr_tbl_dict.get('CONTROL_ID_LIST', '')

    control_id_list = parameters_for_hdr_tbl_dict.get('CONTROL_ID_LIST', [])

    print('control_id_list', control_id_list)   # ["TFA02_IFA19_1"]

    # control_id_list_1_sq_bracket_removed_str = control_id_list_str.replace('[', '')
    # control_id_list_2_sq_bracket_removed_str = control_id_list_1_sq_bracket_removed_str.replace(']', '')
    # control_id_list = control_id_list_2_sq_bracket_removed_str.split(',')

    print(control_id_list)

    print('control_id_list', control_id_list, control_id_list[0])
    parameters_for_hdr_tbl_dict['CONTROL_ID'] = control_id_list[0]  # Currently only one control ID coming is supported.

    parameters_for_hdr_tbl_dict['FUNCTION_ID'] = incoming_request_dict.get('FUNCTION_ID', '')

    parameters_for_hdr_tbl_dict['RUN_ID'] = incoming_request_dict.get('RUN_ID', '')

    # EXCEPTION_COLLECTION_NAME is the name of the EXCEPTION_ appended with the Function_ID
    # eg EXCEPTION_TFA02_IFA19_1
    parameters_for_hdr_tbl_dict['EXCEPTION_COLLECTION_NAME'] = 'EXCEPTION_'+parameters_for_hdr_tbl_dict['CONTROL_ID']

    print('parameters_for_hdr_tbl_dict', parameters_for_hdr_tbl_dict)
    logger.debug(f'created parameters dictionary from incoming request is {parameters_for_hdr_tbl_dict}')

    return parameters_for_hdr_tbl_dict