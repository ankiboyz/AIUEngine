#models.py
'''
Author : Ankur Saxena

This file contains all the DB tables needed by the BCM App.
'''

from flask_sqlalchemy import SQLAlchemy
import enum
# from BCM.app_scope_methods import ccm_sequences
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

status_values = ('SUBMITTED', 'PROCESSING', 'FAILED', 'COMPLETED')
print(dir(enum.Enum))
# status_values_enum = enum.Enum(**status_values)

# There needs to be 2 columns - job status and result  - job status - submitted, processing, completed
# result - Success / Failure
class StatusEnum(enum.Enum):
    # SUBMITTED = 'SUBMITTED'
    # PROCESSING = 'PROCESSING'
    # COMPLETED = 'COMPLETED'
    # FAILED = 'FAILED'
    SUBMITTED = 'SUBMITTED'
    PROCESSING = 'PROCESSING'
    FAILED = 'FAILED'
    COMPLETED = 'COMPLETED'

class StepNameEnum(enum.Enum):
    # SUBMITTED = 'SUBMITTED'
    # PROCESSING = 'PROCESSING'
    # COMPLETED = 'COMPLETED'
    # FAILED = 'FAILED'
    PIPELINE_EXECUTION = 'PIPELINE_EXECUTION'
    MAKE_AWARE_EBCP = 'MAKE_AWARE_EBCP'

class KafkaConsumerEnum(enum.Enum):
    # SUBMITTED = 'SUBMITTED'
    # PROCESSING = 'PROCESSING'
    # COMPLETED = 'COMPLETED'
    # FAILED = 'FAILED'
    UP = 'UP'
    DOWN = 'DOWN'

class YesNoEnum(enum.Enum):
    # SUBMITTED = 'SUBMITTED'
    # PROCESSING = 'PROCESSING'
    # COMPLETED = 'COMPLETED'
    # FAILED = 'FAILED'
    Y = 'Y'
    N = 'N'

db = SQLAlchemy()


class BCMMonitorHDR(db.Model):
    __tablename__ = 'glt_ccm_xtnd_monitor_header'

    id = db.Column(db.BigInteger, primary_key=True)
    control_id = db.Column(db.String(50), nullable=False)
    operation_type = db.Column(db.String(50), nullable=True)
    parameters = db.Column(db.Text, nullable=True)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=True)
    duration = db.Column(db.Float, nullable=True)
    # status = db.Column(db.String(50), nullable=True)
    # status = db.Column(enum.Enum(StatusEnum))
    # status = db.Column(db.Enum(status_values_enum))
    status = db.Column(db.Enum(StatusEnum))
    comments = db.Column(db.Text, nullable=True)
    created_date = db.Column(db.DateTime, nullable=False)
    updated_date = db.Column(db.DateTime, nullable=True)
    details = db.relationship('BCMMonitorDTL', backref='header', lazy=True)


class BCMMonitorDTL(db.Model):
    __tablename__= 'glt_ccm_xtnd_monitor_detail'

    id = db.Column(db.BigInteger, primary_key=True)
    step_name = db.Column(db.Enum(StepNameEnum), nullable=True)
    run_sequence = db.Column(db.Integer, nullable=True)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=True)
    duration = db.Column(db.Float, nullable=True)
    # status = db.Column(db.String(50), nullable=True)
    status = db.Column(db.Enum(StatusEnum))
    comments = db.Column(db.Text, nullable=True)
    created_date = db.Column(db.DateTime, nullable=False)
    updated_date = db.Column(db.DateTime, nullable=True)
    header_id = db.Column(db.Integer, db.ForeignKey('glt_ccm_xtnd_monitor_header.id'), nullable=False)


class BCMSequences(db.Model):
    __tablename__= 'glt_ccm_xtnd_sequences'

    table_name = db.Column(db.String(100), primary_key=True)
    column_name = db.Column(db.String(100), primary_key=True)
    sequences = db.Column(db.BigInteger, nullable=False)


class BCMControlEngineAssoc(db.Model):
    __tablename__= 'glt_ccm_xtnd_cntrl_ngn_assoc'

    engine_id = db.Column(db.String(100), primary_key=True)
    control_id = db.Column(db.String(100), primary_key=True)
    is_multiproc = db.Column(db.Enum(YesNoEnum))
    status = db.Column(db.Enum(KafkaConsumerEnum))

def sequences_provider(ip_tablename, ip_columnname, ip_batchsize, appln):

    '''
    Author: Ankur Saxena

    Objective: The method will connect to the database session configured with the SQLAlchemy and look
        for the GLT_CCM_XNTD_SEQUENCES table and look for the row having the table_name and column_name
        and the value of the sequence for that column will be incremented by the batch size and
        start and end sequence will be returned as a named tuple.

    Input: Table_name, column_name and batch_size.
        The name of the table and column for which the sequence needs to be generated.
        Also provide the batch_size to denote how many number of sequences need be generated.

    Output: returns the dictionary of the start_value and end_value of sequences for the input batch size.
    '''

    # This forms the query as shown here:
    # SELECT glt_ccm_xtnd_sequences.table_name AS glt_ccm_xtnd_sequences_table_name,
    # glt_ccm_xtnd_sequences.column_name AS glt_ccm_xtnd_sequences_column_name,
    # glt_ccm_xtnd_sequences.sequences AS glt_ccm_xtnd_sequences_sequences
    # FROM glt_ccm_xtnd_sequences
    # WHERE glt_ccm_xtnd_sequences.table_name = :table_name_1 AND glt_ccm_xtnd_sequences.column_name = :column_name_1

    # current_sequence_query = CCMSequences.query.filter_by(table_name='glt_ccm_xtnd_monitor_header', column_name='id')
    logger.info(f'The input parameters for this method are ip_tablename = {ip_tablename}'
                f', ip_columnname = {ip_columnname} and ip_batchsize = {ip_batchsize}')
    with appln.app_context():
        current_sequence_query = BCMSequences.query.filter_by(table_name=ip_tablename, column_name=ip_columnname)
        # results_all = current_sequence_query.all()
        results_first = current_sequence_query.first()

        print('This is current_sequence', current_sequence_query)
        print(type(results_first), results_first)

        if results_first is None:
            ccm_sequences_obj = BCMSequences(table_name=ip_tablename, column_name=ip_columnname, sequences=0)
            db.session.add(ccm_sequences_obj)   # insert happens here
            db.session.commit()
            current_sequence_value = 0
            results_first = ccm_sequences_obj

        else:
            current_sequence_value = results_first.sequences

        start_value = current_sequence_value + 1
        end_value = current_sequence_value + ip_batchsize  # so,it denotes the sequence values exhausted by this point.

        # and now commit with this value , only return after commit happens
        results_first.sequences = end_value

        try:
            # print('results_first.sequences' , results_first.sequences)
            db.session.commit()  # update happens here and the sequences value gets changed for the given table and colname.
        except Exception as error:
            db.session.rollback()
            return error        # formalize a return object. so create return objects and return those so it is universally

        print({'start_value': start_value, 'end_value': end_value})
        return {'start_value': start_value, 'end_value': end_value}

def consumer_status_update_per_control_engine(topic_id, row_status, appln):

    '''
    Objective : The function changes the status of the consumer , as per the engine in the DB.

    Input: Following are the inputs it needs:
        1. topic_id ; this is the control_id
        2. row_status : whether the control_id ie the consumer with name of control id need be brought up or down;
                        denoted by the input values as UP or DOWN.
        3. appln_cntxt: this is the app that needs to be passed in for the flask-sqlalchemy environment to work; and
                        also to get the config dictionary.
        '''

    with appln.app_context():
        with db.session():  # this takes care of closing the session and releasing to the pool at the end of the work
            cntrl_monitor_ngn_assoc = BCMControlEngineAssoc()

            # print(f"Consumer for topic {topic_id} been updated to status {row_status} ")
            logger.info(f'Consumer for topic {topic_id} been updated to status {row_status}')

            kfk_control_id = topic_id

            db_row_4_status_upd = cntrl_monitor_ngn_assoc.query.filter_by(control_id=kfk_control_id
                                                                          , engine_id=appln.config["ENGINE_ID"]).all()

            for row in db_row_4_status_upd:
                logger.info(f' Kafka Consumer for topic Id {topic_id} being updated for status, to be set as {row_status}, is  {row.control_id}')

                if row_status == 'DOWN':
                    row.status = KafkaConsumerEnum.DOWN  # make it DOWN

                if row_status == 'UP':
                    row.status = KafkaConsumerEnum.UP  # make it UP

                db.session.commit()
                print(f' row modified is {row}')

def select_from_CCMMonitorHDR(id, appln):
    ''' This method will select the values for the row for the specific ID value'''
    with appln.app_context():
        with db.session():
            bcm_monitor_hdr = BCMMonitorHDR()
            # used .all() However, ID is primary key, hence only row is supposed to be emitted.
            db_row = bcm_monitor_hdr.query.filter_by(id=id).all()

            print(f'db_row', type(db_row), type(db_row[0]), db_row, db_row[0].parameters, type(db_row[0].parameters)) # emitted object is list of row objects
            # db_row <class 'list'> < class 'BCM.models.BCMMonitorHDR' >
            # [< BCMMonitorHDR 56 >]
            # {"RUN_ID": "1b90f9d5-094d-4816-a8a8-8ddf04247486-1622726323002", "CONTROL_ID": "TFA02_IFA19_1"
            # , "EXCEPTION_COLLECTION_NAME": "EXCEPTION_TFA02_IFA19_1", "FUNCTION_ID": "TFA02_COPY"}
            # <class 'str'>
    return db_row

def insert_into_detail_table(header_id, step_name, appln):
    ''' This method will insert the entries into the Detail table for the job.
        This method will need to get the following info:
            1. The run sequence determines the sequence of run, in case of the retries , this will be useful.
    '''

    with appln.app_context():
        # .begin takes care of commit ; in case of error it rollbacks and raises the exception
        # with db.session.begin(): #This one is not working for flask-sqlalchemy , its not issueing commit.

        # get the max run sequence.
        bcm_monitor_dtl = BCMMonitorDTL()
        query_4_max_run_seq = bcm_monitor_dtl.query.filter_by(header_id=header_id).all()
        print('query_4_max_run_seq', query_4_max_run_seq)
        max_run_sequence = 0
        if len(query_4_max_run_seq) != 0:
            max_sequence = [i.run_sequence for i in query_4_max_run_seq]
            print('max_sequence_list', max_sequence, max(max_sequence))
            max_run_sequence = max(max_sequence)

        bcm_monitor_dtl.run_sequence = max_run_sequence + 1

        # get the ID for the DTL table.
        # this provides the string repr of the object returned, string is needed else error in the sequence provider
        bcm_monitor_dtl_table_name = f'{BCMMonitorDTL.__dict__["__table__"]}'
        ret_op = sequences_provider(ip_tablename=bcm_monitor_dtl_table_name,
                                    ip_columnname='id',
                                    ip_batchsize=1, appln=appln)
        bcm_monitor_dtl.id = ret_op['start_value']  # since batchsize was given as 1, so start and end will be same.

        if step_name == 'PIPELINE_EXECUTION':
            bcm_monitor_dtl.step_name = StepNameEnum.PIPELINE_EXECUTION
        if step_name == 'MAKE_AWARE_EBCP':
            bcm_monitor_dtl.step_name = StepNameEnum.MAKE_AWARE_EBCP

        # Setting the created datetime and Start_date time
        bcm_monitor_dtl.created_date = datetime.now()
        bcm_monitor_dtl.updated_date = datetime.now()
        bcm_monitor_dtl.start_date = datetime.now()
        bcm_monitor_dtl.status = StatusEnum.SUBMITTED
        bcm_monitor_dtl.header_id = header_id
        db.session.add(bcm_monitor_dtl)
        try:
            db.session.commit()
            print(f'Committing the entries into the db {bcm_monitor_dtl}')
        except Exception as error:
            db.session.rollback()
            logger.error(f' There has been an ERROR while inserting the records in the Details table , error being {error}')
            raise error
        return bcm_monitor_dtl.id

def update_detail_table(child_header_id, step_name, status, comments, appln):
    ''' This method will be used to update the status of the child_header_id.
        The comments will be passed to be updated in the detail table.
        If we are not making use of app.app_context then the current app proxy should be live . ie context is pushed.
        Flask context: https://testdriven.io/blog/flask-contexts/
    '''
    # Here, without making use of app the session execution happened, because the context has been pushed in the
    # initialization procedure of the pipeline execution.
    bcm_monitor_dtl = BCMMonitorDTL()
    query_4_max_run_seq = bcm_monitor_dtl.query.filter_by(header_id=child_header_id).all()
    print('update_detail_table- query executed', query_4_max_run_seq)
    max_run_sequence = 0


# For unit testing:
# if __name__ == "__main__":
#     print(__name__)
#     # app.debug = True
#     # db.create_all(app=app)
#     # app.run()
#
#     import cx_Oracle
#     import run, BCM
#     from flask import current_app
#
#     # initializing the cx_Oracle client to connect to the Oracle database.
#     cx_Oracle.init_oracle_client(lib_dir=BCM.app.config["ORACLE_CLIENT_PATH"])
#
#     appln, db = run.create_ccm_app()    # as it returns the tuple of application and db
#     with appln.app_context():
#         select_from_CCMMonitorHDR(1, current_app)
