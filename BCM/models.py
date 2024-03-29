#models.py
'''
Author : Ankur Saxena

This file contains all the DB tables needed by the BCM App.
'''

from flask_sqlalchemy import SQLAlchemy
import enum
# from BCM.app_scope_methods import ccm_sequences
from datetime import datetime, timedelta
import logging
from flask import current_app

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
    FAILURE = 'FAILURE'
    COMPLETED = 'COMPLETED'     # Keeping both completed and Success for now , but will be fine tuning it.
    SUCCESS = 'SUCCESS'

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

# coz of this metadata, the sequence is created in same schema automatically if not existing.
# Any new sequence created should have an entry in the tbl_sequence_association_dict dictionary below
# (after the tables been defined).
glt_ccm_xtnd_hdr_seq = db.Sequence('GLT_CCM_XTND_HDR_SEQ', metadata=db.metadata)    # For Header table
glt_ccm_xtnd_dtl_seq = db.Sequence('GLT_CCM_XTND_DTL_SEQ', metadata=db.metadata)    # For Detail table

# print('glt_ccm_xtnd_hdr_seq Next Val is ', glt_ccm_xtnd_hdr_seq.next_value())
# print('glt_ccm_xtnd_dtl_seq Next Val is ', glt_ccm_xtnd_dtl_seq.next_value())


class BCMMonitorHDR(db.Model):
    __tablename__ = 'glt_ccm_xtnd_monitor_header'

    id = db.Column(db.BigInteger, primary_key=True)
    # id = db.Column(db.BigInteger, db.Sequence('glt_ccm_xtnd_hdr_seq'), primary_key=True)
    control_id = db.Column(db.String(50), nullable=False)
    run_id = db.Column(db.String(500), nullable=True)
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


# This is the mapper dictionary for the association of the Table name with the Sequence Generator.
# The Table_name key is populated in a way such that the change in the name of the table will
# not levy an impact on the logic.
tbl_sequence_association_dict = {1:
                                 {'TABLE_NAME': f'{BCMMonitorHDR.__dict__["__table__"]}'
                                  , 'SEQUENCE_GENERATOR_NAME': 'GLT_CCM_XTND_HDR_SEQ'
                                  , 'COLUMN_NAME': 'id'
                                  },
                                 2:
                                 {'TABLE_NAME': f'{BCMMonitorDTL.__dict__["__table__"]}'
                                     , 'SEQUENCE_GENERATOR_NAME': 'GLT_CCM_XTND_DTL_SEQ'
                                     , 'COLUMN_NAME': 'id'
                                  },
                                 }

logger.debug('Table Sequence Association Dictionary', tbl_sequence_association_dict)
print('Table Sequence Association Dictionary', tbl_sequence_association_dict)


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

    with current_app.app_context():
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

        # Added this block as part of the APP-2816 https://greenlightcorp.atlassian.net/browse/APP-2816.
        # The issue with the unique constraint error.

        # Here, we gather the sequence generator for this table and get the next value for this generator.

        for k,v in tbl_sequence_association_dict.items():
            if (v['TABLE_NAME'].upper() == ip_tablename.upper()) & (v['COLUMN_NAME'].upper() == ip_columnname.upper()):
                sequence_gen_name = v.get('SEQUENCE_GENERATOR_NAME', '')
                break;


        # start_value = current_sequence_value + 1
        # end_value = current_sequence_value + ip_batchsize  #so,it denotes the sequence values exhausted by this point.

        start_value = db.engine.execute(db.Sequence(sequence_gen_name))
        # Here, the start_value is copied to the end_value; if batch size is > 1 then it will be updated with new vale later below.
        end_value = start_value

        # If the ip_batchsize is greater than 1 then we need to loop in to get the last sequence.
        if ip_batchsize > 1:
            for i in range(1, ip_batchsize): # for batch size =2 it loops only once as start val is already gathered.
                end_value = db.engine.execute(db.Sequence(sequence_gen_name))

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
                logger.info(f' Topic Id {topic_id} being updated for status, to be set as {row_status}, is  {row.control_id}')
                print(f' Topic Id  {topic_id} being updated for status, to be set as {row_status}, is  {row.control_id}')

                if row_status == 'DOWN':
                    row.status = KafkaConsumerEnum.DOWN  # make it DOWN

                if row_status == 'UP':
                    row.status = KafkaConsumerEnum.UP  # make it UP

                db.session.commit()
                print(f' row modified is {row}')


def select_from_CCMMonitorHDR(id, appln):
    ''' This method will select the values for the row for the specific ID value'''
    # with appln.app_context(): # commenting as appln context shud not be
    # destroyed as might have to be needed downstream
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
        Returns the job detail id for the entry inserted in the DB.
    '''

    # this should not be needed anymore since the app_context() is pushed in the initialization proccs of pipeline
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
        bcm_monitor_dtl_id = bcm_monitor_dtl.id
        try:
            # update the Header table as well to PROCESSING status
            update_header_table(header_id, 2, 'Execution Started', appln)

            db.session.commit()     # committing the detail later, ie after header table to take care of duration
            print(f'Committing the entries into the db {bcm_monitor_dtl}')

        except Exception as error:
            db.session.rollback()
            logger.error(f' There has been an ERROR while inserting the records in the Details table , error being {error}')
            raise error
        return bcm_monitor_dtl_id       # Here, in the return the .id should not be used.


def update_detail_table(job_detail_id, status, comments, appln):
    ''' This method will be used to update the status of the child_header_id.
        The comments will be passed to be updated in the detail table.
        If we are not making use of app.app_context then the current app proxy should be live . ie context is pushed.
        Flask context: https://testdriven.io/blog/flask-contexts/

        Since the Failure of the step signal the failure of the Header ID job as well, so signalling that from here.
    '''
    with db.session():

        # Here, without making use of app the session execution happened, because the context has been pushed in the
        # initialization procedure of the pipeline execution.
        bcm_monitor_dtl = BCMMonitorDTL()

        # since its the key only one is constrained to be available.
        job_dtl_id_row_2_update = bcm_monitor_dtl.query.filter_by(id=job_detail_id).first()
        # job_dtl_id_row_2_update = bcm_monitor_dtl.get(id=job_detail_id)
        # since this is first() so this is a row object.
        print('update_detail_table- query executed', job_dtl_id_row_2_update)

        if status == 0:
            status_enum_value = StatusEnum.FAILURE
        if status == 1:
            status_enum_value = StatusEnum.SUCCESS

        job_dtl_id_row_2_update.status = status_enum_value
        job_dtl_id_row_2_update.updated_date = datetime.now()

        job_dtl_id_row_2_update.end_date = datetime.now()
        job_dtl_id_row_2_update.duration = (job_dtl_id_row_2_update.end_date - job_dtl_id_row_2_update.start_date)/timedelta(minutes=1)
        # job_dtl_id_row_2_update.duration = job_dtl_id_row_2_update.end_date - job_dtl_id_row_2_update.start_date

        job_dtl_id_row_2_update.comments = str(comments)    # only string values else will error out.
        # max_run_sequence = 0
        # print(f'committing to the DB', db.session.dirty, db.session.new)
        # db.session.commit()

        # We will also mark the header job as failure.
        job_header_id = job_dtl_id_row_2_update.header_id
        # db.session.commit()
        try:
            # db.session.merge(job_dtl_id_row_2_update)
            # logger.debug(f'committing to the DB for the job detail id {job_detail_id}', db.session.dirty, db.session.new)
            db.session.commit()
            # Once the detail updated , updating the Header ID as well.
            # if status == 0:
            # !! Change to the logic NOw ,even if it is failure or Success update the Header that flags completion.
                # Only to be called if there is a Failure, since the header is dependent
                # on multiple child so if this pipeline_execution passes but the ack to ebcp does not pass
                # still the main job header is in fail state as it needs to be retried.
                # and also comments will be pertaining to the one that failed the job.
                # update_header_table(job_header_id, 0, comments, appln)
            update_header_table(job_header_id, status, comments, appln)

        except Exception as error:
            db.session.rollback()
            logger.error(f' Got error while updating the DB for the job detail ID {job_detail_id}; error as {error}')
            # raise error # Not raising it as it is in the final block of the pipeline execution


def update_header_table(job_header_id, status, comments, appln):
    ''' This is the method to update the header table with the final status.
        Status value as 0 for FAILURE
        Status value as 1 for SUCCESS
        Status value as 2 for PROCESSING
    '''
    with appln.app_context():   # This was needed as else with Mongo Error in delegator this was unable to get an application context.
        with db.session():

            # Here, without making use of app the session execution happened, because the context has been pushed in the
            # initialization procedure of the pipeline execution.
            bcm_monitor_hdr = BCMMonitorHDR()

            # since its the key only one is constrained to be available.
            job_hdr_id_row_2_update = bcm_monitor_hdr.query.filter_by(id=job_header_id).first()
            # since this is first() so this is a row object.
            print('update_detail_table- query executed', job_hdr_id_row_2_update)

            if status == 0:
                status_enum_value = StatusEnum.FAILURE
            if status == 1:
                status_enum_value = StatusEnum.SUCCESS
            if status == 2:
                status_enum_value = StatusEnum.PROCESSING

            job_hdr_id_row_2_update.status = status_enum_value

            # Its important to update updated date after the duration been calculated.
            # For retrials the creation date could the creation date as of 1 st trial , but we want duration for the
            # current trial.
            # job_hdr_id_row_2_update.updated_date = datetime.now()
            if status == 0 or status == 1:
                # This to be populated only when the job status has been SUCCESS or FAILURE.
                job_hdr_id_row_2_update.end_date = datetime.now()

                # Here for the Header the duration should be between the Updated and End Date.
                # as job header can be updated for Retrials later upon. End date will be picked up as we have set here in the session.
                job_hdr_id_row_2_update.duration = (job_hdr_id_row_2_update.end_date - job_hdr_id_row_2_update.updated_date)/timedelta(minutes=1)
                # job_dtl_id_row_2_update.duration = job_dtl_id_row_2_update.end_date - job_dtl_id_row_2_update.start_date

            job_hdr_id_row_2_update.updated_date = datetime.now()
            job_hdr_id_row_2_update.comments = str(comments)    # only string values else will error out.
            # max_run_sequence = 0
            # print(f'committing to the DB', db.session.dirty, db.session.new)
            # db.session.commit()

            # We will also mark the header job as failure.

            try:
                # db.session.merge(job_dtl_id_row_2_update)
                # logger.debug(f'committing to the DB for the job Header id {job_header_id}', db.session.dirty, db.session.new)
                # getting error in above statement as : TypeError: not all arguments converted during string formatting
                db.session.commit()

            except Exception as error:
                db.session.rollback()
                logger.error(f' Got error while updating the DB for the job Header ID {job_header_id}; error as {error}')
                # raise error # Not raising it as it is in the final block of the pipeline execution


def update_header_table_processing(comments, appln):
    ''' Update the Header table with PROCESSING status to FAILURE with comments. If upon start any of the rows
     are found with PROCESSING status , then update those'''
    with appln.app_context():
        with db.session():  # this takes care of closing the session and releasing to the pool at the end of the work
            bcm_monitor_hdr = BCMMonitorHDR()

            db_row_4_status_upd = bcm_monitor_hdr.query.filter_by(status=StatusEnum.PROCESSING).all()
            print('update_header_table_processing db_row_4_status_upd ', db_row_4_status_upd)

            for row in db_row_4_status_upd:
                logger.debug(f' Topic Id {row.id} being updated for status, from PROCESSING to FAILURE')
                print(f' Topic Id {row.id} being updated for status, from PROCESSING to FAILURE')
                row.status = StatusEnum.FAILURE
                row.end_date = datetime.now()
                row.updated_date = datetime.now()
                row.comments = comments
                row.duration = (row.end_date - row.updated_date)/timedelta(minutes=1)
                print(f' Header row modified is {row}')

            try:
                # logger.debug(f'committing to the DB for the job Header ', str(db.session.dirty), str(db.session.new))
                # getting error in above statement as : TypeError: not all arguments converted during string formatting
                db.session.commit()

            except Exception as error:
                db.session.rollback()
                logger.error(f' Got error while updating the DB for the job Header error as {error}')

