#models.py
'''
Author : Ankur Saxena

This file contains all the DB tables needed by the BCM App.
'''

from flask_sqlalchemy import SQLAlchemy
import enum
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
    step_id = db.Column(db.Integer, nullable=True)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=True)
    duration = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(50), nullable=True)
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
