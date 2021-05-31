#models.py
'''
Author : Ankur Saxena

This file contains all the DB tables needed by the CCM App.
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


class CCMMonitorHDR(db.Model):
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
    details = db.relationship('CCMonitorDTL', backref='header', lazy=True)


class CCMonitorDTL(db.Model):
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


class CCMSequences(db.Model):
    __tablename__= 'glt_ccm_xtnd_sequences'

    table_name = db.Column(db.String(100), primary_key=True)
    column_name = db.Column(db.String(100), primary_key=True)
    sequences = db.Column(db.BigInteger, nullable=False)


class CCMControlEngineAssoc(db.Model):
    __tablename__= 'glt_ccm_xtnd_cntrl_ngn_assoc'

    engine_id = db.Column(db.String(100), primary_key=True)
    control_id = db.Column(db.String(100), primary_key=True)
    is_multiproc = db.Column(db.Enum(YesNoEnum))
    status = db.Column(db.Enum(KafkaConsumerEnum))


def consumer_status_update_per_control_engine(topic_id, row_status, appln_cntxt):

    '''
    Objective : The function changes the status of the consumer , as per the engine in the DB.

    Input: Following are the inputs it needs:
        1. topic_id ; this is the control_id
        2. row_status : whether the control_id ie the consumer with name of control id need be brought up or down;
                        denoted by the input values as UP or DOWN.
        3. appln_cntxt: this is the app that needs to be passed in for the flask-sqlalchemy environment to work; and
                        also to get the config dictionary.
        '''

    with appln_cntxt.app_context():
        cntrl_monitor_ngn_assoc = CCMControlEngineAssoc()

        print('finally block called')
        kfk_control_id = topic_id

        db_row_4_status_upd = cntrl_monitor_ngn_assoc.query.filter_by(control_id=kfk_control_id
                                                                      , engine_id=appln_cntxt.config["ENGINE_ID"]).all()

        for row in db_row_4_status_upd:
            logger.info(f' Kafka Consumer control Id being updated for status is  {row.control_id}')

            if row_status == 'DOWN':
                row.status = KafkaConsumerEnum.DOWN  # make it DOWN

            if row_status == 'UP':
                row.status = KafkaConsumerEnum.UP  # make it UP

            print(f' row modified is {row}')

            db.session.commit()
