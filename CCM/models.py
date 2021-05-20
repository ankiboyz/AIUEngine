from flask_sqlalchemy import SQLAlchemy
import enum

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
    status = db.Column(db.Enum(KafkaConsumerEnum))