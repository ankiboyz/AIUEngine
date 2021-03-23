from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class CCMMonitorHDR(db.Model):
    __tablename__ = 'ccm_monitor_header'

    run_id = db.Column(db.Integer, primary_key=True)
    control_id = db.Column(db.String(50), nullable=False)
    operation_type = db.Column(db.String(50), nullable=True)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=True)
    duration = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(50), nullable=True)
    details = db.relationship('CCMonitorDTL', backref='header', lazy=True)


class CCMonitorDTL(db.Model):
    __tablename__= 'ccm_monitor_detail'

    id = db.Column(db.Integer, primary_key=True)
    step_id = db.Column(db.Integer, nullable=True)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=True)
    duration = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(50), nullable=True)
    comments = db.Column(db.Text, nullable=True)
    header_id = db.Column(db.Integer, db.ForeignKey('ccm_monitor_header.run_id'), nullable=False)

