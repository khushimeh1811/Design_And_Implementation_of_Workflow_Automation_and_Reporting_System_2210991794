from datetime import datetime
from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


class AuditRun(db.Model):
    __tablename__ = "audit_runs"

    id = db.Column(db.Integer, primary_key=True)
    workflow_type = db.Column(db.String(32), nullable=False)
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(32), default="pending")

    gold_file = db.Column(db.String(512), nullable=True)
    other_file = db.Column(db.String(512), nullable=True)
    output_file = db.Column(db.String(512), nullable=True)

    summary_json = db.Column(db.Text, nullable=True)
    error_json = db.Column(db.Text, nullable=True)

    # store the original file names for display
    gold_filename = db.Column(db.String(128), nullable=True)
    other_filename = db.Column(db.String(128), nullable=True)
    output_filename = db.Column(db.String(128), nullable=True)
