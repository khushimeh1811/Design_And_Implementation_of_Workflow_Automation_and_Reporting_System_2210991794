import json
import os
import uuid
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from flask import (Flask, abort, flash, redirect, render_template, request,
                   send_file, url_for)

# When running as a script (python workflow_automation_app\app.py) the
# package name isn't available in sys.path, so we fall back to local imports.
try:
    from workflow_automation_app.audit import AuditEngine
    from workflow_automation_app.models import AuditRun, db
except ImportError:
    from audit import AuditEngine
    from models import AuditRun, db

# -----------------------------------------------------------
# Configuration
# -----------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
DATABASE_FILE = os.path.join(BASE_DIR, "data.db")
ALLOWED_EXTENSIONS = {"xlsx", "xls", "csv"}

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ.get("FLASK_SECRET_KEY", "change-me-for-prod"),
    SQLALCHEMY_DATABASE_URI=f"sqlite:///{DATABASE_FILE}",
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
)

db.init_app(app)


@app.context_processor
def inject_globals():
    return {"current_year": datetime.utcnow().year}


@app.route("/")
def index():
    return redirect(url_for("dashboard"))


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _save_upload(file_storage, prefix: str) -> str:
    """Save uploaded file to disk and return the saved path."""
    _, ext = os.path.splitext(file_storage.filename)
    safe_name = f"{prefix}_{uuid.uuid4().hex}{ext}"
    path = os.path.join(UPLOAD_DIR, safe_name)
    file_storage.save(path)
    return path


def _ensure_db():
    """Initialize the database schema."""
    with app.app_context():
        db.create_all()


def _persist_run(
    workflow_type: str,
    gold_path: str,
    other_path: str,
    gold_filename: str,
    other_filename: str,
    output_path: str,
    output_filename: str,
    status: str,
    summary: dict | None = None,
    error_data: list[dict] | None = None,
):
    run = AuditRun(
        workflow_type=workflow_type,
        status=status,
        gold_file=gold_path,
        other_file=other_path,
        gold_filename=gold_filename,
        other_filename=other_filename,
        output_file=output_path,
        output_filename=output_filename,
        summary_json=json.dumps(summary or {}),
        error_json=json.dumps(error_data or []),
        finished_at=datetime.utcnow(),
    )
    db.session.add(run)
    db.session.commit()
    return run


def _schedule_latest_run():
    """Re-run the most recent successful audit as a scheduled job."""
    with app.app_context():
        latest = (
            AuditRun.query.filter_by(status="completed")
            .order_by(AuditRun.finished_at.desc())
            .first()
        )
        if not latest:
            app.logger.info("No completed runs to schedule.")
            return

        if not (os.path.exists(latest.gold_file) and os.path.exists(latest.other_file)):
            app.logger.warning("Scheduled run skipped because input files are missing.")
            return

        engine = AuditEngine()
        output_name = f"scheduled_{latest.workflow_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_path = os.path.join(OUTPUT_DIR, output_name)

        try:
            if latest.workflow_type == "salto":
                metadata, error_df = engine.run_salto_audit(latest.gold_file, latest.other_file, output_path)
            else:
                metadata, error_df = engine.run_sesam_audit(latest.gold_file, latest.other_file, output_path)

            _persist_run(
                workflow_type=latest.workflow_type,
                gold_path=latest.gold_file,
                other_path=latest.other_file,
                gold_filename=latest.gold_filename,
                other_filename=latest.other_filename,
                output_path=output_path,
                output_filename=output_name,
                status="completed",
                summary=metadata,
                error_data=error_df.to_dict(orient="records") if error_df is not None else [],
            )
            app.logger.info("Scheduled audit run completed: %s", output_name)
        except Exception as exc:
            app.logger.exception("Scheduled audit run failed.")


@app.route("/dashboard")
def dashboard():
    runs = AuditRun.query.order_by(AuditRun.finished_at.desc()).limit(50).all()
    total_runs = AuditRun.query.count()
    success_runs = AuditRun.query.filter_by(status="completed").count()
    failed_runs = total_runs - success_runs
    last_run = AuditRun.query.order_by(AuditRun.finished_at.desc()).first()

    # Chart data (distribution by workflow type)
    workflow_counts = {
        "salto": AuditRun.query.filter_by(workflow_type="salto").count(),
        "sesam": AuditRun.query.filter_by(workflow_type="sesam").count(),
    }

    return render_template(
        "dashboard.html",
        total_runs=total_runs,
        success_runs=success_runs,
        failed_runs=failed_runs,
        last_run=last_run,
        chart_labels=list(workflow_counts.keys()),
        chart_values=list(workflow_counts.values()),
    )


@app.route("/run_audit")
def run_audit():
    return render_template("run_audit.html")


@app.route("/history")
def history():
    runs = AuditRun.query.order_by(AuditRun.finished_at.desc()).limit(50).all()
    return render_template("history.html", runs=runs)


@app.route("/run", methods=["POST"])
def run_workflow():
    # validate inputs
    workflow_type = request.form.get("workflow_type")
    if workflow_type not in {"salto", "sesam"}:
        flash("Please select a valid workflow type.", "danger")
        return redirect(url_for("dashboard"))

    gold_file = request.files.get("gold_file")
    other_file = request.files.get("other_file")

    if not gold_file or gold_file.filename == "" or not allowed_file(gold_file.filename):
        flash("Please upload a valid GOLD file (Excel or CSV).", "danger")
        return redirect(url_for("dashboard"))

    if not other_file or other_file.filename == "" or not allowed_file(other_file.filename):
        flash("Please upload a valid Salto/Sesam file (Excel or CSV).", "danger")
        return redirect(url_for("dashboard"))

    # save uploads
    gold_path = _save_upload(gold_file, "gold")
    other_path = _save_upload(other_file, workflow_type)

    engine = AuditEngine()
    output_name = f"audit_result_{workflow_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, output_name)

    run_record = AuditRun(
        workflow_type=workflow_type,
        status="running",
        gold_file=gold_path,
        other_file=other_path,
        gold_filename=gold_file.filename,
        other_filename=other_file.filename,
        output_file=output_path,
        output_filename=output_name,
        started_at=datetime.utcnow(),
    )
    db.session.add(run_record)
    db.session.commit()

    try:
        if workflow_type == "salto":
            metadata, error_df = engine.run_salto_audit(gold_path, other_path, output_path)
        else:
            metadata, error_df = engine.run_sesam_audit(gold_path, other_path, output_path)

        run_record.status = "completed"
        run_record.finished_at = datetime.utcnow()
        run_record.summary_json = json.dumps(metadata or {})

        if error_df is None:
            error_records = []
        else:
            try:
                error_records = error_df.to_dict(orient="records") if not error_df.empty else []
            except Exception:
                error_records = []

        run_record.error_json = json.dumps(error_records)
        db.session.commit()

        return render_template(
            "result.html",
            output_file=output_name,
            workflow_type=workflow_type,
            generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        )

    except Exception as exc:
        run_record.status = "failed"
        run_record.finished_at = datetime.utcnow()
        db.session.commit()
        app.logger.exception("Audit run failed")
        flash(f"An error occurred while running the audit: {exc}", "danger")
        return redirect(url_for("dashboard"))


@app.route("/download/<path:filename>")
def download_file(filename: str):
    abs_output_dir = os.path.abspath(OUTPUT_DIR)
    abs_path = os.path.abspath(os.path.join(abs_output_dir, filename))

    if not abs_path.startswith(abs_output_dir + os.sep):
        abort(404)

    if not os.path.exists(abs_path):
        flash("File not found.", "danger")
        return redirect(url_for("dashboard"))

    return send_file(abs_path, as_attachment=True)


@app.route("/run/<int:run_id>/errors")
def run_errors(run_id: int):
    run = AuditRun.query.get_or_404(run_id)
    errors = json.loads(run.error_json or "[]")
    return render_template("errors.html", run=run, errors=errors)


@app.route("/api/run/<int:run_id>/json")
def run_json(run_id: int):
    run = AuditRun.query.get_or_404(run_id)
    payload = {
        "id": run.id,
        "workflow_type": run.workflow_type,
        "status": run.status,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "summary": json.loads(run.summary_json or "{}"),
        "errors": json.loads(run.error_json or "[]"),
    }
    return payload


def _start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(_schedule_latest_run, "cron", hour=1, minute=0)
    scheduler.start()


if __name__ == "__main__":
    _ensure_db()
    _start_scheduler()
    app.run(debug=True, port=5000)
