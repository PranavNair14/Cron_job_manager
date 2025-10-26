# app.py  -- Updated CJM4 (Job Execution / Scheduler)
# Requirements: Flask, APScheduler, SQLAlchemy, croniter
# pip install flask apscheduler sqlalchemy croniter

import os
import shlex
import subprocess
import threading
from datetime import datetime, timedelta
import time
import logging

from flask import Flask, render_template, request, redirect, url_for, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError

from sqlalchemy import (create_engine, Column, Integer, String, DateTime,
                        Text, Boolean)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# ---------- Configuration ----------
DATABASE_URL = os.getenv("CJM_DB", "sqlite:///cjm.db")
MAX_RETRIES_DEFAULT = 3
RETRY_BACKOFF_SECONDS = 10  # base for exponential backoff

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cjm")

# ---------- Flask ----------
app = Flask(__name__)

# ---------- DB (SQLAlchemy simple ORM) ----------
Base = declarative_base()
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
db_lock = threading.Lock()  # tiny concurrency guard for sqlite in threads

class JobModel(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    command = Column(Text, nullable=False)
    schedule = Column(String(100), nullable=False)  # crontab-style string
    is_paused = Column(Boolean, default=False)
    max_retries = Column(Integer, default=MAX_RETRIES_DEFAULT)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_run_at = Column(DateTime, nullable=True)
    last_status = Column(String(50), nullable=True)  # success|failed|running
    retry_count = Column(Integer, default=0)

class JobLog(Base):
    __tablename__ = "job_logs"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, nullable=False)
    run_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50))
    stdout = Column(Text)
    stderr = Column(Text)
    attempt = Column(Integer, default=0)


Base.metadata.create_all(engine)

# ---------- Scheduler ----------
scheduler = BackgroundScheduler()
scheduler.start()

# Utility helpers
def db_session():
    # returns a new session
    return SessionLocal()

def validate_cron_expr(expr: str):
    """
    Validate crontab-style 5-field expression using APScheduler CronTrigger.from_crontab
    Raises ValueError if invalid.
    """
    try:
        CronTrigger.from_crontab(expr)
        return True
    except Exception as e:
        raise ValueError(f"Invalid cron expression: {e}")

def schedule_job_in_scheduler(job_row: JobModel):
    job_id = str(job_row.id)
    try:
        # Remove old job if exists
        try:
            scheduler.remove_job(job_id)
        except JobLookupError:
            pass

        if job_row.is_paused:
            logger.info("Job %s is paused; not scheduling in APScheduler.", job_id)
            return

        trigger = CronTrigger.from_crontab(job_row.schedule)
        scheduler.add_job(
            func=execute_job,
            trigger=trigger,
            args=[job_row.id],
            id=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        logger.info("Scheduled job %s -> %s", job_row.id, job_row.schedule)
    except Exception as e:
        logger.error("Failed to schedule job %s: %s", job_row.id, e)

def reschedule_all_jobs():
    s = db_session()
    try:
        rows = s.query(JobModel).all()
        for r in rows:
            schedule_job_in_scheduler(r)
    finally:
        s.close()

# Notification stub - wire real channels here (email/slack/webhook)
def send_notification(job_row: JobModel, title: str, message: str):
    # Placeholder according to SAD: Notification Service (Email/Slack/Webhook)
    # Implement actual integrations and templates later.
    logger.info("[NOTIFY] job=%s title=%s message=%s", job_row.id, title, message)
    # For example: call an external webhook or SMTP send (not implemented here)
    return True

def shell_run(command: str, timeout: int = 300):
    """
    Run a shell command safely. Returns (returncode, stdout, stderr)
    Uses subprocess with shlex.split to avoid shell-injection where possible.
    If command is intended to be a shell expression, the user must be trusted.
    """
    try:
        # split into args for safety. If user supplies a shell pipeline, it must run via shell=True.
        args = shlex.split(command)
        p = subprocess.run(args, capture_output=True, timeout=timeout, text=True)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as te:
        return -1, "", f"TimeoutExpired: {te}"
    except Exception as ex:
        return -1, "", f"Exception: {ex}"

def persist_log(job_id, status, stdout, stderr, attempt):
    s = db_session()
    try:
        log = JobLog(job_id=job_id, status=status, stdout=stdout, stderr=stderr, attempt=attempt)
        s.add(log)
        s.commit()
    finally:
        s.close()

def execute_job(job_id: int):
    """
    The actual executor invoked by APScheduler.
    Implements retry policy per job (max_retries) with exponential backoff.
    """
    logger.info("Executor triggered for job %s", job_id)
    # Acquire DB state
    with db_lock:
        s = db_session()
        job_row = s.query(JobModel).filter(JobModel.id == job_id).first()
        s.close()

    if not job_row:
        logger.error("Job %s not found in DB. Skipping execution.", job_id)
        return

    if job_row.is_paused:
        logger.info("Job %s is paused. Skipping execution.", job_id)
        return

    attempt = 0
    success = False

    # Save last_run_at and status = running
    with db_lock:
        s = db_session()
        r = s.query(JobModel).filter(JobModel.id == job_id).first()
        if r:
            r.last_run_at = datetime.utcnow()
            r.last_status = "running"
            s.commit()
        s.close()

    while attempt <= job_row.max_retries:
        attempt += 1
        logger.info("Job %s: attempt %s/%s", job_id, attempt, job_row.max_retries + 1)

        returncode, stdout, stderr = shell_run(job_row.command)

        status = "success" if returncode == 0 else "failed"
        persist_log(job_id, status, stdout, stderr, attempt)

        # update job-level metadata
        with db_lock:
            s = db_session()
            r = s.query(JobModel).filter(JobModel.id == job_id).first()
            if r:
                r.last_run_at = datetime.utcnow()
                r.last_status = status
                if status == "failed":
                    r.retry_count = (r.retry_count or 0) + 1
                else:
                    r.retry_count = 0
                s.commit()
            s.close()

        if returncode == 0:
            success = True
            send_notification(job_row, "Job Succeeded", f"Job {job_row.name} (id={job_id}) succeeded on attempt {attempt}.")
            break
        else:
            logger.warning("Job %s failed (attempt %s). stdout=%s stderr=%s", job_id, attempt, stdout[:200], stderr[:200])
            # If retries left, backoff then retry
            if attempt <= job_row.max_retries:
                backoff = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.info("Job %s: retrying after %s seconds", job_id, backoff)
                time.sleep(backoff)
            else:
                logger.error("Job %s exhausted retries. Marking as failed.", job_id)
                send_notification(job_row, "Job Failed", f"Job {job_row.name} (id={job_id}) failed after {attempt} attempts. See logs.")
                break

    return success

# ---------- Flask Routes ----------
@app.route("/")
def home():
    s = db_session()
    try:
        rows = s.query(JobModel).order_by(JobModel.id).all()
        # fetch next run info from scheduler
        jobs_display = []
        for r in rows:
            next_run = None
            try:
                job = scheduler.get_job(str(r.id))
                next_run_dt = job.next_run_time  # may be None if no schedule
                next_run = next_run_dt.isoformat() if next_run_dt else None
            except Exception:
                next_run = None
            jobs_display.append({
                "id": r.id,
                "name": r.name,
                "command": r.command,
                "schedule": r.schedule,
                "is_paused": r.is_paused,
                "last_run_at": r.last_run_at,
                "last_status": r.last_status,
                "retry_count": r.retry_count,
                "next_run": next_run,
                "max_retries": r.max_retries,
            })
    finally:
        s.close()
    return render_template("index.html", jobs=jobs_display)

@app.route("/create_job", methods=["POST"])
def create_job():
    name = request.form.get("name")
    command = request.form.get("command")
    schedule = request.form.get("schedule")
    max_retries = request.form.get("max_retries", type=int) or MAX_RETRIES_DEFAULT

    # Validate inputs
    if not (name and command and schedule):
        return "Missing required fields", 400

    try:
        validate_cron_expr(schedule)
    except ValueError as ve:
        return f"Cron validation error: {ve}", 400

    s = db_session()
    try:
        job = JobModel(name=name, command=command, schedule=schedule, max_retries=max_retries)
        s.add(job)
        s.commit()
        s.refresh(job)
        schedule_job_in_scheduler(job)
    finally:
        s.close()

    return redirect(url_for("home"))

@app.route("/edit_job/<int:job_id>", methods=["GET", "POST"])
def edit_job_route(job_id):
    s = db_session()
    try:
        job = s.query(JobModel).filter(JobModel.id == job_id).first()
        if not job:
            return "Job not found", 404

        if request.method == "POST":
            # update fields
            name = request.form.get("name")
            command = request.form.get("command")
            schedule = request.form.get("schedule")
            max_retries = request.form.get("max_retries", type=int) or job.max_retries

            if not (name and command and schedule):
                return "Missing required fields", 400

            try:
                validate_cron_expr(schedule)
            except ValueError as ve:
                return f"Cron validation error: {ve}", 400

            job.name = name
            job.command = command
            job.schedule = schedule
            job.max_retries = max_retries
            s.commit()
            # Reschedule
            schedule_job_in_scheduler(job)
            return redirect(url_for("home"))

        # GET -> render template (existing template expects a job object)
        # Convert to a simple object dict so template works
        template_job = {
            "id": job.id,
            "name": job.name,
            "command": job.command,
            "schedule": job.schedule,
            "max_retries": job.max_retries
        }
        return render_template("edit_job.html", job=template_job)
    finally:
        s.close()

@app.route("/delete_job/<int:job_id>", methods=["POST"])
def delete_job(job_id):
    s = db_session()
    try:
        job = s.query(JobModel).filter(JobModel.id == job_id).first()
        if not job:
            return "Not found", 404
        # remove from scheduler
        try:
            scheduler.remove_job(str(job_id))
        except JobLookupError:
            pass
        s.delete(job)
        s.commit()
    finally:
        s.close()
    return redirect(url_for("home"))

@app.route("/pause_job/<int:job_id>", methods=["POST"])
def pause_job(job_id):
    s = db_session()
    try:
        job = s.query(JobModel).filter(JobModel.id == job_id).first()
        if not job:
            return "Not found", 404
        job.is_paused = True
        s.commit()
        try:
            scheduler.remove_job(str(job_id))
        except JobLookupError:
            pass
    finally:
        s.close()
    return redirect(url_for("home"))

@app.route("/resume_job/<int:job_id>", methods=["POST"])
def resume_job(job_id):
    s = db_session()
    try:
        job = s.query(JobModel).filter(JobModel.id == job_id).first()
        if not job:
            return "Not found", 404
        job.is_paused = False
        s.commit()
        # reschedule
        schedule_job_in_scheduler(job)
    finally:
        s.close()
    return redirect(url_for("home"))

# API: get job status & logs
@app.route("/api/job/<int:job_id>", methods=["GET"])
def api_job(job_id):
    s = db_session()
    try:
        job = s.query(JobModel).filter(JobModel.id == job_id).first()
        if not job:
            return jsonify({"error": "not found"}), 404
        logs = s.query(JobLog).filter(JobLog.job_id == job_id).order_by(JobLog.run_at.desc()).limit(10).all()
        log_list = [{
            "run_at": l.run_at.isoformat(),
            "status": l.status,
            "stdout": l.stdout,
            "stderr": l.stderr,
            "attempt": l.attempt
        } for l in logs]
        next_run = None
        try:
            job_s = scheduler.get_job(str(job_id))
            if job_s and job_s.next_run_time:
                next_run = job_s.next_run_time.isoformat()
        except Exception:
            next_run = None

        return jsonify({
            "id": job.id,
            "name": job.name,
            "command": job.command,
            "schedule": job.schedule,
            "is_paused": job.is_paused,
            "last_run_at": job.last_run_at.isoformat() if job.last_run_at else None,
            "last_status": job.last_status,
            "retry_count": job.retry_count,
            "max_retries": job.max_retries,
            "next_run": next_run,
            "logs": log_list
        })
    finally:
        s.close()

# Startup: reschedule stored jobs
reschedule_all_jobs()

if __name__ == "__main__":
    # For production: run under a proper WSGI server (gunicorn/uvicorn) and enable TLS at proxy/load-balancer
    app.run(host="0.0.0.0", port=5000, debug=True)
