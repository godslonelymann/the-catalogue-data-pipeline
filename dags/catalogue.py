"""Schedule existing CLI jobs; importing this file never scrapes or sends alerts."""

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

PIPELINE_DIR = os.environ.get("CATALOGUE_PIPELINE_DIR", "/home/ubuntu/the-catalogue-data-pipeline")
BACKEND_DIR = os.environ.get("CATALOGUE_BACKEND_DIR", "/home/ubuntu/the-catalogue-backend")

# Each command reads its own repo's .env. Do not pass Airflow's database credentials
# or an inherited application DATABASE_URL into these subprocesses.
JOB_ENV = {
    "HOME": str(Path.home()),
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "PYTHONUNBUFFERED": "1",
}
DEFAULT_ARGS = {"owner": "the-catalogue", "retries": 0}

with DAG(
    dag_id="catalogue_daily",
    description="Refresh products, then send restock alerts after a successful refresh.",
    schedule=CronTriggerTimetable("30 2 * * *", timezone="Asia/Kolkata"),
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["catalogue"],
) as catalogue_daily:
    refresh_products = BashOperator(
        task_id="refresh_products",
        cwd=PIPELINE_DIR,
        bash_command="exec .venv/bin/python -m pipeline.run",
        env=JOB_ENV,
        append_env=False,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=1),
    )

    send_restock_alerts = BashOperator(
        task_id="send_restock_alerts",
        cwd=BACKEND_DIR,
        bash_command="exec .venv/bin/python -m notifier.check_alerts",
        env=JOB_ENV,
        append_env=False,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=1),
    )

    refresh_products >> send_restock_alerts

# A failed scraper must never suppress the daily database backup.
with DAG(
    dag_id="catalogue_backup",
    description="Back up the brands database independently of product refreshes.",
    schedule=CronTriggerTimetable("30 3 * * *", timezone="Asia/Kolkata"),
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["catalogue"],
) as catalogue_backup:
    backup_database = BashOperator(
        task_id="backup_database",
        cwd=PIPELINE_DIR,
        # Trailing space prevents Airflow treating the command as a Jinja .sh file.
        bash_command="exec bash scripts/backup.sh ",
        env=JOB_ENV,
        append_env=False,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )
