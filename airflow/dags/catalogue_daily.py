"""Daily catalogue refresh. Importing this module performs no network/DB work."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

from pipeline import tasks


def run_id():
    return get_current_context()["run_id"]


@dag(
    dag_id="catalogue_daily",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5),
                  "retry_exponential_backoff": True, "execution_timeout": timedelta(hours=2)},
    tags=["catalogue"],
)
def catalogue_daily():
    @task
    def active_brands():
        return tasks.get_active_brands(run_id())

    @task(pool="catalogue_brands")
    def fetch(brand):
        tasks.fetch(run_id(), brand)

    @task(pool="catalogue_brands")
    def validate(brand):
        tasks.validate(run_id(), brand)

    @task(pool="catalogue_brands")
    def clean(brand):
        tasks.clean(run_id(), brand)

    @task(pool="catalogue_brands")
    def save(brand):
        tasks.save(run_id(), brand)

    @task_group
    def refresh_brand(brand):
        fetched = fetch(brand)
        validated = validate(brand)
        cleaned = clean(brand)
        saved = save(brand)
        fetched >> validated >> cleaned >> saved

    @task(trigger_rule="all_done")
    def successful_brands():
        return tasks.successful_brands(run_id())

    @task(pool="catalogue_telegram")
    def notify(brand_ids):
        return tasks.notify(run_id(), brand_ids)

    @task(trigger_rule="all_done")
    def report():
        return tasks.report(run_id())

    @task(pool="catalogue_telegram")
    def send_summary(result):
        tasks.send_summary(run_id(), result)

    @task(trigger_rule="none_failed", retries=0)
    def check_outcome(result):
        # Direct dependencies also preserve failures that occurred outside task code.
        if not result["ok"]:
            raise RuntimeError("catalogue run has failures; see report and failed tasks")

    brands = active_brands()
    refreshed = refresh_brand.expand(brand=brands)
    successful = successful_brands()
    brands >> successful
    refreshed >> successful
    notified = notify(successful)
    reported = report()
    [brands, successful, notified] >> reported
    summary = send_summary(reported)
    checked = check_outcome(reported)
    # ALL_DONE reporting must not turn a run with a failed brand into success.
    refreshed >> checked
    [brands, successful, notified, summary] >> checked


catalogue_daily()
