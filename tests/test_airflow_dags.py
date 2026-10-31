"""Optional integration checks against real Airflow; no scraping or DB writes."""

import importlib.util
import json
import shlex
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk", reason="Install deploy/airflow/requirements.txt to check DAGs")


@pytest.fixture
def workflows(monkeypatch, tmp_path):
    monkeypatch.setenv("CATALOGUE_PIPELINE_DIR", str(tmp_path / "pipeline checkout"))
    monkeypatch.setenv("CATALOGUE_BACKEND_DIR", str(tmp_path / "backend checkout"))
    source = Path(__file__).resolve().parents[1] / "dags" / "catalogue.py"
    spec = importlib.util.spec_from_file_location("catalogue_dags", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_alerts_require_a_successful_refresh(workflows):
    refresh = workflows.catalogue_daily.get_task("refresh_products")
    alerts = workflows.catalogue_daily.get_task("send_restock_alerts")
    assert alerts.upstream_task_ids == {refresh.task_id}
    assert alerts.trigger_rule == "all_success"
    assert refresh.cwd.endswith("pipeline checkout")
    assert alerts.cwd.endswith("backend checkout")


def test_no_retries_backlogs_or_overlapping_runs(workflows):
    for dag in (workflows.catalogue_daily, workflows.catalogue_backup):
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert dag.is_paused_upon_creation is True
        for task in dag.tasks:
            assert task.retries == 0
            assert task.append_env is False
            assert "DATABASE_URL" not in task.env
            assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" not in task.env
            assert task.do_xcom_push is False


def test_schedules_use_ist_and_backup_is_independent(workflows):
    from airflow.timetables.base import TimeRestriction

    # On Nov 2 at midnight IST, the next triggers must be that morning's jobs.
    earliest = workflows.pendulum.datetime(2026, 11, 2, tz="Asia/Kolkata")
    restriction = TimeRestriction(earliest=earliest, latest=None, catchup=True)
    for dag, hour in ((workflows.catalogue_daily, 2), (workflows.catalogue_backup, 3)):
        info = dag.timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
        assert info.run_after.in_timezone("Asia/Kolkata") == earliest.add(hours=hour, minutes=30)
    backup = workflows.catalogue_backup.get_task("backup_database")
    assert backup.upstream_task_ids == set()
    assert len(workflows.catalogue_backup.tasks) == 1


def test_command_uses_checkout_and_excludes_inherited_secrets(workflows, monkeypatch):
    task = workflows.catalogue_daily.get_task("refresh_products")
    executable = Path(task.cwd) / ".venv" / "bin" / "python"
    executable.parent.mkdir(parents=True)
    probe = (
        "import json, os, sys; print(json.dumps({'cwd': os.getcwd(), "
        "'args': sys.argv[1:], 'database': os.getenv('DATABASE_URL'), "
        "'telegram': os.getenv('TELEGRAM_BOT_TOKEN'), "
        "'metadata': os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')}))"
    )
    executable.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} -c {shlex.quote(probe)} \"$@\"\n")
    executable.chmod(0o700)
    monkeypatch.setenv("DATABASE_URL", "wrong-database")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "wrong-token")
    monkeypatch.setenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "private-metadata")

    result = json.loads(task.execute(context={}))
    assert Path(result["cwd"]).resolve() == Path(task.cwd).resolve()
    assert result["args"] == ["-m", "pipeline.run"]
    assert result["database"] is None
    assert result["telegram"] is None
    assert result["metadata"] is None


def test_failed_command_is_reported_as_failure(workflows):
    from airflow.exceptions import AirflowException

    task = workflows.catalogue_daily.get_task("refresh_products")
    executable = Path(task.cwd) / ".venv" / "bin" / "python"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\nexit 1\n")
    executable.chmod(0o700)
    with pytest.raises(AirflowException, match="non-zero exit code 1"):
        task.execute(context={})


def test_backup_shell_script_is_not_treated_as_a_template(workflows):
    task = workflows.catalogue_backup.get_task("backup_database")
    script = Path(task.cwd) / "scripts" / "backup.sh"
    script.parent.mkdir(parents=True)
    script.write_text("printf 'dummy backup completed\\n'\n")
    task.render_template_fields(context={})
    assert task.execute(context={}) == "dummy backup completed"
