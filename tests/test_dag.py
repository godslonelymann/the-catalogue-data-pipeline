"""Parse with real Airflow; optional Dag.test uses an isolated metadata database."""

import os
from pathlib import Path

import pytest

pytest.importorskip('airflow.sdk', reason='Airflow is installed only in its separate environment')
from airflow.dag_processing.dagbag import DagBag

from pipeline import tasks

DAGS = Path(__file__).resolve().parent.parent / 'airflow' / 'dags'


@pytest.fixture
def dag(monkeypatch):
    monkeypatch.setattr(tasks, 'connect', lambda: pytest.fail('DAG parsing must not connect to the database'))
    bag = DagBag(dag_folder=str(DAGS), known_pools={'default_pool', 'catalogue_brands', 'catalogue_telegram'})
    assert not bag.import_errors
    return bag.dags['catalogue_daily']


def test_schedule_and_resource_limits(dag):
    assert dag.timezone.name == 'Asia/Kolkata'
    assert dag.timetable.expression == '30 2 * * *'
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 2
    assert not dag.catchup
    assert dag.get_task('notify').pool == dag.get_task('send_summary').pool == 'catalogue_telegram'


def test_reporting_cannot_hide_a_failed_brand(dag):
    assert dag.get_task('successful_brands').trigger_rule == 'all_done'
    assert dag.get_task('report').trigger_rule == 'all_done'
    guard = dag.get_task('check_outcome')
    assert guard.trigger_rule == 'none_failed'
    assert not guard.downstream_task_ids
    assert {'refresh_brand.save', 'notify', 'report', 'send_summary'} <= guard.upstream_task_ids
    assert dag.get_task('refresh_brand.fetch').task_group is dag.get_task('refresh_brand.save').task_group
    from airflow.sdk.definitions.taskgroup import MappedTaskGroup
    assert isinstance(dag.get_task('refresh_brand.fetch').task_group, MappedTaskGroup)


@pytest.mark.skipif(not os.environ.get('TEST_AIRFLOW_DATABASE_URL'), reason='isolated Airflow metadata DB not configured')
@pytest.mark.parametrize('mode', ['success', 'partial_failure', 'empty', 'discovery_failure', 'notify_failure'])
def test_real_scheduler_dependencies(dag, monkeypatch, mode):
    import pendulum
    from airflow.utils.state import DagRunState
    from airflow.configuration import conf
    from airflow import settings
    assert conf.get('database', 'sql_alchemy_conn') == os.environ['TEST_AIRFLOW_DATABASE_URL']
    monkeypatch.setenv('AIRFLOW__CORE__DAGS_FOLDER', str(DAGS))
    monkeypatch.setattr(settings, 'DAGS_FOLDER', str(DAGS))

    saved = []
    notices = []
    summaries = []
    brands = [{'id': 1}, {'id': 2}] if mode != 'empty' else []
    def discover(run_id):
        if mode == 'discovery_failure':
            raise RuntimeError('injected discovery failure')
        return brands
    def fetch(run_id, brand):
        if mode == 'partial_failure' and brand['id'] == 1:
            raise RuntimeError('injected fetch failure')
    def notify(run_id, brand_ids):
        notices.append(brand_ids)
        if mode == 'notify_failure':
            raise RuntimeError('injected notifier failure')
    monkeypatch.setattr(tasks, 'get_active_brands', discover)
    monkeypatch.setattr(tasks, 'fetch', fetch)
    monkeypatch.setattr(tasks, 'validate', lambda *args: None)
    monkeypatch.setattr(tasks, 'clean', lambda *args: None)
    monkeypatch.setattr(tasks, 'save', lambda run_id, brand: saved.append(brand['id']))
    monkeypatch.setattr(tasks, 'successful_brands', lambda run_id: sorted(saved))
    monkeypatch.setattr(tasks, 'notify', notify)
    monkeypatch.setattr(tasks, 'report', lambda run_id: {'ok': mode in ('success', 'empty'), 'text': mode})
    monkeypatch.setattr(tasks, 'send_summary', lambda run_id, report: summaries.append(report['text']))
    for task in dag.tasks:
        task.retries = 0
    result = dag.test(logical_date=pendulum.now('UTC'))
    assert result.state == (DagRunState.SUCCESS if mode in ('success', 'empty') else DagRunState.FAILED)
    assert summaries == [mode]
    assert notices == [sorted(saved)]
    if mode == 'partial_failure':
        assert saved == [2]
        states = {(t.task_id, t.map_index): t.state for t in result.get_task_instances()}
        assert states[('refresh_brand.fetch', 0)] == 'failed'
        assert states[('refresh_brand.save', 1)] == 'success'
