"""Failure and retry tests use a throwaway PostgreSQL database, never production."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest

from pipeline import rules, run, staging, tasks
from tests.test_load import BRAND, T0, TEST_DATABASE_URL, conn, count, db, make_product

pytestmark = db
SQL = Path(__file__).resolve().parent.parent / 'sql' / 'airflow_pipeline_schema.sql'


@pytest.fixture
def staged(conn, monkeypatch):
    conn.execute('DROP SCHEMA IF EXISTS pipeline CASCADE')
    conn.execute(SQL.read_text())
    # Reapplying migrations must preserve checkpoints.
    conn.execute(SQL.read_text())
    staging.initialize(conn, 'r1', [BRAND])
    monkeypatch.setenv('DATABASE_URL', TEST_DATABASE_URL)
    monkeypatch.setattr(tasks, 'connect', lambda: psycopg.connect(TEST_DATABASE_URL, autocommit=True))
    yield conn
    conn.execute('DROP SCHEMA IF EXISTS pipeline CASCADE')


def prepare(conn, monkeypatch, products=None, run_id='r1', brand=BRAND):
    if products is None:
        products = [make_product(i) for i in range(1, 11)]
    monkeypatch.setattr(tasks.extract, 'fetch_all_products', lambda *args: products)
    tasks.fetch(run_id, brand)
    tasks.validate(run_id, brand)
    tasks.clean(run_id, brand)


def test_cleaning_checkpoint_preserves_existing_rules_and_exact_types(staged, monkeypatch):
    products = [make_product(1, price='999.51')]
    prepare(staged, monkeypatch, products)
    row = staging.read(staged, 'r1', BRAND['id'])
    assert staging.decode_cleaned(row['cleaned']) == [rules.clean_product(products[0])]
    tasks.save('r1', BRAND)
    assert tasks.successful_brands('r1') == [BRAND['id']]


def test_clean_retry_uses_same_fetch(staged, monkeypatch):
    calls = []
    monkeypatch.setattr(tasks.extract, 'fetch_all_products', lambda *args: calls.append(1) or [make_product(1)])
    tasks.fetch('r1', BRAND)
    tasks.validate('r1', BRAND)
    original = rules.clean_product
    monkeypatch.setattr(rules, 'clean_product', lambda p: (_ for _ in ()).throw(ValueError('injected')))
    with pytest.raises(ValueError):
        tasks.clean('r1', BRAND)
    assert staging.read(staged, 'r1', BRAND['id'])['failed_stage'] == 'clean'
    monkeypatch.setattr(rules, 'clean_product', original)
    tasks.fetch('r1', BRAND)  # even clearing fetch must retain the committed checkpoint
    tasks.clean('r1', BRAND)
    tasks.save('r1', BRAND)
    assert calls == [1]


def test_save_retry_cannot_repeat_missing_counts_or_history(staged, monkeypatch):
    run.process_brand(staged, BRAND, [make_product(i) for i in range(1, 11)], T0)
    prepare(staged, monkeypatch, [make_product(i) for i in range(1, 10)])
    tasks.save('r1', BRAND)
    before = staged.execute('SELECT * FROM catalog.product_changes ORDER BY id').fetchall()
    # Two independent workers racing on a cleared, completed save must both be no-ops.
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(lambda _: tasks.save('r1', BRAND), range(2)))
    assert count(staged, 'SELECT missing_runs FROM catalog.products WHERE shopify_product_id = 10') == 1
    assert staged.execute('SELECT * FROM catalog.product_changes ORDER BY id').fetchall() == before
    assert count(staged, 'SELECT count(*) FROM catalog.raw_products') == 10


def test_completion_and_catalogue_changes_rollback_together(staged, monkeypatch):
    prepare(staged, monkeypatch)
    original = run.process_brand
    def fail_after_catalogue(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError('failed before completion checkpoint')
    monkeypatch.setattr(run, 'process_brand', fail_after_catalogue)
    with pytest.raises(RuntimeError):
        tasks.save('r1', BRAND)
    assert count(staged, 'SELECT count(*) FROM catalog.products') == 0
    assert count(staged, 'SELECT count(*) FROM catalog.raw_products') == 0
    assert count(staged, 'SELECT count(*) FROM catalog.product_changes') == 0
    assert staging.read(staged, 'r1', BRAND['id'])['saved_at'] is None
    monkeypatch.setattr(run, 'process_brand', original)
    tasks.save('r1', BRAND)
    assert count(staged, 'SELECT count(*) FROM catalog.products') == 10


def test_save_rechecks_count_after_validation(staged, monkeypatch):
    prepare(staged, monkeypatch, [make_product(1)])
    run.process_brand(staged, BRAND, [make_product(i) for i in range(1, 11)], T0)
    with pytest.raises(run.SafetyCheckError):
        tasks.save('r1', BRAND)
    assert tasks.successful_brands('r1') == []
    assert count(staged, 'SELECT count(*) FROM catalog.products WHERE missing_runs = 0') == 10


def test_one_failed_brand_does_not_block_another(staged, monkeypatch):
    other = {'id': 1, 'name': 'Other', 'base_url': 'https://other.example'}
    staging.initialize(staged, 'r2', [BRAND, other])
    def fetch(client, url):
        if url == BRAND['base_url']:
            raise RuntimeError('store unavailable')
        return [make_product(1)]
    monkeypatch.setattr(tasks.extract, 'fetch_all_products', fetch)
    with pytest.raises(RuntimeError):
        tasks.fetch('r2', BRAND)
    tasks.fetch('r2', other)
    tasks.validate('r2', other)
    tasks.clean('r2', other)
    tasks.save('r2', other)
    assert tasks.successful_brands('r2') == [1]
    report = tasks.report('r2')
    assert not report['ok'] and 'store unavailable' in report['text']


def test_manifest_is_stable_on_retry_and_empty_runs_are_valid(staged):
    assert staging.initialize(staged, 'r1', []) == [BRAND]
    assert staging.initialize(staged, 'empty', []) == []
    assert staging.initialize(staged, 'empty', [BRAND]) == []
    # Reporting failed discovery must not prevent a subsequent discovery retry.
    staged.execute("INSERT INTO pipeline.runs (run_id) VALUES ('discovery-retry')")
    assert staging.initialize(staged, 'discovery-retry', [BRAND]) == [BRAND]


def test_stale_unfinished_snapshot_cannot_overwrite_newer_save(staged, monkeypatch):
    prepare(staged, monkeypatch)
    staged.execute("UPDATE pipeline.brand_runs SET fetched_at = %s WHERE run_id = 'r1'", (T0,))
    staging.initialize(staged, 'r2', [BRAND])
    prepare(staged, monkeypatch, run_id='r2')
    tasks.save('r2', BRAND)
    with pytest.raises(ValueError, match='newer snapshot'):
        tasks.save('r1', BRAND)


def test_notifier_scope_is_rechecked_against_committed_saves(staged, monkeypatch):
    from notifier import check_alerts
    prepare(staged, monkeypatch)
    tasks.save('r1', BRAND)
    monkeypatch.setenv('ALERT_DATABASE_URL', 'unused')
    seen = []
    def deliver(*args, **kwargs):
        seen.append(kwargs['brand_ids'])
        return {'sent': 0, 'failed': 0, 'uncertain': 0}
    monkeypatch.setattr(check_alerts, 'deliver_alerts', deliver)
    tasks.notify('r1', [BRAND['id'], 1])
    assert seen == [[BRAND['id']]]
    assert tasks.report('r1')['ok']


def test_uncertain_summary_is_not_sent_twice(staged, monkeypatch):
    import httpx
    from pipeline.config import Settings
    monkeypatch.setattr(tasks.config, 'load_settings', lambda: Settings('unused', 'TOKEN', '42'))
    calls = []
    def timeout(*args, **kwargs):
        calls.append(1)
        raise httpx.ReadTimeout('uncertain')
    monkeypatch.setattr(httpx, 'post', timeout)
    with pytest.raises(RuntimeError, match='summary delivery failed'):
        tasks.send_summary('r1', {'text': 'summary'})
    with pytest.raises(RuntimeError, match='uncertain'):
        tasks.send_summary('r1', {'text': 'summary'})
    assert calls == [1]
