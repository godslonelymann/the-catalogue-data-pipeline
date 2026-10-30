"""Change recording uses real transitions, stable variants, and exact prices."""

import copy
from datetime import timedelta
from decimal import Decimal

import pytest

from pipeline import changes, run
from pipeline.backfill_changes import backfill_brand
from tests.test_load import BRAND, T0, conn, count, db, make_product


def state(*, active=True, available=True, price="999.00", variant_active=True):
    return {1: {"id": 1, "is_active": active, "is_gift_card": False, "variants": {
        11: {"id": 11, "title": "M / Black", "size_norm": "M", "price": Decimal(price),
             "available": available, "is_active": variant_active},
    }}}


def kinds(events):
    return [e[2] for e in events]


def test_diff_does_not_call_new_sizes_restocks():
    before = state()
    after = copy.deepcopy(before)
    after[1]["variants"][12] = {**after[1]["variants"][11], "id": 12}
    assert changes.diff(before, after, T0) == []


def test_diff_compares_actual_prices_at_variant_level():
    events = changes.diff(state(available=False, price="999.50"), state(price="899.25"), T0)
    assert kinds(events) == ["price_drop", "restock"]
    assert events[0][1:6] == (11, "price_drop", "M / Black", Decimal("999.50"), Decimal("899.25"))
    assert kinds(changes.diff(state(price="899.25"), state(price="999.50"), T0)) == ["price_increase"]


def test_removed_is_not_sold_out_and_returning_products_are_relisted():
    assert kinds(changes.diff(state(), state(active=False), T0)) == ["removed"]
    assert changes.diff(state(active=False), state(active=False), T0) == []
    assert kinds(changes.diff(state(active=False), state(), T0)) == ["relisted", "restock"]
    assert kinds(changes.diff(state(), state(variant_active=False), T0)) == ["sold_out"]


def test_new_product_has_one_event_and_gift_cards_have_none():
    assert kinds(changes.diff({}, state(), T0)) == ["new"]
    gift = state()
    gift[1]["is_gift_card"] = True
    assert changes.diff({}, gift, T0) == []


@db
def test_pipeline_records_changes_once_and_ignores_description_edits(conn):
    product = make_product(1, available=False)
    run.process_brand(conn, BRAND, [product], T0)
    assert count(conn, "SELECT count(*) FROM catalog.product_changes WHERE event_type = 'new'") == 1
    product["body_html"] = "<p>Updated description</p>"
    run.process_brand(conn, BRAND, [product], T0 + timedelta(hours=1))
    assert count(conn, "SELECT count(*) FROM catalog.product_changes") == 1
    for v in product["variants"]:
        v.update(price="799.50", available=True)
    run.process_brand(conn, BRAND, [product], T0 + timedelta(days=1))
    assert count(conn, "SELECT count(*) FROM catalog.product_changes WHERE event_type = 'price_drop'") == 2
    assert count(conn, "SELECT count(*) FROM catalog.product_changes WHERE event_type = 'restock'") == 2
    run.process_brand(conn, BRAND, [product], T0 + timedelta(days=1, hours=1))
    assert count(conn, "SELECT count(*) FROM catalog.product_changes") == 5


@db
def test_change_recording_rolls_back_with_brand_transaction(conn, monkeypatch):
    def fail(*args):
        raise RuntimeError("injected failure")
    monkeypatch.setattr(changes, "insert", fail)
    with pytest.raises(RuntimeError):
        run.process_brand(conn, BRAND, [make_product(1)], T0)
    assert count(conn, "SELECT count(*) FROM catalog.products") == 0
    assert count(conn, "SELECT count(*) FROM catalog.raw_products") == 0


@db
def test_backfill_recovers_same_events_without_duplicates(conn):
    product = make_product(1, available=False)
    run.process_brand(conn, BRAND, [product], T0)
    product["variants"][0].update(price="899.50", available=True)
    run.process_brand(conn, BRAND, [product], T0 + timedelta(days=1))
    # Use a transaction rollback to emulate the event table being empty on upgrade.
    with conn.transaction(force_rollback=True):
        conn.execute("DELETE FROM catalog.product_changes")
        backfill_brand(conn, BRAND["id"])
        assert count(conn, "SELECT count(*) FROM catalog.product_changes") == 3
        backfill_brand(conn, BRAND["id"])
        assert count(conn, "SELECT count(*) FROM catalog.product_changes") == 3


@db
def test_failed_fetch_does_not_create_removal_events(conn):
    run.process_brand(conn, BRAND, [make_product(i) for i in range(10)], T0)
    with pytest.raises(run.SafetyCheckError):
        run.process_brand(conn, BRAND, [], T0 + timedelta(days=1))
    assert count(conn, "SELECT count(*) FROM catalog.product_changes WHERE event_type = 'removed'") == 0


@db
def test_removal_is_logged_once_after_two_successful_missing_checks(conn):
    products = [make_product(i) for i in range(10)]
    run.process_brand(conn, BRAND, products, T0)
    run.process_brand(conn, BRAND, products[:-1], T0 + timedelta(days=1))
    assert count(conn, "SELECT count(*) FROM catalog.product_changes WHERE event_type = 'removed'") == 0
    run.process_brand(conn, BRAND, products[:-1], T0 + timedelta(days=2))
    run.process_brand(conn, BRAND, products[:-1], T0 + timedelta(days=3))
    assert count(conn, "SELECT count(*) FROM catalog.product_changes WHERE event_type = 'removed'") == 1
