"""Tests for loading (pipeline/load.py + pipeline/run.py).

The pure tests always run. The database tests run only when TEST_DATABASE_URL points at an
empty, throwaway database (they drop and recreate the catalog schema). Use an admin user.
"""

import copy
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from pipeline import load, run

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
BRAND = {"id": 4, "name": "Sullitt", "base_url": "https://sullitt.com"}
T0 = datetime(2026, 10, 1, 21, 0, tzinfo=timezone.utc)


def make_product(pid, n_variants=2, available=True, price="999.00", compare_at="1499.00"):
    sizes = ["S", "M", "L", "XL"][:n_variants]
    return {
        "id": pid,
        "title": f"Tee {pid}",
        "handle": f"tee-{pid}",
        "product_type": "T-Shirts",
        "tags": ["new"],
        "body_html": "<p>x</p>",
        "images": [{"src": f"https://cdn.shopify.com/{pid}.jpg"}],
        "options": [{"name": "Size", "position": 1, "values": sizes}],
        "variants": [
            {"id": pid * 10 + i, "title": s, "option1": s, "option2": None, "option3": None,
             "price": price, "compare_at_price": compare_at, "available": available,
             "position": i + 1, "sku": ""}
            for i, s in enumerate(sizes)
        ],
        "created_at": "2026-09-01T10:00:00+05:30",
        "published_at": "2026-09-01T10:00:00+05:30",
        "updated_at": "2026-09-02T10:00:00+05:30",
    }


# ---------------------------------------------------------------- pure

def test_payload_hash_ignores_key_order():
    assert load.payload_hash({"a": 1, "b": [1, 2]}) == load.payload_hash({"b": [1, 2], "a": 1})
    assert load.payload_hash({"a": 1}) != load.payload_hash({"a": 2})


def test_payload_hash_ignores_updated_at():
    # Some stores set updated_at to the request time on every call.
    a = make_product(1)
    b = copy.deepcopy(a)
    b["updated_at"] = "2026-10-27T00:19:00+05:30"
    b["variants"][0]["updated_at"] = "2026-10-27T00:19:00+05:30"
    assert load.payload_hash(a) == load.payload_hash(b)
    b["variants"][0]["available"] = False
    assert load.payload_hash(a) != load.payload_hash(b)


def test_dedupe_products_keeps_first():
    products = [{"id": 1, "x": "first"}, {"id": 2}, {"id": 1, "x": "second"}]
    assert run.dedupe_products(products) == [{"id": 1, "x": "first"}, {"id": 2}]


def test_check_product_count():
    run.check_product_count("x", 0, 0)       # first run, nothing in DB yet
    run.check_product_count("x", 80, 100)    # exactly 20% lower is allowed
    with pytest.raises(run.SafetyCheckError):
        run.check_product_count("x", 79, 100)


def test_summary_text():
    text = run.summary_text([
        run.BrandResult("Genrage", True, products=248, variants=1169, raw_new=3),
        run.BrandResult("Farak", False, error="ExtractError: timeout"),
    ])
    assert text.startswith("❌")
    assert "248 products" in text
    assert "Farak failed: ExtractError: timeout" in text
    assert run.summary_text([run.BrandResult("Genrage", True, products=1)]).startswith("✅")


# ---------------------------------------------------------------- database

db = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL not set")


@pytest.fixture
def conn():
    import psycopg

    with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as c:
        c.execute("DROP SCHEMA IF EXISTS catalog CASCADE")
        c.execute((SQL_DIR / "catalog_schema.sql").read_text())
        c.execute((SQL_DIR / "seed_brands.sql").read_text())
        yield c


def count(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


@db
def test_first_run_loads_everything(conn):
    products = [make_product(i) for i in range(1, 11)]
    result = run.process_brand(conn, BRAND, products, T0)
    assert (result.products, result.variants, result.raw_new) == (10, 20, 10)
    assert count(conn, "SELECT count(*) FROM catalog.products WHERE brand_id = 4 AND is_active") == 10
    assert count(conn, "SELECT count(*) FROM catalog.variants") == 20
    row = conn.execute(
        "SELECT price, compare_at_price, discount_pct, size_norm FROM catalog.variants "
        "WHERE shopify_variant_id = 10"
    ).fetchone()
    assert row == (Decimal("999.00"), Decimal("1499.00"), Decimal("33.4"), "S")


@db
def test_second_identical_run_changes_nothing(conn):
    products = [make_product(i) for i in range(1, 11)]
    run.process_brand(conn, BRAND, products, T0)
    before = conn.execute("SELECT * FROM catalog.variants ORDER BY id").fetchall()

    result = run.process_brand(conn, BRAND, copy.deepcopy(products), T0 + timedelta(days=1))
    assert result.raw_new == 0
    assert count(conn, "SELECT count(*) FROM catalog.raw_products") == 10
    assert conn.execute("SELECT * FROM catalog.variants ORDER BY id").fetchall() == before


@db
def test_changed_product_adds_raw_row_and_updates_variant(conn):
    run.process_brand(conn, BRAND, [make_product(1, available=False)], T0)
    result = run.process_brand(conn, BRAND, [make_product(1, available=True)], T0 + timedelta(days=1))
    assert result.raw_new == 1
    assert count(conn, "SELECT count(*) FROM catalog.raw_products") == 2
    assert count(conn, "SELECT count(*) FROM catalog.variants WHERE available "
                       "AND updated_at = %s", T0 + timedelta(days=1)) == 2


@db
def test_safety_check_changes_nothing(conn):
    run.process_brand(conn, BRAND, [make_product(i) for i in range(1, 11)], T0)
    with pytest.raises(run.SafetyCheckError):
        run.process_brand(conn, BRAND, [make_product(i) for i in range(1, 8)], T0 + timedelta(days=1))
    assert count(conn, "SELECT count(*) FROM catalog.products WHERE is_active AND missing_runs = 0") == 10


@db
def test_safety_check_is_a_skip_not_a_crash(conn, monkeypatch, caplog):
    run.process_brand(conn, BRAND, [make_product(i) for i in range(1, 11)], T0)
    monkeypatch.setattr(run.extract, "fetch_all_products", lambda client, url: [make_product(1)])

    result = run.run_brand(conn, None, BRAND, T0 + timedelta(days=1))

    assert not result.ok and result.error.startswith("safety check:")
    skip = [r for r in caplog.records if "skipped by safety check" in r.getMessage()]
    assert len(skip) == 1 and skip[0].levelname == "WARNING" and skip[0].exc_info is None


@db
def test_missing_product_deactivated_after_two_runs(conn):
    run.process_brand(conn, BRAND, [make_product(i) for i in range(1, 11)], T0)
    nine = [make_product(i) for i in range(1, 10)]

    r1 = run.process_brand(conn, BRAND, nine, T0 + timedelta(days=1))
    assert (r1.missing, r1.deactivated) == (1, 0)
    assert conn.execute("SELECT is_active, missing_runs FROM catalog.products "
                        "WHERE shopify_product_id = 10").fetchone() == (True, 1)

    r2 = run.process_brand(conn, BRAND, nine, T0 + timedelta(days=2))
    assert (r2.missing, r2.deactivated) == (1, 1)
    assert conn.execute("SELECT is_active, missing_runs FROM catalog.products "
                        "WHERE shopify_product_id = 10").fetchone() == (False, 2)

    # It comes back: active again, counter reset. Rows are never deleted.
    run.process_brand(conn, BRAND, [make_product(i) for i in range(1, 11)], T0 + timedelta(days=3))
    assert conn.execute("SELECT is_active, missing_runs FROM catalog.products "
                        "WHERE shopify_product_id = 10").fetchone() == (True, 0)


@db
def test_removed_variant_becomes_inactive(conn):
    run.process_brand(conn, BRAND, [make_product(1, n_variants=3)], T0)
    run.process_brand(conn, BRAND, [make_product(1, n_variants=2)], T0 + timedelta(days=1))
    rows = conn.execute("SELECT size_norm, is_active FROM catalog.variants ORDER BY position").fetchall()
    assert rows == [("S", True), ("M", True), ("L", False)]


@db
def test_failed_brand_rolls_back(conn):
    bad = make_product(2)
    bad["variants"][0]["price"] = None  # cleaning fails half way through the brand
    with pytest.raises(ValueError):
        run.process_brand(conn, BRAND, [make_product(1), bad], T0)
    assert count(conn, "SELECT count(*) FROM catalog.products") == 0
    assert count(conn, "SELECT count(*) FROM catalog.raw_products") == 0


@db
def test_db_check_rejects_fake_sale_price(conn):
    import psycopg

    run.process_brand(conn, BRAND, [make_product(1)], T0)
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute("UPDATE catalog.variants SET compare_at_price = price WHERE shopify_variant_id = 10")
