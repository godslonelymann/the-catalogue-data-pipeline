"""Durable per-run checkpoints. Product payloads never travel through XCom."""

import json
from datetime import datetime
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def encode_cleaned(products: list[dict]) -> list[dict]:
    def encode(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        raise TypeError(type(value).__name__)

    return json.loads(json.dumps(products, default=encode))


def decode_cleaned(products: list[dict]) -> list[dict]:
    # Copy the staged data; never change an in-memory checkpoint while decoding.
    products = json.loads(json.dumps(products))
    for product in products:
        for field in ("shopify_created_at", "published_at", "shopify_updated_at"):
            if product[field] is not None:
                product[field] = datetime.fromisoformat(product[field])
        for variant in product["variants"]:
            for field in ("price", "compare_at_price"):
                if variant[field] is not None:
                    variant[field] = Decimal(variant[field])
    return products


def initialize(conn: psycopg.Connection, run_id: str, brands: list[dict]) -> list[dict]:
    """Persist the brand manifest once, including a run with zero active brands."""
    with conn.transaction():
        conn.execute(
            "INSERT INTO pipeline.runs (run_id) VALUES (%s) ON CONFLICT DO NOTHING RETURNING run_id",
            (run_id,),
        )
        # Lock even on retry: another discovery attempt may still be committing.
        ready = conn.execute("SELECT manifest_ready FROM pipeline.runs WHERE run_id = %s FOR UPDATE", (run_id,)).fetchone()[0]
        if not ready:
            for brand in brands:
                conn.execute(
                    "INSERT INTO pipeline.brand_runs (run_id, brand_id, brand) VALUES (%s, %s, %s)",
                    (run_id, brand["id"], Jsonb(brand)),
                )
            conn.execute("UPDATE pipeline.runs SET manifest_ready = true WHERE run_id = %s", (run_id,))
        return [row[0] for row in conn.execute(
            "SELECT brand FROM pipeline.brand_runs WHERE run_id = %s ORDER BY brand_id", (run_id,),
        ).fetchall()]


def read(conn: psycopg.Connection, run_id: str, brand_id: int, *, lock=False) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM pipeline.brand_runs WHERE run_id = %s AND brand_id = %s"
            + (" FOR UPDATE" if lock else ""), (run_id, brand_id),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"brand {brand_id} has no manifest for run {run_id}")
    return row


def record_failure(conn: psycopg.Connection, run_id: str, brand_id: int, stage: str, error: Exception):
    conn.execute(
        "UPDATE pipeline.brand_runs SET failed_stage = %s, error = %s "
        "WHERE run_id = %s AND brand_id = %s AND saved_at IS NULL",
        (stage, f"{type(error).__name__}: {error}"[:1000], run_id, brand_id),
    )


def results(conn: psycopg.Connection, run_id: str) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        # Reporting must not load every brand's three potentially large payloads.
        cur.execute(
            "SELECT brand_id, brand, saved_at, result, failed_stage, error "
            "FROM pipeline.brand_runs WHERE run_id = %s ORDER BY brand_id", (run_id,),
        )
        return cur.fetchall()
