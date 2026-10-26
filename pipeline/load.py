"""Database reads and writes for the pipeline.

Every function runs inside the caller's transaction (one transaction per brand).
Rows are never deleted: missing products and variants are only marked inactive.
"""

import hashlib
import json
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def payload_hash(product: dict) -> str:
    """sha256 of the canonical JSON of one product, used to skip unchanged raw rows.

    `updated_at` (product and variants) is left out of the hash: several stores
    (Fugazee, Sullitt, NoughtOne, Urban Monkey, ...) set it to the request time on every call.
    The stored payload still keeps it.
    """
    stable = {k: v for k, v in product.items() if k != "updated_at"}
    stable["variants"] = [
        {k: v for k, v in variant.items() if k != "updated_at"}
        for variant in product.get("variants") or []
    ]
    canonical = json.dumps(stable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def get_active_brands(conn: psycopg.Connection) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, name, base_url FROM catalog.brands WHERE is_active ORDER BY id")
        return cur.fetchall()


def active_product_count(conn: psycopg.Connection, brand_id: int) -> int:
    row = conn.execute(
        "SELECT count(*) FROM catalog.products WHERE brand_id = %s AND is_active",
        (brand_id,),
    ).fetchone()
    return row[0]


def insert_raw(conn: psycopg.Connection, brand_id: int, products: list[dict], fetched_at: datetime) -> int:
    """Insert a raw row only for products whose payload changed since their latest raw row."""
    latest = dict(conn.execute(
        """
        SELECT DISTINCT ON (shopify_product_id) shopify_product_id, payload_hash
        FROM catalog.raw_products
        WHERE brand_id = %s
        ORDER BY shopify_product_id, fetched_at DESC, id DESC
        """,
        (brand_id,),
    ).fetchall())

    rows = []
    for product in products:
        digest = payload_hash(product)
        if latest.get(int(product["id"])) != digest:
            rows.append((brand_id, int(product["id"]), Jsonb(product), digest, fetched_at))

    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO catalog.raw_products
                    (brand_id, shopify_product_id, payload, payload_hash, fetched_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
    return len(rows)


UPSERT_PRODUCT = """
INSERT INTO catalog.products (
    brand_id, shopify_product_id, handle, title, product_type, category, tags, body_html,
    image_urls, options, is_gift_card, shopify_created_at, published_at, shopify_updated_at,
    is_active, missing_runs, first_seen_at, last_seen_at
) VALUES (
    %(brand_id)s, %(shopify_product_id)s, %(handle)s, %(title)s, %(product_type)s, %(category)s,
    %(tags)s, %(body_html)s, %(image_urls)s, %(options)s, %(is_gift_card)s,
    %(shopify_created_at)s, %(published_at)s, %(shopify_updated_at)s,
    true, 0, %(now)s, %(now)s
)
ON CONFLICT (brand_id, shopify_product_id) DO UPDATE SET
    handle             = EXCLUDED.handle,
    title              = EXCLUDED.title,
    product_type       = EXCLUDED.product_type,
    category           = EXCLUDED.category,
    tags               = EXCLUDED.tags,
    body_html          = EXCLUDED.body_html,
    image_urls         = EXCLUDED.image_urls,
    options            = EXCLUDED.options,
    is_gift_card       = EXCLUDED.is_gift_card,
    shopify_created_at = EXCLUDED.shopify_created_at,
    published_at       = EXCLUDED.published_at,
    shopify_updated_at = EXCLUDED.shopify_updated_at,
    is_active          = true,
    missing_runs       = 0,
    last_seen_at       = EXCLUDED.last_seen_at
RETURNING id
"""

# updated_at only moves when a value really changed, so a second identical run changes nothing.
UPSERT_VARIANT = """
INSERT INTO catalog.variants AS v (
    product_id, shopify_variant_id, position, title, option1, option2, option3,
    size_raw, size_norm, sku, price, compare_at_price, available, is_active,
    first_seen_at, updated_at
) VALUES (
    %(product_id)s, %(shopify_variant_id)s, %(position)s, %(title)s, %(option1)s, %(option2)s,
    %(option3)s, %(size_raw)s, %(size_norm)s, %(sku)s, %(price)s, %(compare_at_price)s,
    %(available)s, true, %(now)s, %(now)s
)
ON CONFLICT (product_id, shopify_variant_id) DO UPDATE SET
    position         = EXCLUDED.position,
    title            = EXCLUDED.title,
    option1          = EXCLUDED.option1,
    option2          = EXCLUDED.option2,
    option3          = EXCLUDED.option3,
    size_raw         = EXCLUDED.size_raw,
    size_norm        = EXCLUDED.size_norm,
    sku              = EXCLUDED.sku,
    price            = EXCLUDED.price,
    compare_at_price = EXCLUDED.compare_at_price,
    available        = EXCLUDED.available,
    is_active        = true,
    updated_at = CASE
        WHEN (v.position, v.title, v.option1, v.option2, v.option3, v.size_raw, v.size_norm,
              v.sku, v.price, v.compare_at_price, v.available, v.is_active)
             IS DISTINCT FROM
             (EXCLUDED.position, EXCLUDED.title, EXCLUDED.option1, EXCLUDED.option2,
              EXCLUDED.option3, EXCLUDED.size_raw, EXCLUDED.size_norm, EXCLUDED.sku,
              EXCLUDED.price, EXCLUDED.compare_at_price, EXCLUDED.available, true)
        THEN EXCLUDED.updated_at
        ELSE v.updated_at
    END
"""


def upsert_product(conn: psycopg.Connection, brand_id: int, product: dict, now: datetime) -> int:
    """Upsert one cleaned product and its variants. Returns catalog.products.id."""
    params = {k: v for k, v in product.items() if k != "variants"}
    params.update(brand_id=brand_id, options=Jsonb(product["options"]), now=now)
    product_id = conn.execute(UPSERT_PRODUCT, params).fetchone()[0]

    variants = product["variants"]
    if variants:
        with conn.cursor() as cur:
            cur.executemany(
                UPSERT_VARIANT,
                [{**v, "product_id": product_id, "now": now} for v in variants],
            )

    # Variants that disappeared from a product we did see -> inactive.
    conn.execute(
        """
        UPDATE catalog.variants
        SET is_active = false, updated_at = %(now)s
        WHERE product_id = %(product_id)s
          AND is_active
          AND NOT (shopify_variant_id = ANY(%(seen)s::bigint[]))
        """,
        {"product_id": product_id, "seen": [v["shopify_variant_id"] for v in variants], "now": now},
    )
    return product_id


def mark_missing_products(
    conn: psycopg.Connection, brand_id: int, seen_ids: list[int], missed_runs_limit: int
) -> tuple[int, int]:
    """Products of this brand not seen in this run: missing_runs += 1, and inactive once
    missing_runs reaches the limit. Returns (missing, newly_deactivated)."""
    rows = conn.execute(
        """
        UPDATE catalog.products
        SET missing_runs = LEAST(missing_runs + 1, 32767),
            is_active    = is_active AND missing_runs + 1 < %(limit)s
        WHERE brand_id = %(brand_id)s
          AND NOT (shopify_product_id = ANY(%(seen)s::bigint[]))
        RETURNING is_active, missing_runs
        """,
        {"brand_id": brand_id, "seen": seen_ids, "limit": missed_runs_limit},
    ).fetchall()
    newly_deactivated = sum(1 for is_active, runs in rows if not is_active and runs == missed_runs_limit)
    return len(rows), newly_deactivated
