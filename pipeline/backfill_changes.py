"""Recover observed new/price/stock events from existing raw snapshots.

Run once after applying catalog_schema.sql: python -m pipeline.backfill_changes
Safe to repeat. Does not scrape stores, change current products, or send Telegram messages.
Historical removals cannot be reconstructed from snapshots of present products.
"""

import logging

import psycopg
from psycopg.rows import dict_row

from pipeline import changes, config, load, rules


def backfill_brand(conn, brand_id):
    current = changes.snapshot(conn, brand_id)
    identities = {p["shopify_product_id"]: p for p in current.values()}
    previous = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT shopify_product_id, payload, fetched_at FROM catalog.raw_products
               WHERE brand_id = %s ORDER BY fetched_at, id""",
            (brand_id,),
        )
        for raw in cur:
            identity = identities.get(raw["shopify_product_id"])
            if not identity:
                continue
            cleaned = rules.clean_product(raw["payload"])
            variant_ids = {v["shopify_variant_id"]: v["id"] for v in identity["variants"].values()}
            variants = {
                variant_ids[v["shopify_variant_id"]]: {
                    **v, "id": variant_ids[v["shopify_variant_id"]], "is_active": True,
                }
                for v in cleaned["variants"] if v["shopify_variant_id"] in variant_ids
            }
            old = previous.get(identity["id"])
            if old:
                for vid, v in old["variants"].items():
                    if vid not in variants:
                        variants[vid] = {**v, "is_active": False}
            product = {**identity, "is_active": True, "is_gift_card": cleaned["is_gift_card"],
                       "variants": variants}
            changes.insert(conn, changes.diff(previous, {identity["id"]: product}, raw["fetched_at"]))
            previous[identity["id"]] = product


def main():
    settings = config.load_settings()
    logging.basicConfig(level=logging.INFO)
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        for brand in load.get_active_brands(conn):
            with conn.transaction():
                backfill_brand(conn, brand["id"])
            logging.info("Recovered snapshot history for %s", brand["name"])


if __name__ == "__main__":
    main()
