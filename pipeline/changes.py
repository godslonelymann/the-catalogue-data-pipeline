"""Record observed product changes. Metadata edits do not become shopping updates."""

from psycopg.rows import dict_row


def snapshot(conn, brand_id):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT id, shopify_product_id, is_active, is_gift_card FROM catalog.products WHERE brand_id = %s",
            (brand_id,),
        )
        products = {p["id"]: {**p, "variants": {}} for p in cur.fetchall()}
        cur.execute(
            """SELECT v.id, v.product_id, v.shopify_variant_id, v.title, v.size_norm,
                      v.price, v.available, v.is_active
               FROM catalog.variants v JOIN catalog.products p ON p.id = v.product_id
               WHERE p.brand_id = %s""",
            (brand_id,),
        )
        for v in cur.fetchall():
            products[v["product_id"]]["variants"][v["id"]] = v
    return products


def diff(before, after, observed_at):
    """Compare stable DB identities and Decimal prices, including size-specific stock."""
    events = []
    for pid, product in after.items():
        if product["is_gift_card"]:
            continue
        previous = before.get(pid)

        def add(kind, variant=None, old=None):
            label = None
            if variant:
                label = variant["title"]
                if not label or label == "Default Title":
                    label = variant["size_norm"] or "One size"
            events.append((pid, variant["id"] if variant else None, kind, label,
                           old["price"] if old else None,
                           variant["price"] if variant else None, observed_at))

        if previous is None:
            if product["is_active"]:
                add("new")
            continue
        if not product["is_active"]:
            if previous["is_active"]:
                add("removed")
            continue
        if not previous["is_active"]:
            add("relisted")
        for vid, v in product["variants"].items():
            old = previous["variants"].get(vid)
            if not old:
                continue  # a newly introduced size is not a restock
            if v["is_active"] and old["is_active"] and v["price"] != old["price"]:
                add("price_drop" if v["price"] < old["price"] else "price_increase", v, old)
            was_available = previous["is_active"] and old["is_active"] and old["available"]
            is_available = v["is_active"] and v["available"]
            if is_available and not was_available:
                add("restock", v, old)
            elif was_available and not is_available:
                add("sold_out", v, old)
    return events


def insert(conn, events):
    if events:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO catalog.product_changes
                       (product_id, variant_id, event_type, variant_label, old_price, new_price, observed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING""",
                events,
            )
