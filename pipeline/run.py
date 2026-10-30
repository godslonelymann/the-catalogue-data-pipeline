"""Daily pipeline entry point: python -m pipeline.run

For each active brand: extract -> safety check -> raw -> clean -> upsert -> mark missing.
Each brand runs in its own transaction, so one failing brand never stops the others.
At the end a summary goes to Telegram.
"""

import logging
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
import psycopg

from pipeline import changes, config, extract, load, rules

log = logging.getLogger("pipeline")


class SafetyCheckError(Exception):
    pass


@dataclass
class BrandResult:
    name: str
    ok: bool
    products: int = 0
    variants: int = 0
    raw_new: int = 0
    missing: int = 0
    deactivated: int = 0
    error: str | None = None


def dedupe_products(products: list[dict]) -> list[dict]:
    """A product can appear on two pages if the store changes during paging. Keep the first."""
    seen: set[int] = set()
    unique = []
    for p in products:
        if int(p["id"]) not in seen:
            seen.add(int(p["id"]))
            unique.append(p)
    return unique


def check_product_count(name: str, fetched: int, current_active: int) -> None:
    """Refuse the brand if this run has more than SAFETY_DROP fewer products than the DB."""
    if current_active and fetched < current_active * (1 - config.SAFETY_DROP):
        raise SafetyCheckError(
            f"only {fetched} products fetched, {current_active} active in DB "
            f"(drop > {config.SAFETY_DROP:.0%}), nothing changed"
        )


def process_brand(conn: psycopg.Connection, brand: dict, products: list[dict], now: datetime) -> BrandResult:
    """Load one brand's fetched products. All or nothing: one transaction."""
    products = dedupe_products(products)
    with conn.transaction():
        check_product_count(brand["name"], len(products), load.active_product_count(conn, brand["id"]))
        before = changes.snapshot(conn, brand["id"])

        raw_new = load.insert_raw(conn, brand["id"], products, now)
        cleaned = [rules.clean_product(p) for p in products]

        unknown_sizes: Counter[str] = Counter()
        for product in cleaned:
            load.upsert_product(conn, brand["id"], product, now)
            for v in product["variants"]:
                if v["size_raw"] is not None and not rules.is_known_size(v["size_norm"]):
                    unknown_sizes[v["size_norm"]] += 1

        missing, deactivated = load.mark_missing_products(
            conn, brand["id"], [p["shopify_product_id"] for p in cleaned], config.MISSED_RUNS_LIMIT
        )
        changes.insert(conn, changes.diff(before, changes.snapshot(conn, brand["id"]), now))

    if unknown_sizes:
        log.info("%s: unknown sizes (add aliases?): %s", brand["name"], dict(unknown_sizes.most_common(20)))
    return BrandResult(
        name=brand["name"],
        ok=True,
        products=len(cleaned),
        variants=sum(len(p["variants"]) for p in cleaned),
        raw_new=raw_new,
        missing=missing,
        deactivated=deactivated,
    )


def run_brand(conn: psycopg.Connection, client: httpx.Client, brand: dict, now: datetime) -> BrandResult:
    started = time.monotonic()
    try:
        products = extract.fetch_all_products(client, brand["base_url"])
        result = process_brand(conn, brand, products, now)
        log.info(
            "%s: ok, %d products, %d variants, %d raw changes, %d missing, %d deactivated (%.1f s)",
            result.name, result.products, result.variants, result.raw_new, result.missing, result.deactivated,
            time.monotonic() - started,
        )
        return result
    except SafetyCheckError as e:
        # Expected protection, not a bug: no stack trace, just the counts.
        log.warning("%s: skipped by safety check: %s (%.1f s)", brand["name"], e, time.monotonic() - started)
        return BrandResult(name=brand["name"], ok=False, error=f"safety check: {e}")
    except Exception as e:  # one brand must never stop the others
        log.exception("%s: failed after %.1f s", brand["name"], time.monotonic() - started)
        return BrandResult(name=brand["name"], ok=False, error=f"{type(e).__name__}: {e}")


def summary_text(results: list[BrandResult]) -> str:
    ok = [r for r in results if r.ok]
    failed = [r for r in results if not r.ok]
    icon = "✅" if not failed else "❌"
    lines = [
        f"{icon} The Catalogue pipeline: {sum(r.products for r in ok):,} products, "
        f"{sum(r.variants for r in ok):,} variants, {len(failed)} errors"
    ]
    for r in ok:
        line = f"• {r.name}: {r.products:,} products, {r.raw_new:,} changed"
        if r.deactivated:
            line += f", {r.deactivated} removed"
        lines.append(line)
    for r in failed:
        lines.append(f"❌ {r.name} failed: {(r.error or '')[:300]}")
    return "\n".join(lines)


def send_telegram(settings: config.Settings, text: str) -> None:
    if not (settings.telegram_bot_token and settings.telegram_chat_id):
        log.warning("Telegram is not configured, summary not sent")
        return
    try:
        response = httpx.post(
            f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
            json={"chat_id": settings.telegram_chat_id, "text": text},
            timeout=30,
        )
        response.raise_for_status()
    except httpx.HTTPError as e:
        # Do not log the exception text: the request URL contains the bot token.
        log.error("Telegram summary not sent: %s", type(e).__name__)


def main() -> int:
    settings = config.load_settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    now = datetime.now(timezone.utc)
    started = time.monotonic()
    log.info("run started")

    with psycopg.connect(settings.database_url, autocommit=True) as conn, extract.make_client() as client:
        brands = load.get_active_brands(conn)
        log.info("%d active brands", len(brands))
        results = [run_brand(conn, client, brand, now) for brand in brands]

    text = summary_text(results)
    log.info("summary:\n%s", text)
    send_telegram(settings, text)
    failed = [r.name for r in results if not r.ok]
    log.info("run finished in %.1f s, %d failed%s", time.monotonic() - started, len(failed), f": {failed}" if failed else "")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
