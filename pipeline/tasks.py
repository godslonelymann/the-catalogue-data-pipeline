"""Airflow-independent tasks; every call opens its own catalogue connection."""

import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from pipeline import config, extract, load, rules, run, staging

log = logging.getLogger(__name__)


def connect():
    return psycopg.connect(config.load_settings().database_url, autocommit=True)


def get_active_brands(run_id: str) -> list[dict]:
    with connect() as conn:
        return staging.initialize(conn, run_id, load.get_active_brands(conn))


def _stage(run_id: str, brand: dict, stage: str, operation):
    with connect() as conn:
        try:
            with conn.transaction():
                row = staging.read(conn, run_id, brand["id"], lock=True)
                if row["saved_at"] is None:
                    operation(conn, row)
                    conn.execute(
                        "UPDATE pipeline.brand_runs SET failed_stage = NULL, error = NULL "
                        "WHERE run_id = %s AND brand_id = %s", (run_id, brand["id"]),
                    )
        except Exception as error:
            staging.record_failure(conn, run_id, brand["id"], stage, error)
            raise  # Airflow must retain the failed task and perform its own retries.
    return brand["id"]


def fetch(run_id: str, brand: dict) -> int:
    def operation(conn, row):
        if row["raw"] is not None:
            return
        with extract.make_client() as client:
            products = extract.fetch_all_products(client, row["brand"]["base_url"])
        conn.execute(
            "UPDATE pipeline.brand_runs SET raw = %s, fetched_at = %s WHERE run_id = %s AND brand_id = %s",
            (Jsonb(products), datetime.now(timezone.utc), run_id, brand["id"]),
        )
    return _stage(run_id, brand, "fetch", operation)


def validate(run_id: str, brand: dict) -> int:
    def operation(conn, row):
        if row["validated"] is not None:
            return
        if row["raw"] is None:
            raise ValueError("fetch has not completed")
        products = run.dedupe_products(row["raw"])
        run.check_product_count(row["brand"]["name"], len(products), load.active_product_count(conn, brand["id"]))
        conn.execute(
            "UPDATE pipeline.brand_runs SET validated = %s WHERE run_id = %s AND brand_id = %s",
            (Jsonb(products), run_id, brand["id"]),
        )
    return _stage(run_id, brand, "validate", operation)


def clean(run_id: str, brand: dict) -> int:
    def operation(conn, row):
        if row["cleaned"] is not None:
            return
        if row["validated"] is None:
            raise ValueError("validation has not completed")
        products = staging.encode_cleaned([rules.clean_product(p) for p in row["validated"]])
        conn.execute(
            "UPDATE pipeline.brand_runs SET cleaned = %s WHERE run_id = %s AND brand_id = %s",
            (Jsonb(products), run_id, brand["id"]),
        )
    return _stage(run_id, brand, "clean", operation)


def save(run_id: str, brand: dict) -> int:
    def operation(conn, row):
        if row["cleaned"] is None:
            raise ValueError("cleaning has not completed")
        # Serialize catalogue writers across runs, including the legacy CLI.
        conn.execute("SELECT id FROM catalog.brands WHERE id = %s FOR UPDATE", (brand["id"],))
        newer = conn.execute(
            "SELECT 1 FROM pipeline.brand_runs WHERE brand_id = %s AND saved_at IS NOT NULL "
            "AND fetched_at > %s LIMIT 1", (brand["id"], row["fetched_at"]),
        ).fetchone()
        if newer:
            raise ValueError("a newer snapshot was already saved; start a new run instead")
        result = run.process_brand(
            conn, row["brand"], row["validated"], row["fetched_at"],
            cleaned_products=staging.decode_cleaned(row["cleaned"]),
        )
        # Same outer transaction as raw history, upserts, missing counts, and changes.
        conn.execute(
            "UPDATE pipeline.brand_runs SET saved_at = now(), result = %s WHERE run_id = %s AND brand_id = %s",
            (Jsonb(asdict(result)), run_id, brand["id"]),
        )
    return _stage(run_id, brand, "save", operation)


def successful_brands(run_id: str) -> list[int]:
    with connect() as conn:
        return [row["brand_id"] for row in staging.results(conn, run_id) if row["saved_at"] is not None]


def notify(run_id: str, brand_ids: list[int]) -> dict:
    # Backend owns app.* and uses a separate, restricted database credential.
    from notifier.check_alerts import deliver_alerts

    database_url = os.environ.get("ALERT_DATABASE_URL")
    if not database_url:
        raise RuntimeError("ALERT_DATABASE_URL is not set")
    settings = config.load_settings()
    # Recheck completion in the catalogue DB; never trust a caller's brand list alone.
    allowed = sorted(set(brand_ids) & set(successful_brands(run_id)))
    outcome = deliver_alerts(database_url, settings.telegram_bot_token, settings.telegram_chat_id,
                             brand_ids=allowed, run_id=run_id)
    with connect() as conn:
        conn.execute("UPDATE pipeline.runs SET notifications = %s WHERE run_id = %s", (Jsonb(outcome), run_id))
    if outcome["failed"] or outcome["uncertain"]:
        raise RuntimeError(f"restock deliveries need attention: {outcome}")
    return outcome


def report(run_id: str) -> dict:
    with connect() as conn:
        exists = conn.execute("SELECT notifications, manifest_ready FROM pipeline.runs WHERE run_id = %s", (run_id,)).fetchone()
        results = []
        for row in staging.results(conn, run_id):
            if row["result"] is not None:
                results.append(run.BrandResult(**row["result"]))
            else:
                results.append(run.BrandResult(row["brand"]["name"], False,
                    error=row["error"] or "task did not complete; check Airflow task logs"))
        notifications = exists[0] if exists else None
        ok = exists is not None and exists[1] and all(r.ok for r in results) and notifications is not None \
            and not notifications["failed"] and not notifications["uncertain"]
        text = run.summary_text(results)
        if not ok:
            text = text.replace("✅", "❌", 1)
        if not exists or not exists[1]:
            text += "\n❌ Brand discovery did not complete; check Airflow."
        if notifications is None:
            text += "\n❌ Restock notification task did not complete; check Airflow."
        else:
            text += f"\nRestock deliveries: {notifications}"
        log.info("%s", text)
        conn.execute("UPDATE pipeline.runs SET summary = %s, finished_at = now() WHERE run_id = %s", (text, run_id))
    # A separate sender task serializes Telegram traffic through the Airflow pool.
    return {"ok": ok, "text": text}


def send_summary(run_id: str, report: dict) -> None:
    """Checkpoint the attempt before sending; never retry an ambiguous summary."""
    import httpx
    from notifier.check_alerts import SENDER_LOCK, accepted_message, delivery_status

    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = config.load_settings()
    if not (settings.telegram_bot_token and settings.telegram_chat_id):
        raise RuntimeError("Telegram summary is not configured")
    with connect() as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(%s)", (SENDER_LOCK,)).fetchone()[0]:
            raise RuntimeError("another Telegram sender is running")
        with conn.transaction():
            # A failed discovery still needs a durable summary attempt record.
            conn.execute("INSERT INTO pipeline.runs (run_id) VALUES (%s) ON CONFLICT DO NOTHING", (run_id,))
            previous = conn.execute("SELECT summary_delivery FROM pipeline.runs WHERE run_id = %s FOR UPDATE",
                                    (run_id,)).fetchone()[0]
            if previous and previous["status"] == "sent":
                return
            if previous and previous["status"] in ("sending", "uncertain"):
                conn.execute("UPDATE pipeline.runs SET summary_delivery = %s WHERE run_id = %s",
                             (Jsonb({"status": "uncertain"}), run_id))
                # Raise after commit, preserving the uncertainty checkpoint.
                uncertain = True
            else:
                uncertain = False
                conn.execute("UPDATE pipeline.runs SET summary_delivery = %s WHERE run_id = %s",
                             (Jsonb({"status": "sending"}), run_id))
        if uncertain:
            raise RuntimeError("summary delivery is uncertain; review Telegram before retrying")
        try:
            response = httpx.post(
                f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
                json={"chat_id": settings.telegram_chat_id, "text": report["text"][:4000]}, timeout=30,
            )
            message_id = accepted_message(response)
        except Exception as error:
            conn.execute("UPDATE pipeline.runs SET summary_delivery = %s WHERE run_id = %s",
                         (Jsonb({"status": delivery_status(error), "error": type(error).__name__}), run_id))
            # Never expose a token-bearing httpx request URL to Airflow logs.
            raise RuntimeError(f"summary delivery failed ({type(error).__name__}); inspect its ledger") from None
        conn.execute("UPDATE pipeline.runs SET summary_delivery = %s WHERE run_id = %s",
                     (Jsonb({"status": "sent", "message_id": message_id}), run_id))
