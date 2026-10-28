"""Settings read from the environment (and from .env, if present)."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

USER_AGENT = "Mozilla/5.0 (the-catalogue personal price tracker)"
PAGE_LIMIT = 250          # Shopify default is 30, which silently truncates
REQUEST_DELAY_S = 1.0     # pause between requests to the same brand
TIMEOUT_S = 30.0
MAX_RETRIES = 4
SAFETY_DROP = 0.20        # skip a brand if its product count drops by more than this
MISSED_RUNS_LIMIT = 2     # deactivate a product after this many missed runs


@dataclass(frozen=True)
class Settings:
    database_url: str
    telegram_bot_token: str | None
    telegram_chat_id: str | None
    log_level: str = "INFO"   # DEBUG shows more detail for one run: LOG_LEVEL=DEBUG python -m pipeline.run


def load_settings() -> Settings:
    load_dotenv()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set (see .env.example)")
    return Settings(
        database_url=database_url,
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN") or None,
        telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID") or None,
        log_level=(os.environ.get("LOG_LEVEL") or "INFO").upper(),
    )
