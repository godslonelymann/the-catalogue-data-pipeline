"""Download every product of one brand from Shopify's /products.json."""

import logging
import time

import httpx

from pipeline import config

log = logging.getLogger(__name__)

RETRY_STATUSES = {429, 430, 500, 502, 503, 504}


class ExtractError(Exception):
    pass


def make_client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"},
        timeout=config.TIMEOUT_S,
        follow_redirects=True,
    )


def _retry_wait(attempt: int, response: httpx.Response | None) -> float:
    """Seconds to wait before the next try. Honour Retry-After when the store sends it."""
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.replace(".", "", 1).isdigit():
            return min(float(retry_after), 120.0)
    return 2.0 * 2 ** attempt  # 2, 4, 8, 16 s


def fetch_page(client: httpx.Client, base_url: str, page: int) -> list[dict]:
    url = f"{base_url}/products.json"
    params = {"limit": config.PAGE_LIMIT, "page": page}
    for attempt in range(config.MAX_RETRIES + 1):
        response = None
        try:
            response = client.get(url, params=params)
            if response.status_code not in RETRY_STATUSES:
                response.raise_for_status()
                products = response.json().get("products")
                if not isinstance(products, list):
                    raise ExtractError(f"{url} page {page}: no 'products' list in response")
                return products
            reason = f"HTTP {response.status_code}"
        except httpx.TimeoutException:
            reason = "timeout"
        except httpx.TransportError as e:
            reason = f"network error: {e}"
        except httpx.HTTPStatusError as e:
            raise ExtractError(f"{url} page {page}: HTTP {e.response.status_code}") from e
        except ValueError as e:
            raise ExtractError(f"{url} page {page}: response is not JSON") from e

        if attempt == config.MAX_RETRIES:
            raise ExtractError(f"{url} page {page}: {reason} after {attempt + 1} tries")
        wait = _retry_wait(attempt, response)
        log.warning("%s page %d: %s, retry in %.0f s", base_url, page, reason, wait)
        time.sleep(wait)
    raise AssertionError("unreachable")


def fetch_all_products(client: httpx.Client, base_url: str) -> list[dict]:
    """All products of one store. Stops when a page has fewer than PAGE_LIMIT products."""
    products: list[dict] = []
    page = 1
    while True:
        batch = fetch_page(client, base_url, page)
        products.extend(batch)
        log.info("%s page %d: %d products", base_url, page, len(batch))
        if len(batch) < config.PAGE_LIMIT:
            break
        page += 1
        time.sleep(config.REQUEST_DELAY_S)
    return products
