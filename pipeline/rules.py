"""Cleaning rules (CLAUDE.md §7).

Pure functions only: no database, no network. Every rule has tests in tests/test_rules.py.
"""

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

# ---------------------------------------------------------------- text, tags, dates


def clean_text(value) -> str | None:
    """Strip whitespace. Empty text becomes None (stored as NULL)."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def clean_tags(tags) -> list[str]:
    """Strip each tag and drop empty ones. Accepts a list or a comma-separated string."""
    if tags is None:
        return []
    if isinstance(tags, str):
        tags = tags.split(",")
    return [t.strip() for t in tags if t and str(t).strip()]


def image_urls(images) -> list[str]:
    """All image URLs, in the order the store gives them. The first one is the main image."""
    return [img["src"] for img in images or [] if img.get("src")]


def parse_timestamp(value) -> datetime | None:
    text = clean_text(value)
    if text is None:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


# ---------------------------------------------------------------- prices (§7.3)

CENT = Decimal("0.01")


def parse_price(value) -> Decimal | None:
    """Price string -> Decimal with 2 places. Missing or empty -> None. Never float."""
    text = clean_text(value)
    if text is None:
        return None
    try:
        return Decimal(text).quantize(CENT)
    except InvalidOperation as e:
        raise ValueError(f"bad price: {value!r}") from e


def sale_compare_at(price: Decimal, compare_at: Decimal | None) -> Decimal | None:
    """Keep compare_at_price only when it is higher than price. 0.00, equal or lower -> None."""
    if compare_at is None or compare_at <= price:
        return None
    return compare_at


# ---------------------------------------------------------------- gift cards (§7.4)

_GIFT_CARD = re.compile(r"gift[\s-]*card", re.IGNORECASE)


def is_gift_card(title: str | None, product_type: str | None) -> bool:
    """Title or product_type says "gift card". requires_shipping is NOT used (it is unreliable)."""
    return any(text and _GIFT_CARD.search(text) for text in (title, product_type))


# ---------------------------------------------------------------- sizes (§7.1)

LETTER_SIZES = ["XXS", "XS", "S", "M", "L", "XL", "XXL", "XXXL", "4XL", "5XL"]
ONE_SIZE = "ONE SIZE"

SIZE_ALIASES = {
    "X-SMALL": "XS", "X SMALL": "XS", "EXTRA SMALL": "XS",
    "SMALL": "S",
    "MEDIUM": "M",
    "LARGE": "L",
    "X-LARGE": "XL", "X LARGE": "XL", "XLARGE": "XL", "EXTRA LARGE": "XL",
    "XX-LARGE": "XXL", "XX LARGE": "XXL", "XXLARGE": "XXL", "2XL": "XXL",
    "2X-LARGE": "XXL", "2X LARGE": "XXL",
    "XXX-LARGE": "XXXL", "XXX LARGE": "XXXL", "3XL": "XXXL",
    "XXXXL": "4XL", "XXXXXL": "5XL",
    "FREE SIZE": ONE_SIZE, "FREESIZE": ONE_SIZE, "ONE SIZE": ONE_SIZE, "ONESIZE": ONE_SIZE,
}

_NUMBER = re.compile(r"\d+(\.\d+)?")
_SIZE_SEPARATORS = re.compile(r"[\s/()\-]+")


def _normalize_one(text: str) -> str:
    text = " ".join(text.split()).upper()
    return SIZE_ALIASES.get(text, text)


def normalize_size(raw: str | None) -> str | None:
    """Size value -> size_norm. Unknown values are kept as upper(trim(raw))."""
    text = clean_text(raw)
    if text is None:
        return None
    norm = _normalize_one(text)
    if norm in LETTER_SIZES or norm == ONE_SIZE or _NUMBER.fullmatch(norm):
        return norm
    # Combined values such as "S/28", "S 30" or 'S-28 (CHEST 38" AND WAIST 28")':
    # keep the letter size.
    for part in _SIZE_SEPARATORS.split(norm):
        part = SIZE_ALIASES.get(part, part)
        if part in LETTER_SIZES:
            return part
    return norm


def is_known_size(norm: str | None) -> bool:
    """True when size_norm is a letter size, ONE SIZE, or a number. Unknown ones get logged."""
    return norm is not None and (
        norm in LETTER_SIZES or norm == ONE_SIZE or bool(_NUMBER.fullmatch(norm))
    )


def looks_like_size(value: str | None) -> bool:
    """Letter size, free size, or a number in the waist/shoe range 24-48."""
    norm = normalize_size(value)
    if norm is None:
        return False
    if norm in LETTER_SIZES or norm == ONE_SIZE:
        return True
    return bool(_NUMBER.fullmatch(norm)) and 24 <= float(norm) <= 48


def _option_name(option: dict) -> str:
    return (option.get("name") or "").lower().strip()


def detect_size_position(options: list[dict]) -> int | None:
    """Which option (1, 2 or 3) holds the clothing size, or None."""
    positioned = [(opt.get("position") or i + 1, opt) for i, opt in enumerate(options or [])]
    positioned = [(pos, opt) for pos, opt in positioned if pos in (1, 2, 3)]

    # 1. Name match: "Size", "SIZE", "size", "Size " ...
    for pos, opt in positioned:
        if _option_name(opt) == "size":
            return pos

    # 2. Value fallback. Names that mention size but are not "size" ("Frame Size",
    #    "Size type") are not clothing sizes, so they are skipped.
    for pos, opt in positioned:
        if "size" in _option_name(opt):
            continue
        values = opt.get("values") or []
        if values and all(looks_like_size(v) for v in values):
            return pos
    return None


def size_fields(product: dict) -> list[tuple[str | None, str | None]]:
    """(size_raw, size_norm) for each variant of the product, in the same order."""
    variants = product.get("variants") or []
    pos = detect_size_position(product.get("options") or [])
    if pos is not None:
        result = []
        for v in variants:
            raw = clean_text(v.get(f"option{pos}"))
            result.append((raw, normalize_size(raw)))
        return result
    # 3. A single "Default Title" variant (caps, bags, ...) is one size.
    if len(variants) == 1 and clean_text(variants[0].get("title")) == "Default Title":
        return [(None, ONE_SIZE)]
    return [(None, None)] * len(variants)


# ---------------------------------------------------------------- category (§7.2)

# Order matters: the first category whose keyword matches wins.
# "polo t-shirt" -> polo, "sweatshirt" before "shirt", "cargo shorts" -> shorts, "denim jacket" -> jacket.
CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("polo", ["polo", "polos"]),
    ("hoodie", ["hoodie", "hoodies", "hoody", "hooded"]),
    ("sweatshirt", ["sweatshirt", "sweatshirts", "crewneck", "crewnecks", "sweater", "sweaters"]),
    ("tee", ["tee", "tees", "t-shirt", "t-shirts", "tshirt", "tshirts", "t shirt", "t shirts"]),
    ("shirt", ["shirt", "shirts", "overshirt", "overshirts"]),
    ("jacket", ["jacket", "jackets", "bomber", "varsity", "windbreaker", "coat", "puffer", "shacket"]),
    ("shorts", ["shorts"]),
    ("denim", ["denim", "denims", "jeans", "jean"]),
    ("pants", ["pant", "pants", "trouser", "trousers", "cargo", "cargos", "jogger", "joggers",
               "sweatpants", "trackpants", "track pants"]),
    ("cap", ["cap", "caps", "hat", "hats", "beanie", "beanies"]),
    ("socks", ["sock", "socks"]),
    ("bag", ["bag", "bags", "backpack", "backpacks", "tote", "totes", "sling"]),
    ("accessory", ["accessory", "accessories", "sunglasses", "keychain", "keychains", "wallet",
                   "wallets", "belt", "belts", "chain", "chains", "bracelet", "necklace", "pendant",
                   "mask", "masks"]),
]

CATEGORIES = [name for name, _ in CATEGORY_KEYWORDS] + ["other"]

_CATEGORY_PATTERNS = [
    (name, re.compile(r"(?<![a-z0-9])(?:" + "|".join(map(re.escape, words)) + r")(?![a-z0-9])",
                      re.IGNORECASE))
    for name, words in CATEGORY_KEYWORDS
]


def match_category(text: str | None) -> str | None:
    """First category with a whole-word keyword match in text, or None."""
    if not text:
        return None
    for name, pattern in _CATEGORY_PATTERNS:
        if pattern.search(text):
            return name
    return None


def categorize(product_type: str | None, tags: list[str], title: str | None) -> str:
    """product_type first, then tags, then title, else 'other'."""
    for text in (product_type, " | ".join(tags), title):
        category = match_category(text)
        if category:
            return category
    return "other"


# ---------------------------------------------------------------- whole product


def clean_options(options) -> list[dict]:
    """Options as received: name, position, values."""
    return [
        {"name": o.get("name"), "position": o.get("position"), "values": o.get("values") or []}
        for o in options or []
    ]


def clean_variant(variant: dict, size_raw: str | None, size_norm: str | None) -> dict:
    price = parse_price(variant.get("price"))
    if price is None:
        raise ValueError(f"variant {variant.get('id')} has no price")
    return {
        "shopify_variant_id": int(variant["id"]),
        "position": variant.get("position"),
        "title": clean_text(variant.get("title")),
        "option1": clean_text(variant.get("option1")),
        "option2": clean_text(variant.get("option2")),
        "option3": clean_text(variant.get("option3")),
        "size_raw": size_raw,
        "size_norm": size_norm,
        "sku": clean_text(variant.get("sku")),
        "price": price,
        "compare_at_price": sale_compare_at(price, parse_price(variant.get("compare_at_price"))),
        "available": bool(variant.get("available")),
    }


def clean_product(raw: dict) -> dict:
    """One /products.json product -> row for catalog.products, with its variants."""
    title = clean_text(raw.get("title")) or clean_text(raw.get("handle")) or str(raw["id"])
    product_type = clean_text(raw.get("product_type"))
    tags = clean_tags(raw.get("tags"))
    variants = raw.get("variants") or []
    return {
        "shopify_product_id": int(raw["id"]),
        "handle": clean_text(raw.get("handle")) or str(raw["id"]),
        "title": title,
        "product_type": product_type,
        "category": categorize(product_type, tags, title),
        "tags": tags,
        "body_html": clean_text(raw.get("body_html")),
        "image_urls": image_urls(raw.get("images")),
        "options": clean_options(raw.get("options")),
        "is_gift_card": is_gift_card(title, product_type),
        "shopify_created_at": parse_timestamp(raw.get("created_at")),
        "published_at": parse_timestamp(raw.get("published_at")),
        "shopify_updated_at": parse_timestamp(raw.get("updated_at")),
        "variants": [
            clean_variant(v, size_raw, size_norm)
            for v, (size_raw, size_norm) in zip(variants, size_fields(raw))
        ],
    }
