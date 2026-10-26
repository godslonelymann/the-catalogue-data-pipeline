"""Tests for pipeline/rules.py. No database needed.

Each data quirk from CLAUDE.md §5.2 has at least one case here.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pipeline import rules


def product(options=None, variants=None, **extra):
    """Small /products.json-like product for tests."""
    base = {
        "id": 111,
        "title": "Test Tee",
        "handle": "test-tee",
        "product_type": "",
        "tags": [],
        "body_html": "<p>Nice</p>",
        "images": [],
        "options": options or [],
        "variants": variants or [],
        "created_at": "2026-01-02T10:00:00+05:30",
        "published_at": None,
        "updated_at": "2026-01-03T10:00:00+05:30",
    }
    base.update(extra)
    return base


def variant(vid=1, title="Default Title", option1="Default Title", option2=None, option3=None,
            price="999.00", compare_at_price=None, available=True, position=1, sku=""):
    return {
        "id": vid, "title": title, "option1": option1, "option2": option2, "option3": option3,
        "price": price, "compare_at_price": compare_at_price, "available": available,
        "position": position, "sku": sku,
    }


# ---------------------------------------------------------------- text, tags, images, dates

def test_clean_text_empty_becomes_none():
    assert rules.clean_text("") is None
    assert rules.clean_text("   ") is None
    assert rules.clean_text(None) is None
    assert rules.clean_text("  Warping Theories ") == "Warping Theories"


def test_clean_tags_strips_and_drops_empty():
    assert rules.clean_tags([" tee ", "", "  ", "summer"]) == ["tee", "summer"]
    assert rules.clean_tags([]) == []
    assert rules.clean_tags(None) == []
    assert rules.clean_tags("a, b,,c ") == ["a", "b", "c"]


def test_image_urls_keeps_order_and_handles_no_images():
    images = [{"src": "https://cdn/1.jpg"}, {"src": "https://cdn/2.jpg"}]
    assert rules.image_urls(images) == ["https://cdn/1.jpg", "https://cdn/2.jpg"]
    # Fugazee and Urban Monkey have products with no images.
    assert rules.image_urls([]) == []
    assert rules.image_urls(None) == []


def test_parse_timestamp():
    ts = rules.parse_timestamp("2026-01-02T10:00:00+05:30")
    assert ts == datetime(2026, 1, 2, 4, 30, tzinfo=timezone.utc)
    assert rules.parse_timestamp(None) is None
    assert rules.parse_timestamp("") is None


# ---------------------------------------------------------------- prices

def test_parse_price_is_decimal_not_float():
    price = rules.parse_price("1299.00")
    assert price == Decimal("1299.00")
    assert isinstance(price, Decimal)
    assert rules.parse_price("1299") == Decimal("1299.00")
    assert rules.parse_price(None) is None
    assert rules.parse_price("") is None


def test_parse_price_highest_seen_fits():
    # NoughtOne has 160000.00, which must fit NUMERIC(10,2).
    assert rules.parse_price("160000.00") == Decimal("160000.00")


def test_parse_price_rejects_garbage():
    with pytest.raises(ValueError):
        rules.parse_price("abc")


@pytest.mark.parametrize("price, compare_at, expected", [
    ("1000.00", "1500.00", Decimal("1500.00")),  # real sale
    ("1000.00", "0.00", None),                    # 0.00 means no sale (702 variants)
    ("1000.00", "1000.00", None),                 # equal means no sale (1,465 variants)
    ("1000.00", "800.00", None),                  # Farak data-entry errors: compare < price
    ("1000.00", None, None),                      # missing
    ("1000.00", "", None),                        # empty string
])
def test_sale_compare_at(price, compare_at, expected):
    p = rules.parse_price(price)
    assert rules.sale_compare_at(p, rules.parse_price(compare_at)) == expected


# ---------------------------------------------------------------- gift cards

def test_gift_card_by_title_or_type():
    assert rules.is_gift_card("Genrage Gift Card", None)
    assert rules.is_gift_card("E-GIFT CARD", "")
    assert rules.is_gift_card("Something", "Gift Cards")
    assert rules.is_gift_card("Urban Monkey Giftcard", None)
    assert not rules.is_gift_card("Gift Box Tee", "T-Shirts")


def test_requires_shipping_false_is_not_gift_card():
    # Sullitt's "Sakura Predator Denim" has requires_shipping=false but is a real product.
    raw = product(title="Sakura Predator Denim",
                  variants=[{**variant(), "requires_shipping": False}])
    assert rules.clean_product(raw)["is_gift_card"] is False


# ---------------------------------------------------------------- size normalisation

@pytest.mark.parametrize("raw, expected", [
    ("M", "M"),
    (" m ", "M"),
    ("X-Large", "XL"),
    ("X LARGE", "XL"),
    ("Extra Large", "XL"),
    ("XX-Large", "XXL"),
    ("2XL", "XXL"),
    ("Small", "S"),
    ("Medium", "M"),
    ("Large", "L"),
    ("Free Size", "ONE SIZE"),
    ("FREESIZE", "ONE SIZE"),
    ("one size", "ONE SIZE"),
    ("S/28", "S"),          # combined values keep the letter size
    ("28/S", "S"),
    ('S-28 (CHEST 38" AND WAIST 28")', "S"),   # Fugazee co-ord sets
    ('M-30(CHEST 40" AND WAIST 30")', "M"),
    ("XXL 38", "XXL"),                          # Natty Garb
    ("2X LARGE", "XXL"),                        # Farak
    ("2X-Large", "XXL"),
    ("5XL", "5XL"),                             # Arkar
    ("3XL/38", "XXXL"),                         # Fugazee: alias inside a combined value
    ("2XL/36", "XXL"),                          # Urban Monkey
    ("XXXXL", "4XL"),
    ("32", "32"),           # numbers stay numbers
    ("UK 9", "UK 9"),       # unknown: upper(trim(raw))
    ("Customise Size", "CUSTOMISE SIZE"),       # Arkar: unknown, kept and logged
    ("52MM", "52MM"),
    ("", None),
    (None, None),
])
def test_normalize_size(raw, expected):
    assert rules.normalize_size(raw) == expected


def test_is_known_size():
    assert rules.is_known_size("XL")
    assert rules.is_known_size("ONE SIZE")
    assert rules.is_known_size("32")
    assert not rules.is_known_size("UK 9")
    assert not rules.is_known_size(None)


@pytest.mark.parametrize("value, expected", [
    ("M", True), ("XXL", True), ("Free Size", True), ("30", True), ("48", True),
    ("S/28", True), ("Black", False), ("1", False), ("100", False), ("", False),
])
def test_looks_like_size(value, expected):
    assert rules.looks_like_size(value) is expected


# ---------------------------------------------------------------- size detection

@pytest.mark.parametrize("name", ["Size", "SIZE", "size", "Size "])
def test_size_option_name_variants(name):
    options = [{"name": name, "position": 1, "values": ["S", "M"]}]
    assert rules.detect_size_position(options) == 1


def test_size_at_option2_noughtone():
    # NoughtOne: size at option2, with Fabric / Care instructions as other options.
    options = [
        {"name": "Fabric", "position": 1, "values": ["Cotton"]},
        {"name": "Size", "position": 2, "values": ["S", "M", "L"]},
        {"name": "Care instructions", "position": 3, "values": ["Cold wash"]},
    ]
    assert rules.detect_size_position(options) == 2


def test_size_at_option3_noughtone():
    options = [
        {"name": "Fabric", "position": 1, "values": ["Cotton"]},
        {"name": "Care instructions", "position": 2, "values": ["Cold wash"]},
        {"name": "Size", "position": 3, "values": ["M"]},
    ]
    assert rules.detect_size_position(options) == 3


def test_color_at_option1_warping_theories():
    options = [
        {"name": "Color", "position": 1, "values": ["Black", "White"]},
        {"name": "Size", "position": 2, "values": ["S", "M"]},
    ]
    assert rules.detect_size_position(options) == 2


def test_option_literally_named_s_natty_garb():
    # Natty Garb: one product whose option name is "S"; the values are sizes.
    options = [{"name": "S", "position": 1, "values": ["S", "M", "L", "XL"]}]
    assert rules.detect_size_position(options) == 1


def test_value_fallback_numbers_waist():
    options = [{"name": "Waist", "position": 1, "values": ["28", "30", "32", "34"]}]
    assert rules.detect_size_position(options) == 1


@pytest.mark.parametrize("name, values", [
    ("Frame Size", ["M", "L"]),        # Urban Monkey sunglasses
    ("Size type", ["S", "M"]),         # not a clothing size
    ("Serial No", ["1", "2", "3"]),    # numbers outside the waist range
    ("Denominations", ["500", "1000"]),
    ("Color", ["Black", "White"]),
])
def test_not_a_clothing_size(name, values):
    options = [{"name": name, "position": 1, "values": values}]
    assert rules.detect_size_position(options) is None


def test_size_fields_default_title_is_one_size():
    # Urban Monkey caps: one "Default Title" variant, no size option.
    raw = product(options=[{"name": "Title", "position": 1, "values": ["Default Title"]}],
                  variants=[variant()])
    assert rules.size_fields(raw) == [(None, "ONE SIZE")]


def test_size_fields_reads_the_right_option():
    raw = product(
        options=[{"name": "Color", "position": 1, "values": ["Black"]},
                 {"name": "Size ", "position": 2, "values": ["X-Large", "M"]}],
        variants=[variant(1, "Black / X-Large", "Black", "X-Large"),
                  variant(2, "Black / M", "Black", "M", position=2)],
    )
    assert rules.size_fields(raw) == [("X-Large", "XL"), ("M", "M")]


def test_size_fields_no_size_several_variants():
    raw = product(options=[{"name": "Color", "position": 1, "values": ["Black", "White"]}],
                  variants=[variant(1, "Black", "Black"), variant(2, "White", "White")])
    assert rules.size_fields(raw) == [(None, None), (None, None)]


# ---------------------------------------------------------------- category

@pytest.mark.parametrize("text, expected", [
    ("Oversized T-Shirt", "tee"),
    ("oversized tshirt", "tee"),
    ("Graphic Tee", "tee"),
    ("Polo T-Shirt", "polo"),
    ("Sweatshirt", "sweatshirt"),
    ("Linen Shirt", "shirt"),
    ("Zip Hoodie", "hoodie"),
    ("Denim Jacket", "jacket"),
    ("Baggy Jeans", "denim"),
    ("Cargo Shorts", "shorts"),
    ("Parachute Cargo", "pants"),
    ("Wide Leg Trousers", "pants"),
    ("Snapback Cap", "cap"),
    ("Ankle Socks", "socks"),
    ("Tote Bag", "bag"),
    ("Sunglasses", "accessory"),
    ("Mystery Box", None),
    ("Capri Blue Tank", None),  # whole words only: "Capri" is not "cap"
])
def test_match_category(text, expected):
    assert rules.match_category(text) == expected


def test_categorize_order_product_type_then_tags_then_title():
    assert rules.categorize("Hoodies", ["tee"], "Cool Tee") == "hoodie"
    assert rules.categorize(None, ["shirts"], "Cool Tee") == "shirt"
    assert rules.categorize(None, [], "Cool Tee") == "tee"
    assert rules.categorize("Clothing", [], "Mystery Box") == "other"


def test_categorize_empty_type_and_tags():
    # Sullitt / Offscript: product_type always empty. Natty Garb: 91% no tags.
    assert rules.categorize(None, [], "Boxy Fit Hoodie") == "hoodie"


# ---------------------------------------------------------------- whole product

def test_clean_product_full():
    raw = product(
        title="  Boxy Tee ",
        product_type="",
        tags=[" Tees ", ""],
        vendor="SPRING COLLECTION",
        body_html="<table><tr><td>S</td></tr></table>",
        images=[{"src": "https://cdn/a.jpg"}, {"src": "https://cdn/b.jpg"}],
        options=[{"name": "Size", "position": 1, "values": ["S", "M"]}],
        variants=[
            variant(1, "S", "S", price="999.00", compare_at_price="1499.00", available=False, sku="3"),
            variant(2, "M", "M", price="999.00", compare_at_price="0.00", position=2, sku=""),
        ],
    )
    p = rules.clean_product(raw)
    assert p["shopify_product_id"] == 111
    assert p["title"] == "Boxy Tee"
    assert p["product_type"] is None
    assert p["tags"] == ["Tees"]
    assert p["category"] == "tee"
    assert p["body_html"].startswith("<table>")  # size charts are kept
    assert p["image_urls"] == ["https://cdn/a.jpg", "https://cdn/b.jpg"]
    assert p["options"] == [{"name": "Size", "position": 1, "values": ["S", "M"]}]
    assert p["is_gift_card"] is False
    assert "vendor" not in p  # vendor is never used as the brand name

    s, m = p["variants"]
    assert s["size_norm"] == "S" and s["available"] is False
    assert s["compare_at_price"] == Decimal("1499.00")
    assert s["sku"] == "3"
    assert m["compare_at_price"] is None
    assert m["sku"] is None  # empty SKU (Warping Theories) -> NULL


def test_clean_product_gift_card_denominations():
    raw = product(
        title="Gift Card",
        options=[{"name": "Denominations", "position": 1, "values": ["500", "1000"]}],
        variants=[variant(1, "500", "500", price="500.00"),
                  variant(2, "1000", "1000", price="1000.00", position=2)],
    )
    p = rules.clean_product(raw)
    assert p["is_gift_card"] is True
    assert [v["size_norm"] for v in p["variants"]] == [None, None]


def test_clean_product_missing_price_fails():
    raw = product(variants=[variant(price=None)])
    with pytest.raises(ValueError):
        rules.clean_product(raw)
