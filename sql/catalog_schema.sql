-- catalog schema: brand data written by the pipeline.
-- Run as the admin user (postgres) on database `brands`:
--   psql -h localhost -U postgres -d brands -f sql/catalog_schema.sql
-- Safe to run more than once.
--
-- The roles are created without a password. Set one after the first run:
--   ALTER ROLE pipeline LOGIN PASSWORD '...';
--   ALTER ROLE app_user LOGIN PASSWORD '...';

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline') THEN
        CREATE ROLE pipeline NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_user') THEN
        CREATE ROLE app_user NOLOGIN;
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.brands (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    base_url    TEXT NOT NULL UNIQUE,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per product per change (insert only when payload_hash changes).
CREATE TABLE IF NOT EXISTS catalog.raw_products (
    id                  BIGSERIAL PRIMARY KEY,
    brand_id            INT NOT NULL REFERENCES catalog.brands (id),
    shopify_product_id  BIGINT NOT NULL,
    payload             JSONB NOT NULL,
    payload_hash        TEXT NOT NULL,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS raw_products_latest_idx
    ON catalog.raw_products (brand_id, shopify_product_id, fetched_at DESC);

CREATE TABLE IF NOT EXISTS catalog.products (
    id                  BIGSERIAL PRIMARY KEY,
    brand_id            INT NOT NULL REFERENCES catalog.brands (id),
    shopify_product_id  BIGINT NOT NULL,
    handle              TEXT NOT NULL,
    title               TEXT NOT NULL,
    product_type        TEXT,
    category            TEXT NOT NULL DEFAULT 'other',
    tags                TEXT[] NOT NULL DEFAULT '{}',
    body_html           TEXT,
    image_urls          TEXT[] NOT NULL DEFAULT '{}',
    options             JSONB NOT NULL DEFAULT '[]',
    is_gift_card        BOOLEAN NOT NULL DEFAULT false,
    shopify_created_at  TIMESTAMPTZ,
    published_at        TIMESTAMPTZ,
    shopify_updated_at  TIMESTAMPTZ,
    is_active           BOOLEAN NOT NULL DEFAULT true,
    missing_runs        SMALLINT NOT NULL DEFAULT 0,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (brand_id, shopify_product_id)
);

CREATE INDEX IF NOT EXISTS products_listing_idx
    ON catalog.products (brand_id, is_active, is_gift_card);

CREATE TABLE IF NOT EXISTS catalog.variants (
    id                  BIGSERIAL PRIMARY KEY,
    product_id          BIGINT NOT NULL REFERENCES catalog.products (id),
    shopify_variant_id  BIGINT NOT NULL,
    position            SMALLINT,
    title               TEXT,
    option1             TEXT,
    option2             TEXT,
    option3             TEXT,
    size_raw            TEXT,
    size_norm           TEXT,
    sku                 TEXT,
    price               NUMERIC(10,2) NOT NULL,
    compare_at_price    NUMERIC(10,2),
    discount_pct        NUMERIC(5,1) GENERATED ALWAYS AS (
                            CASE WHEN compare_at_price > 0
                                 THEN round((compare_at_price - price) * 100 / compare_at_price, 1)
                            END
                        ) STORED,
    available           BOOLEAN NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT true,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (product_id, shopify_variant_id),
    -- compare_at_price is only stored when the variant is really on sale (rules §7.3)
    CONSTRAINT variants_sale_price_check
        CHECK (compare_at_price IS NULL OR compare_at_price > price)
);

CREATE INDEX IF NOT EXISTS variants_size_idx ON catalog.variants (size_norm);

-- Access: pipeline reads and writes catalog (no DELETE: rows are never deleted).
-- app_user (backend) can only read catalog.
GRANT USAGE ON SCHEMA catalog TO pipeline, app_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA catalog TO pipeline;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA catalog TO pipeline;
GRANT SELECT ON ALL TABLES IN SCHEMA catalog TO app_user;
