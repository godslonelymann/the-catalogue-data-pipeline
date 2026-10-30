-- Apply as admin to the CATALOGUE database, after catalog_schema.sql.
-- Airflow's own metadata belongs in a separate database with a separate owner.
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    manifest_ready BOOLEAN NOT NULL DEFAULT false,
    finished_at TIMESTAMPTZ,
    notifications JSONB,
    summary_delivery JSONB,
    summary TEXT
);

CREATE TABLE IF NOT EXISTS pipeline.brand_runs (
    run_id TEXT NOT NULL REFERENCES pipeline.runs (run_id),
    brand_id INTEGER NOT NULL REFERENCES catalog.brands (id),
    brand JSONB NOT NULL,
    raw JSONB,
    validated JSONB,
    cleaned JSONB,
    fetched_at TIMESTAMPTZ,
    saved_at TIMESTAMPTZ,
    result JSONB,
    failed_stage TEXT,
    error TEXT,
    PRIMARY KEY (run_id, brand_id),
    CHECK ((saved_at IS NULL) = (result IS NULL))
);
CREATE INDEX IF NOT EXISTS brand_runs_saved_idx
    ON pipeline.brand_runs (brand_id, fetched_at DESC) WHERE saved_at IS NOT NULL;

-- No access to app.* is needed by the pipeline database role.
GRANT USAGE ON SCHEMA pipeline TO pipeline;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA pipeline TO pipeline;
