# the-catalogue-pipeline

Downloads every product of 10 Indian streetwear brands (Shopify stores) once a day,
cleans the data, and stores it in Postgres (`catalog` schema).

```
/products.json ──► extract ──► raw_products (only on change) ──► rules ──► products + variants
```

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then fill in DATABASE_URL and the Telegram values
```

## Database (once, as admin)

```bash
psql -h localhost -U postgres -d brands -f sql/catalog_schema.sql
psql -h localhost -U postgres -d brands -f sql/seed_brands.sql
psql -h localhost -U postgres -d brands -c "ALTER ROLE pipeline LOGIN PASSWORD 'change-me'"
```

The schema file creates the `pipeline` and `app_user` roles if they are missing.
`pipeline` can read, insert and update `catalog.*` (no delete). `app_user` can only read it.

## Run

```bash
python -m pipeline.run
```

Per brand: fetch all pages → safety check (stop if > 20% fewer products) → raw rows →
clean → upsert → mark missing products. Each brand is one transaction, so one failing
brand never stops the others. A summary goes to Telegram at the end.
The exit code is 1 if any brand failed.

## Product update history

Each successful brand transaction now also records new products, variant price changes,
restocks, sold-out sizes, removals, and relistings in `catalog.product_changes`.
Description/image edits remain raw changes and do not become shopping badges.
Prices compare the same variant's observed selling prices, not `compare_at_price`.

For an existing database, apply the updated `sql/catalog_schema.sql` as the database
admin **before** deploying the new pipeline or backend. This adds one table and three
indexes; existing product and variant rows are preserved. Then run:

```bash
python -m pipeline.backfill_changes
```

This recovers history from the retained raw snapshots, is safe to repeat, and sends no
Telegram messages. Historical product removals/relistings cannot be recovered reliably
from raw snapshots; these are recorded on future successful pipeline runs.

## Tests

```bash
pytest                                   # rules and pure helpers, no database
TEST_DATABASE_URL=postgresql://postgres:...@localhost:5432/catalogue_test pytest
```

The database tests need an **empty, throwaway** database. They drop and recreate `catalog`.

## Airflow on EC2 (IST)

The Airflow DAGs in `dags/catalogue.py` run the existing CLI commands:

- `catalogue_daily`: daily at 02:30 IST, refresh products → send backend restock
  alerts only if the refresh succeeds.
- `catalogue_backup`: daily at 03:30 IST, independently back up the database.

Follow [the Airflow deploy guide](deploy/airflow/README.md) to install the separate
environment, metadata database, dashboard login, and systemd services. DAGs start
paused. Remove the old Catalogue cron entries before enabling them. No application
schema changes are needed. Task retries and historical catchup are disabled because
the current commands are not safe to repeat automatically.

## Legacy cron on EC2 (IST)

Use these only until Airflow cutover; do not enable both schedulers:

```
30 2 * * *  cd ~/the-catalogue-data-pipeline && .venv/bin/python -m pipeline.run >> logs/pipeline.log 2>&1
30 3 * * *  ~/the-catalogue-data-pipeline/scripts/backup.sh >> ~/the-catalogue-data-pipeline/logs/backup.log 2>&1
```

`scripts/backup.sh` writes `~/backups/brands-YYYY-MM-DD.dump` and keeps the last 7 days.
Restore: `pg_restore -d brands <file>`.

## Logs

With Airflow, read task output in the dashboard or `~/airflow/logs`, and service
output with `journalctl -u catalogue-airflow@scheduler -f`. The locations below
apply to legacy cron/manual redirection.

- `logs/pipeline.log`: one line per page, one result line per brand (with its time), a summary,
  and "run finished in N s". A brand skipped by the safety check is a `WARNING` line with both counts.
- `logs/backup.log`: one line per backup.
- `LOG_LEVEL=DEBUG python -m pipeline.run` adds the HTTP status and time of every page request.
- Rotation: `deploy/the-catalogue-pipeline.logrotate` rotates both files weekly and keeps 8 weeks.
  Install: `sudo cp deploy/the-catalogue-pipeline.logrotate /etc/logrotate.d/the-catalogue-pipeline`.

## Files

| File | What it does |
|---|---|
| `pipeline/extract.py` | Fetch `/products.json` pages: `limit=250`, 1 s between requests, retry on 429/430/5xx |
| `pipeline/rules.py` | Pure cleaning rules: size, category, sale price, gift card |
| `pipeline/load.py` | Raw insert with hash dedupe, upserts, missing products |
| `pipeline/run.py` | Entry point, per-brand isolation, Telegram summary |
| `sql/` | Schema and brand seed (source of truth) |
