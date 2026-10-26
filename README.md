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

## Tests

```bash
pytest                                   # rules and pure helpers, no database
TEST_DATABASE_URL=postgresql://postgres:...@localhost:5432/catalogue_test pytest
```

The database tests need an **empty, throwaway** database. They drop and recreate `catalog`.

## Cron on EC2 (IST)

```
30 2 * * *  cd ~/the-catalogue-pipeline && .venv/bin/python -m pipeline.run >> logs/pipeline.log 2>&1
30 3 * * *  ~/the-catalogue-pipeline/scripts/backup.sh >> ~/the-catalogue-pipeline/logs/backup.log 2>&1
```

`scripts/backup.sh` writes `~/backups/brands-YYYY-MM-DD.dump` and keeps the last 7 days.
Restore: `pg_restore -d brands <file>`.

## Files

| File | What it does |
|---|---|
| `pipeline/extract.py` | Fetch `/products.json` pages: `limit=250`, 1 s between requests, retry on 429/430/5xx |
| `pipeline/rules.py` | Pure cleaning rules: size, category, sale price, gift card |
| `pipeline/load.py` | Raw insert with hash dedupe, upserts, missing products |
| `pipeline/run.py` | Entry point, per-brand isolation, Telegram summary |
| `sql/` | Schema and brand seed (source of truth) |
