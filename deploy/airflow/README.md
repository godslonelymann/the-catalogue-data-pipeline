# Airflow on EC2

Airflow starts the existing commands; it does not replace the scraper, database,
or API. Keep each project's `.venv` and `.env` separate from Airflow's environment.

| DAG | Schedule (Asia/Kolkata) | Tasks |
| --- | --- | --- |
| `catalogue_daily` | Daily, 02:30 | Product refresh → restock alerts, only after a successful refresh |
| `catalogue_backup` | Daily, 03:30 | Back up `brands`, independently of the refresh |

There are no automatic retries, historical catchup runs, or overlapping runs of
the same DAG. The pipeline commits successful brands even if another brand fails.
A complete rerun also advances missing-product counters; inspect failures before
manually rerunning. Alerts may also be duplicated if a process stops after Telegram
accepts a message but before it marks the alert fired. Airflow does not make either
command safe to repeat. A partial refresh failure blocks this run's alert task.

The cron timetable waits for the next scheduled time when enabled. Creating or
parsing these DAGs never runs application jobs. Both DAGs start **paused**.

## 1. Pull the code and install dependencies

All server commands below run as `ubuntu`, not root. EC2 uses these two checkouts:

```text
/home/ubuntu/the-catalogue-data-pipeline
/home/ubuntu/the-catalogue-backend
```

The local folder is named `the-catalogue-pipeline`; its GitHub/EC2 checkout is named
`the-catalogue-data-pipeline`. The DAG is shipped in the pipeline repository only.
For another layout, change the three checkout paths in `catalogue.env` below.

```bash
cd ~/the-catalogue-data-pipeline
git pull
bash deploy/airflow/install.sh
```

The installer reuses `~/airflow-venv` (including the Airflow already installed
there), installs pinned Airflow 3.3.2 with Apache's constraints for its Python
version, adds the Postgres/FAB providers, and checks dependencies. It creates
`~/airflow/catalogue.env` from the example only if the file does not already exist.
It does not start services, run application jobs, or change cron. Stop existing
Airflow services before installing upgrades. Do not run `airflow standalone`
alongside these services.

## 2. Create Airflow's own metadata database

The existing `brands` database and its schemas need **no changes**. Airflow uses a
new database and login solely for schedules, task states, logs metadata, and users.

Generate a database password with `openssl rand -hex 24` and keep it for step 3.
Then open an admin SQL session:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1
```

For a fresh installation, enter:

```sql
CREATE ROLE catalogue_airflow LOGIN;
\password catalogue_airflow
CREATE DATABASE catalogue_airflow OWNER catalogue_airflow;
\q
```

The `\password` prompt asks for the generated password twice. If this database and
role already exist, reuse them; do not drop them or reset their password on updates.
The Airflow login does not need access to `brands`.

## 3. Fill in the server-only configuration

```bash
nano ~/airflow/catalogue.env
chmod 600 ~/airflow/catalogue.env
```

Replace every `CHANGE_ME`:

- Database URL: insert the password from step 2. A hex password is URL-safe.
- `AIRFLOW__CORE__FERNET_KEY`: generate with the command below.
- `AIRFLOW__API_AUTH__JWT_SECRET` and `AIRFLOW__API__SECRET_KEY`: generate a separate
  value for each with `openssl rand -hex 32`.

```bash
~/airflow-venv/bin/python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

Keep these values stable across restarts. Never commit `catalogue.env`. Do not put
the application's `DATABASE_URL` or Telegram tokens in it: each job reads its own
checkout's existing `.env` with its existing database role.

Load the configuration and create Airflow's tables and dashboard user:

```bash
set -a
source ~/airflow/catalogue.env
set +a

~/airflow-venv/bin/airflow db migrate
~/airflow-venv/bin/airflow users create \
  --username admin --firstname Anurag --lastname Admin \
  --role Admin --email YOUR_EMAIL_ADDRESS
```

Replace `YOUR_EMAIL_ADDRESS` before running. The user command prompts for a new
dashboard password. User creation is needed once, not on every deployment.

The configuration uses Postgres with `LocalExecutor`, a FAB password login, one
task slot, one API worker, and one DAG parser for the existing 4 GB instance.
Watch memory after cutover. Neither Redis nor a separate worker is required.

## 4. Start services with the DAGs still paused

```bash
sudo cp ~/the-catalogue-data-pipeline/deploy/airflow/catalogue-airflow@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now catalogue-airflow@api-server catalogue-airflow@scheduler catalogue-airflow@dag-processor
sudo systemctl status catalogue-airflow@api-server catalogue-airflow@scheduler catalogue-airflow@dag-processor --no-pager
```

After the DAG processor has loaded the files, check:

```bash
~/airflow-venv/bin/airflow dags list
~/airflow-venv/bin/airflow dags list-import-errors
curl -fsS http://127.0.0.1:8080/api/v2/monitor/health
curl -fsS http://127.0.0.1:8000/health
```

Both DAGs should appear paused, with no import errors. Read the JSON health fields:
Airflow's metadata database, scheduler, and DAG processor should be healthy. An
absent triggerer is expected; these tasks do not use deferrable operators.

The Airflow API listens only on `127.0.0.1:8080`. Keep EC2 port 8080 closed.
From your **Mac**, open an SSH tunnel:

```bash
ssh -i ~/Downloads/the-catalogue-new-key-.pem \
  -N -L 8080:127.0.0.1:8080 ubuntu@13.207.212.188
```

Open http://localhost:8080 and log in as the user from step 3.

## 5. Switch scheduling from cron to Airflow

Do this outside the existing 02:30–03:30 run window, with no old job still running.
Do not trigger a real refresh just to test the dashboard while cron is active.

```bash
crontab -l > ~/airflow/crontab.before-airflow
crontab -e
```

Remove **only** the three Catalogue entries that run `pipeline.run`,
`notifier.check_alerts`, and `scripts/backup.sh`. Preserve unrelated entries. Verify
with `crontab -l`. Then, with the configuration from step 3 loaded:

```bash
~/airflow-venv/bin/airflow dags unpause catalogue_daily
~/airflow-venv/bin/airflow dags unpause catalogue_backup
```

The next daily refresh starts at 02:30 IST. Alerts start immediately after it
succeeds, rather than at a fixed 03:00. Backups remain independently scheduled at
03:30. `backup.sh` runs as `ubuntu` and needs passwordless
`sudo -u postgres pg_dump`, just as it did under cron; the service intentionally
allows that existing behavior. It keeps seven days of `brands` backups on EC2.
These dumps do not include the separate Airflow metadata database.

To roll back, first pause both DAGs, wait for running tasks to finish, stop and
disable all three Airflow services, then restore the three saved cron entries.
Do not restore the whole saved crontab if unrelated entries have changed since.

## Logs and updates

- Task output: Airflow dashboard → DAG → task → Logs; files under `~/airflow/logs`.
- Services: `journalctl -u catalogue-airflow@scheduler -u catalogue-airflow@dag-processor -u catalogue-airflow@api-server -f`.
- API and Caddy logs keep their existing locations. New scheduled task output is
  no longer appended to the old `logs/pipeline.log`, `alerts.log`, or `backup.log`.
- Task logs need periodic retention/cleanup; the existing app logrotate rules do
  not cover `~/airflow/logs`. Monitor disk use and remove expired task-log folders
  only after their tasks have finished. Back up Airflow's metadata separately if
  you need its run history and dashboard users after a server loss.
- For DAG-only updates, pull the pipeline repo; the DAG processor reloads the
  file. Pause DAGs and let tasks finish before updating CLI code or dependencies.
- For configuration changes, reload the env in your shell and restart all three
  services. Recopy the unit and run `systemctl daemon-reload` if the unit changes.

## Local verification

Normal pipeline tests skip the optional Airflow tests if Airflow is not installed.
Use a separate environment to test against the real Airflow package:

```bash
python3 -m venv /tmp/catalogue-airflow-tests
source /tmp/catalogue-airflow-tests/bin/activate
python_version="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install -r deploy/airflow/requirements.txt pytest==8.4.2 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-$python_version.txt"
AIRFLOW_HOME=/tmp/catalogue-airflow-test-home pytest -q tests/test_airflow_dags.py
```

The tests check dependencies, environment isolation, overlap/retry safeguards, and
the next scheduled times. They do not scrape stores or send Telegram messages.

Reference: Apache's [Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/installing-from-pypi.html)
and [production deployment guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/production-deployment.html).
