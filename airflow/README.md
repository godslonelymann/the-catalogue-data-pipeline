# Daily catalogue workflow

`catalogue_daily` runs at **02:30 Asia/Kolkata** (`21:00 UTC` on the previous date).
It disables catchup and permits one run at a time. Dynamic task groups give each
active brand its own Fetch → Validate → Clean → Save chain. Two task slots and a
two-slot brand pool limit work; brands can continue when a different brand fails.
The final tasks read committed completions, send scoped restock notifications,
report every brand, and preserve failed task/run states.

Only run IDs and brand manifests use XCom. Payloads stay in `pipeline.brand_runs`
in the catalogue database. Raw downloads, deduplicated validation, and cleaned
snapshots are committed independently. Decimal prices and timezone-aware dates
round-trip through staging without floats. Saving rechecks the count safeguard
and commits catalogue writes plus completion in one transaction. Clearing a
completed save is a no-op, including its missing-product counts. An unfinished
snapshot older than a saved snapshot is rejected. Use a new run to fetch fresh
data; clearing tasks in an existing run deliberately reuses its checkpoints.

## Test before switching production

Use empty, throwaway databases: the test fixtures drop/recreate schemas. Pipeline
and backend tests must use different databases. The pipeline task tests import the
backend notifier, so add that repository to `PYTHONPATH`:

```sh
# From the pipeline repo, after installing its normal requirements:
TEST_DATABASE_URL=postgresql:///catalogue_pipeline_test \
  PYTHONPATH=.:../the-catalogue-backend .venv/bin/pytest -q
# From the backend repo:
TEST_DATABASE_URL=postgresql:///catalogue_backend_test .venv/bin/pytest -q
```

With Airflow installed, run the DAG validation tests too:

```sh
PYTHONPATH=.:../the-catalogue-backend /srv/catalogue/airflow-venv/bin/pytest -q tests/test_dag.py
```

To run the five in-process Airflow scheduling scenarios too, create an isolated
metadata database first (SQLite is sufficient for these tests, not deployment):

```sh
export AIRFLOW_HOME=/tmp/catalogue-airflow-tests
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/catalogue-airflow-tests/airflow.db
export AIRFLOW__CORE__LOAD_EXAMPLES=False
/srv/catalogue/airflow-venv/bin/airflow db migrate
TEST_AIRFLOW_DATABASE_URL="$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" \
  PYTHONPATH=.:../the-catalogue-backend \
  /srv/catalogue/airflow-venv/bin/pytest -q tests/test_dag.py
```

Use a separate shell for this test setup so its environment does not override
your deployment configuration.

For a staging rehearsal, point **both** catalogue credentials below to a test
catalogue database seeded with test brands/alerts. Use a test Telegram bot/chat.
Trigger the DAG manually, inject a fetch or cleaning failure for one brand, verify
other brands save, clear failed tasks, and confirm completed saves do not repeat
missing counts or delivery attempts. Check the Graph view and final run status.

## Isolated Airflow environment (Ubuntu / systemd)

This deployment uses Airflow **3.3.2**, Python **3.11**, and LocalExecutor. It does
not install Airflow into the API or pipeline's existing virtual environments.
The existing server uses Python 3.14; install a separate Python 3.11 interpreter
before proceeding. Do not install the backend's pinned FastAPI requirements in
Airflow's environment: the notifier only needs the HTTP/database/dotenv packages
already supplied by the pipeline requirements.

Keep both checked-out repositories at:

```
/srv/catalogue/the-catalogue-pipeline
/srv/catalogue/the-catalogue-backend
```

Create the service user and writable state directory (code stays read-only to the
service account):

```sh
sudo useradd --system --home-dir /srv/catalogue/airflow --shell /usr/sbin/nologin catalogue-airflow
sudo install -d -o catalogue-airflow -g catalogue-airflow -m 700 /srv/catalogue/airflow
sudo install -d -o root -g catalogue-airflow -m 750 /etc/catalogue
sudo python3.11 -m venv /srv/catalogue/airflow-venv
sudo /srv/catalogue/airflow-venv/bin/pip install \
  'apache-airflow[postgres]==3.3.2' \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt
sudo /srv/catalogue/airflow-venv/bin/pip install \
  'apache-airflow[postgres]==3.3.2' \
  -r /srv/catalogue/the-catalogue-pipeline/requirements.txt
sudo /srv/catalogue/airflow-venv/bin/pip check
```

As the PostgreSQL admin, create a **separate metadata database and owner**. Supply
real passwords interactively, never in committed files:

```sql
CREATE ROLE airflow LOGIN;
\password airflow
CREATE DATABASE airflow_metadata OWNER airflow;
```

Apply these two migrations to the **catalogue database**, not `airflow_metadata`:

```sh
sudo -u postgres psql -v ON_ERROR_STOP=1 -d brands \
  -f /srv/catalogue/the-catalogue-pipeline/sql/airflow_pipeline_schema.sql
sudo -u postgres psql -v ON_ERROR_STOP=1 -d brands \
  -f /srv/catalogue/the-catalogue-backend/sql/alert_delivery_schema.sql
```

The first adds `pipeline.runs` and `pipeline.brand_runs`, accessible only to the
pipeline role. The second adds `app.alert_deliveries`, accessible to `app_user`.
The pipeline still cannot access `app.*`; the notifier still cannot write
`catalog.*`. Airflow's metadata role receives no catalogue privileges.

Copy `airflow.env.example` to `/etc/catalogue/airflow.env`. Fill in the three
separate database URLs (percent-encode special characters in passwords), Telegram
values, JWT secret, and Fernet key. Keep URLs pointed to the test databases until
the rehearsal passes. Generate keys with:

```sh
/srv/catalogue/airflow-venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(48))'
/srv/catalogue/airflow-venv/bin/python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
sudo chown catalogue-airflow:catalogue-airflow /etc/catalogue/airflow.env
sudo chmod 600 /etc/catalogue/airflow.env
```

Set `passwords.json` to `{"admin":"<a long random password>"}` in
`/srv/catalogue/airflow/`, owned by the service account with mode 600. SimpleAuth
is appropriate for this single-user, loopback-only dashboard. Never enable its
all-admin bypass. Give the service account read/traverse access to both repos.

Initialize using the same environment as the services:

```sh
sudo -u catalogue-airflow bash -c '
  set -a
  . /etc/catalogue/airflow.env
  set +a
  /srv/catalogue/airflow-venv/bin/airflow db migrate
  /srv/catalogue/airflow-venv/bin/airflow pools set catalogue_brands 2 "Catalogue brand tasks"
  /srv/catalogue/airflow-venv/bin/airflow pools set catalogue_telegram 1 "Serialized Telegram sender"
'
sudo cp /srv/catalogue/the-catalogue-pipeline/airflow/systemd/catalogue-airflow@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now catalogue-airflow@api-server catalogue-airflow@scheduler catalogue-airflow@dag-processor
```

The API server listens on **127.0.0.1:8080**. Keep its security-group port closed.
Open the dashboard with an SSH tunnel to the server:

```sh
ssh -L 8080:127.0.0.1:8080 ubuntu@<server>
```

Visit `http://localhost:8080`, log in, and manually trigger the paused DAG.
Inspect services with `systemctl status catalogue-airflow@{api-server,scheduler,dag-processor}`
and `journalctl -u catalogue-airflow@scheduler`; task logs live under
`/srv/catalogue/airflow/logs`. Include this state and metadata DB in backups;
the existing catalogue backup covers the new staging/delivery tables.

## Capacity and switchover

Check `free -h`, `df -h`, PostgreSQL connection usage, and peak RSS during a full
test run. The existing approximately 4 GB server also runs PostgreSQL and the API;
measure headroom and resize if necessary. Start with two workers/tasks, one API
worker, one DAG parser. Reduce the pool/task parallelism to one if memory is tight.
Do not enable Celery/Redis for this single-server workflow.

After the test rehearsal, stop the three Airflow services, change credentials to
the production catalogue database, apply migrations there, and restart services.
Keep the DAG paused. Wait for any old cron jobs to finish, remove **both** the
02:30 pipeline cron and 03:00 notifier cron entries, then unpause the DAG. Keep
the 03:30 backup cron. Verify the next scheduled run, failed-task visibility,
Telegram summary, and the delivery ledger. Do not run the old pipeline CLI
alongside Airflow: it has no workflow completion checkpoints. For rollback,
pause the DAG, let in-flight tasks finish, stop Airflow, then restore the old
cron schedule; retain the delivery migration and updated notifier.

## Delivery uncertainty and recovery

`app.alert_deliveries` records `sending`, `sent`, `failed`, or `uncertain`, attempt
counts, timestamps, and Telegram message IDs. One database advisory lock protects
all notifier callers, including manual CLI runs; the one-slot Airflow pool also
serializes restock sends and summaries. Explicit rejection allows retry; timeout,
transport failure, malformed acknowledgement, or HTTP 5xx becomes `uncertain`.
A worker that dies with a `sending` claim is also recovered as `uncertain`.
There is no Telegram idempotency key: this avoids automatic duplicate sends but
requires operator review when acceptance cannot be determined.

Inspect `SELECT * FROM app.alert_deliveries WHERE status IN ('sending','uncertain','failed');`
and check the Telegram chat. If the message arrived, mark its delivery `sent` and
its alert `fired` together in an admin transaction. If absence is established,
change delivery status to `failed` and clear/retry the notification task. Do not
reset uncertain deliveries blindly. Airflow records summary attempts in
`pipeline.runs.summary_delivery` using the same no-automatic-resend policy.

Staging snapshots are retained for diagnosis/retries. Monitor table size and use
an admin maintenance job to clear `raw`, `validated`, and `cleaned` only for
finished runs older than your retention window (for example 14 days). Keep run,
brand completion/result, and delivery rows indefinitely so old retries stay safe.
Clearing old unfinished snapshots requires starting a new run.

References: [Airflow mapping](https://airflow.apache.org/docs/apache-airflow/3.3.2/authoring-and-scheduling/dynamic-task-mapping.html),
[run status and leaf tasks](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/dag-run.html),
[isolated installation and constraints](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/installing-from-pypi.html).
