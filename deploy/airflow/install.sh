#!/usr/bin/env bash
# Run as ubuntu on EC2. Installs dependencies only; never starts jobs or edits cron.
set -euo pipefail

deploy_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
airflow_venv="${AIRFLOW_VENV:-$HOME/airflow-venv}"
airflow_home="${AIRFLOW_HOME:-$HOME/airflow}"

if [[ ! -x "$airflow_venv/bin/python" ]]; then
    python3 -m venv "$airflow_venv"
fi
python_version="$("$airflow_venv/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
case "$python_version" in
    3.10|3.11|3.12|3.13|3.14) ;;
    *) echo "Airflow 3.3.2 requires Python 3.10–3.14; found $python_version" >&2; exit 1 ;;
esac

"$airflow_venv/bin/python" -m pip install --no-cache-dir \
    -r "$deploy_dir/requirements.txt" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-$python_version.txt"
"$airflow_venv/bin/python" -m pip check

umask 077
mkdir -p "$airflow_home"
# Preserve an existing server configuration and its secrets on repeat installs.
if [[ ! -e "$airflow_home/catalogue.env" ]]; then
    cp "$deploy_dir/airflow.env.example" "$airflow_home/catalogue.env"
fi
printf 'Dependencies installed. Complete database/login setup in %s/README.md before starting services.\n' "$deploy_dir"
