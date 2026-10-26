#!/usr/bin/env bash
# Daily backup of the `brands` database. Keeps the last 7 days.
# Cron (IST): 30 3 * * *  ~/the-catalogue-data-pipeline/scripts/backup.sh >> ~/the-catalogue-data-pipeline/logs/backup.log 2>&1
#
# Runs pg_dump as the `postgres` OS user, because only the admin can read both
# schemas (`pipeline` cannot read app.*). On Ubuntu the `ubuntu` user can sudo without a password.
# Note: backups stay on the same server. Copy them somewhere else now and then.
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-$HOME/backups}"
KEEP_DAYS=7
DB_NAME="brands"

mkdir -p "$BACKUP_DIR"
file="$BACKUP_DIR/${DB_NAME}-$(date +%F).dump"

# Custom format (-Fc) is compressed. Restore with: pg_restore -d brands <file>
sudo -u postgres pg_dump -Fc "$DB_NAME" > "$file.tmp"
mv "$file.tmp" "$file"
echo "$(date '+%F %T') backup ok: $file ($(du -h "$file" | cut -f1))"

# Remove backups older than KEEP_DAYS days.
find "$BACKUP_DIR" -name "${DB_NAME}-*.dump" -type f -mtime +$((KEEP_DAYS - 1)) -print -delete
