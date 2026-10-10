#!/usr/bin/env bash
# Daily backup of the SGW-Admin SQLite (accounts and sessions only; club dates
# live in GitHub). Run by sgw-admin-backup.timer.
#
# The snapshot is taken inside the container with SQLite's online backup API:
# gunicorn writes in WAL mode, and copying the volume file directly could catch
# a half-written page. The host has no sqlite3 CLI, and going through the
# container also avoids depending on the compose project's volume name.
# The container's root fs is read-only and /tmp is a 16 MB tmpfs, so the
# temporary snapshot lives next to the DB in /data and is always removed.
set -euo pipefail

CONTAINER="${CONTAINER:-sgw-admin}"
DEST="${BACKUP_DEST:-/root/backups/sgw-admin}"
KEEP="${BACKUP_KEEP:-14}"
OUT="$DEST/sgw-admin-$(date -u +%Y%m%dT%H%M%SZ).db"

# Argument "rm" only removes the temp snapshot; otherwise take it and print its path.
SNAPSHOT_PY='
import os, sqlite3, sys
src = os.environ.get("SGW_ADMIN_DB", "/data/sgw-admin.db")
dst = os.path.join(os.path.dirname(src), ".backup-tmp.db")
if sys.argv[1:] == ["rm"]:
    if os.path.exists(dst):
        os.remove(dst)
    sys.exit(0)
if os.path.exists(dst):
    os.remove(dst)
source, target = sqlite3.connect(src), sqlite3.connect(dst)
try:
    source.backup(target)
finally:
    target.close()
    source.close()
print(dst)
'

umask 077
install -d -m 700 "$DEST"
cleanup() {
    docker exec "$CONTAINER" python -I -c "$SNAPSHOT_PY" rm >/dev/null 2>&1 || true
    rm -f "$OUT.part"
}
trap cleanup EXIT

snapshot="$(docker exec "$CONTAINER" python -I -c "$SNAPSHOT_PY")"
docker cp "$CONTAINER:$snapshot" "$OUT.part"
python3 -I - "$OUT.part" <<'PY'
import sqlite3
import sys

result = sqlite3.connect(sys.argv[1]).execute("PRAGMA integrity_check").fetchone()[0]
sys.exit(0 if result == "ok" else f"integrity_check: {result}")
PY
chmod 600 "$OUT.part"
mv "$OUT.part" "$OUT"

ls -1 "$DEST"/sgw-admin-*.db | sort | head -n -"$KEEP" | xargs -r rm -f --
echo "Backup: $OUT"
