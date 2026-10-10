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
KEEP="${BACKUP_KEEP-14}"
if ! [[ "$KEEP" =~ ^[1-9][0-9]*$ ]]; then
    echo "backup: BACKUP_KEEP must be a positive integer, got '$KEEP'" >&2
    exit 2
fi
OUT="$DEST/sgw-admin-$(date -u +%Y%m%dT%H%M%SZ).db"

# Argument "rm" only removes the temp snapshot and its -wal/-shm sidecars;
# otherwise take the snapshot and print its path.
SNAPSHOT_PY='
import os, sqlite3, sys
src = os.environ.get("SGW_ADMIN_DB", "/data/sgw-admin.db")
dst = os.path.join(os.path.dirname(src), ".backup-tmp.db")
for suffix in ("", "-wal", "-shm"):
    if os.path.exists(dst + suffix):
        os.remove(dst + suffix)
if sys.argv[1:] == ["rm"]:
    sys.exit(0)
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
exec 9>"$DEST/.lock"
if ! flock -n 9; then
    echo "backup: another backup is already running" >&2
    exit 1
fi
cleanup() {
    docker exec "$CONTAINER" python -I -c "$SNAPSHOT_PY" rm >/dev/null 2>&1 || true
    rm -f "$OUT.part"
}
trap cleanup EXIT

snapshot="$(docker exec "$CONTAINER" python -I -c "$SNAPSHOT_PY")" || {
    echo "backup: container $CONTAINER not running or snapshot failed" >&2
    exit 1
}
docker exec "$CONTAINER" cat "$snapshot" >"$OUT.part" || {
    echo "backup: copying the snapshot out of $CONTAINER failed" >&2
    exit 1
}
python3 -I - "$OUT.part" <<'PY' || exit 1
import os
import sqlite3
import sys

path = sys.argv[1]
if os.path.getsize(path) == 0:
    sys.exit("backup: copy is empty")
try:
    conn = sqlite3.connect(path)
    result = conn.execute("PRAGMA integrity_check").fetchone()[0]
    tables = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
except sqlite3.DatabaseError as exc:
    sys.exit(f"backup: copy is not a valid database: {exc}")
if result != "ok":
    sys.exit(f"backup: integrity_check failed: {result}")
if tables == 0:
    sys.exit("backup: copy has no tables")
PY
chmod 600 "$OUT.part"
mv "$OUT.part" "$OUT"

# Retention: only files with the generated name, never the new one; the new one
# counts towards KEEP.
old=()
for f in "$DEST"/sgw-admin-*.db; do
    [[ "${f##*/}" =~ ^sgw-admin-[0-9]{8}T[0-9]{6}Z\.db$ ]] && [ "$f" != "$OUT" ] && old+=("$f")
done
if [ "${#old[@]}" -ge "$KEEP" ]; then
    printf '%s\n' "${old[@]}" | sort | head -n -"$((KEEP - 1))" | xargs -r rm -f --
fi
echo "Backup: $OUT"
