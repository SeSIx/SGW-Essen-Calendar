"""Backup der App-SQLite: konsistenter Snapshot aus dem Container, 14 Stück aufbewahren."""

import os
import sqlite3
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "admin" / "ops" / "backup_db.sh"

FAKE_DOCKER = r"""#!/usr/bin/env bash
# Stand-in for the docker CLI: runs `exec` locally against $FAKE_DATA, `cp` as a file copy.
set -euo pipefail
echo "$*" >> "$FAKE_LOG"
[ "${FAKE_FAIL:-}" = "1" ] && { echo "Error: No such container" >&2; exit 1; }
case "$1" in
  exec)
    shift 2
    [ "$1" = "python" ] && set -- python3 "${@:2}"
    SGW_ADMIN_DB="$FAKE_DATA/sgw-admin.db" exec "$@" ;;
  cp)
    cp "${2#*:}" "$3" ;;
  *) echo "unexpected: $*" >&2; exit 2 ;;
esac
"""


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(FAKE_DOCKER, encoding="utf-8")
    docker.chmod(0o755)
    data = tmp_path / "data"
    data.mkdir()
    conn = sqlite3.connect(data / "sgw-admin.db")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE users (login TEXT)")
    conn.execute("INSERT INTO users VALUES ('trainer')")
    conn.commit()
    conn.close()
    dest = tmp_path / "backups"
    return {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FAKE_DATA": str(data),
        "FAKE_LOG": str(tmp_path / "docker.log"),
        "BACKUP_DEST": str(dest),
    }


def _run(env):
    return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True,
                          check=False)


def test_backup_is_a_consistent_private_copy(env):
    result = _run(env)
    assert result.returncode == 0, result.stderr
    [backup] = Path(env["BACKUP_DEST"]).glob("sgw-admin-*.db")
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    assert stat.S_IMODE(Path(env["BACKUP_DEST"]).stat().st_mode) == 0o700
    rows = sqlite3.connect(backup).execute("SELECT login FROM users").fetchall()
    assert rows == [("trainer",)]
    assert f"Backup: {backup}" in result.stdout
    assert not (Path(env["FAKE_DATA"]) / ".backup-tmp.db").exists(), "temp snapshot removed"


def test_keeps_only_the_newest_14(env):
    dest = Path(env["BACKUP_DEST"])
    dest.mkdir(mode=0o700)
    for day in range(1, 16):
        (dest / f"sgw-admin-202601{day:02d}T000000Z.db").write_bytes(b"old")
    assert _run(env).returncode == 0
    kept = sorted(p.name for p in dest.glob("sgw-admin-*.db"))
    assert len(kept) == 14
    assert "sgw-admin-20260101T000000Z.db" not in kept
    assert "sgw-admin-20260102T000000Z.db" not in kept


def test_container_down_fails_without_partial_file(env):
    result = _run({**env, "FAKE_FAIL": "1"})
    assert result.returncode != 0
    dest = Path(env["BACKUP_DEST"])
    assert list(dest.glob("sgw-admin-*")) == []


def test_temp_snapshot_removed_when_copy_fails(env):
    """A failing `docker cp` must still remove the snapshot inside the container."""
    fake = Path(env["PATH"].split(":")[0]) / "docker"
    fake.write_text(fake.read_text().replace(
        "cp)\n    cp ", "cp)\n    exit 1\n    cp "), encoding="utf-8")
    result = _run(env)
    assert result.returncode != 0
    assert not (Path(env["FAKE_DATA"]) / ".backup-tmp.db").exists()
    assert list(Path(env["BACKUP_DEST"]).glob("sgw-admin-*")) == []
