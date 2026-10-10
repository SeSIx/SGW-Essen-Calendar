"""Backup der App-SQLite: konsistenter Snapshot aus dem Container, 14 Stück aufbewahren."""

import os
import sqlite3
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "admin" / "ops" / "backup_db.sh"

FAKE_DOCKER = r"""#!/usr/bin/env bash
# Stand-in for the docker CLI: runs `exec` locally against $FAKE_DATA.
set -euo pipefail
echo "$*" >> "$FAKE_LOG"
[ "${FAKE_FAIL:-}" = "1" ] && { echo "Error: No such container" >&2; exit 1; }
case "$1" in
  exec)
    shift 2
    if [ "$1" = "cat" ]; then
      [ "${FAKE_CAT:-}" = "fail" ] && exit 1
      [ "${FAKE_CAT:-}" = "empty" ] && exit 0
      [ "${FAKE_CAT:-}" = "corrupt" ] && { echo "this is not a database" ; exit 0; }
    fi
    [ "$1" = "python" ] && set -- python3 "${@:2}"
    SGW_ADMIN_DB="$FAKE_DATA/sgw-admin.db" exec "$@" ;;
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


def _stamps(dest, names):
    dest.mkdir(mode=0o700, exist_ok=True)
    for name in names:
        (dest / name).write_bytes(b"old")


def test_keeps_only_the_newest_14(env):
    dest = Path(env["BACKUP_DEST"])
    _stamps(dest, [f"sgw-admin-202601{day:02d}T000000Z.db" for day in range(1, 16)])
    assert _run(env).returncode == 0
    kept = sorted(p.name for p in dest.glob("sgw-admin-*.db"))
    assert len(kept) == 14
    assert "sgw-admin-20260101T000000Z.db" not in kept
    assert "sgw-admin-20260102T000000Z.db" not in kept
    assert "sgw-admin-20260103T000000Z.db" in kept


def test_retention_ignores_foreign_files(env):
    dest = Path(env["BACKUP_DEST"])
    _stamps(dest, ["sgw-admin-manual.db", "notes.txt", "sgw-admin-2026010T000000Z.db"])
    _stamps(dest, [f"sgw-admin-202601{day:02d}T000000Z.db" for day in range(1, 15)])
    assert _run(env).returncode == 0
    for name in ("sgw-admin-manual.db", "notes.txt", "sgw-admin-2026010T000000Z.db"):
        assert (dest / name).exists(), name
    assert not (dest / "sgw-admin-20260101T000000Z.db").exists()


def test_future_dated_file_does_not_evict_the_new_backup(env):
    dest = Path(env["BACKUP_DEST"])
    _stamps(dest, [f"sgw-admin-209901{day:02d}T000000Z.db" for day in range(1, 15)])
    result = _run({**env, "BACKUP_KEEP": "3"})
    assert result.returncode == 0, result.stderr
    [new] = [p for p in dest.glob("sgw-admin-*.db") if not p.name.startswith("sgw-admin-2099")]
    assert f"Backup: {new}" in result.stdout
    assert len(list(dest.glob("sgw-admin-*.db"))) == 3


@pytest.mark.parametrize("keep", ["0", "-1", "abc", "", "1.5"])
def test_invalid_keep_is_rejected_before_anything_happens(env, keep):
    dest = Path(env["BACKUP_DEST"])
    _stamps(dest, ["sgw-admin-20260101T000000Z.db"])
    result = _run({**env, "BACKUP_KEEP": keep})
    assert result.returncode == 2
    assert "BACKUP_KEEP" in result.stderr
    assert not Path(env["FAKE_LOG"]).exists(), "no docker call"
    assert (dest / "sgw-admin-20260101T000000Z.db").exists()


def test_container_down_fails_with_message_and_no_partial_file(env):
    result = _run({**env, "FAKE_FAIL": "1"})
    assert result.returncode != 0
    assert "backup: container sgw-admin not running or snapshot failed" in result.stderr
    assert list(Path(env["BACKUP_DEST"]).glob("sgw-admin-*")) == []


@pytest.mark.parametrize("mode", ["empty", "corrupt"])
def test_bad_copy_is_rejected(env, mode):
    result = _run({**env, "FAKE_CAT": mode})
    assert result.returncode != 0
    assert "backup:" in result.stderr
    assert list(Path(env["BACKUP_DEST"]).glob("sgw-admin-*")) == []
    assert not (Path(env["FAKE_DATA"]) / ".backup-tmp.db").exists()


def test_copy_without_tables_is_rejected(env):
    empty = Path(env["FAKE_DATA"]) / "sgw-admin.db"
    empty.unlink()
    sqlite3.connect(empty).close()
    result = _run(env)
    assert result.returncode != 0
    assert "no tables" in result.stderr
    assert list(Path(env["BACKUP_DEST"]).glob("sgw-admin-*")) == []


def test_temp_snapshot_removed_when_copy_fails(env):
    result = _run({**env, "FAKE_CAT": "fail"})
    assert result.returncode != 0
    assert not (Path(env["FAKE_DATA"]) / ".backup-tmp.db").exists()
    assert list(Path(env["BACKUP_DEST"]).glob("sgw-admin-*")) == []


def test_temp_sidecars_are_removed_too(env):
    for suffix in ("-wal", "-shm"):
        (Path(env["FAKE_DATA"]) / f".backup-tmp.db{suffix}").write_bytes(b"x")
    assert _run(env).returncode == 0
    assert list(Path(env["FAKE_DATA"]).glob(".backup-tmp.db*")) == []


def test_second_run_is_refused_while_lock_is_held(env):
    import fcntl

    dest = Path(env["BACKUP_DEST"])
    dest.mkdir(mode=0o700)
    with open(dest / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        result = _run(env)
    assert result.returncode == 1
    assert "already running" in result.stderr
    assert not Path(env["FAKE_LOG"]).exists()


def test_missing_source_fails_and_creates_no_database(env):
    (Path(env["FAKE_DATA"]) / "sgw-admin.db").unlink()
    result = _run(env)
    assert result.returncode != 0
    assert list(Path(env["FAKE_DATA"]).iterdir()) == []
    assert list(Path(env["BACKUP_DEST"]).glob("sgw-admin-*")) == []
