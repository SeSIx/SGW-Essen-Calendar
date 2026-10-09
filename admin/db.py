"""The app's own SQLite file: accounts, sessions, one-time links, login attempts.

Events are not stored here. They live in custom_events.json on GitHub.
"""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id             INTEGER PRIMARY KEY,
    login          TEXT NOT NULL UNIQUE,
    display_name   TEXT NOT NULL,
    password_hash  TEXT,
    failed_logins  INTEGER NOT NULL DEFAULT 0,
    locked_until   INTEGER,
    created_at     INTEGER NOT NULL,
    last_login_at  INTEGER
);
CREATE TABLE IF NOT EXISTS sessions (
    id_hash     TEXT PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    csrf_token  TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tokens (
    token_hash  TEXT PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    purpose     TEXT NOT NULL CHECK (purpose IN ('invite', 'reset')),
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    used_at     INTEGER
);
CREATE TABLE IF NOT EXISTS login_attempts (
    id  INTEGER PRIMARY KEY,
    ip  TEXT NOT NULL,
    at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS login_attempts_ip_at ON login_attempts (ip, at);
"""


def connect(path: str) -> sqlite3.Connection:
    # Autocommit mode: every multi-statement change goes through transaction(),
    # which takes the write lock up front so two gunicorn workers cannot both
    # redeem the same link.
    conn = sqlite3.connect(path, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
