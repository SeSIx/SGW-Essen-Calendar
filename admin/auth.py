"""Accounts, passwords, sessions and one-time links.

Secrets that grant access (session ids, link tokens) are only ever stored as
SHA-256 hashes, so a copy of the database file does not let anyone log in.
"""

import hashlib
import re
import secrets
import sqlite3
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

from admin import db

SESSION_TTL = 30 * 24 * 3600
TOKEN_TTL = 48 * 3600
LOCK_AFTER = 10
LOCK_SECONDS = 15 * 60
IP_LIMIT = 20
IP_WINDOW = 15 * 60
MAX_USERS = 3
PASSWORD_MIN = 12
PASSWORD_MAX = 128
LOGIN_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,31}")
LINK_PURPOSES = ("invite", "reset")
_BAD_NAME_CATEGORIES = ("Cc", "Cf", "Zl", "Zp")  # controls, bidi/format marks, line/para separators

GENERIC_LOGIN_ERROR = "Name oder Passwort falsch"
TOO_MANY = "Zu viele Versuche – bitte in 15 Minuten erneut"

_hasher = PasswordHasher()  # argon2-cffi defaults: Argon2id


@lru_cache(maxsize=1)
def _dummy_hash() -> str:
    """Unknown names are checked against this hash so they take as long as real ones.

    Computed on first use: hashing at import would cost every worker start-up time.
    """
    return _hasher.hash("timing-dummy-not-a-real-password")


_COMMON = frozenset(
    line.strip().lower()
    for line in (Path(__file__).parent / "data" / "common-passwords.txt")
    .read_text(encoding="utf-8").splitlines()
    if line.strip()
)


class AuthError(Exception):
    """A problem reported to a person (CLI or form), in German."""


class InvalidLink(AuthError):
    pass


class PasswordRejected(AuthError):
    pass


@dataclass(frozen=True)
class SessionInfo:
    user_id: int
    login: str
    display_name: str
    csrf_token: str


@dataclass(frozen=True)
class LoginResult:
    session: str | None
    error: str | None


def sha256(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def password_problem(password: str, login: str) -> str | None:
    if len(password) < PASSWORD_MIN:
        return f"Mindestens {PASSWORD_MIN} Zeichen"
    if len(password) > PASSWORD_MAX:
        return f"Höchstens {PASSWORD_MAX} Zeichen"
    if password.lower() in _COMMON:
        return "Dieses Passwort ist zu verbreitet – bitte ein anderes wählen"
    if login and login.lower() in password.lower():
        return "Das Passwort darf deinen Namen nicht enthalten"
    return None


def _verify(stored: str, password: str) -> bool:
    try:
        return _hasher.verify(stored, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def create_user(conn: sqlite3.Connection, login: str, display_name: str, now: int) -> int:
    if not LOGIN_RE.fullmatch(login):
        raise AuthError("Login: 2–32 Zeichen, nur a–z, 0–9 und -")
    display_name = display_name.strip()
    if not 1 <= len(display_name) <= 40 or any(
            unicodedata.category(c) in _BAD_NAME_CATEGORIES for c in display_name):
        raise AuthError("Anzeigename: 1–40 Zeichen, ohne Steuerzeichen")
    with db.transaction(conn):
        (count,) = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        if conn.execute("SELECT 1 FROM users WHERE login = ?", (login,)).fetchone():
            raise AuthError(f"Konto „{login}“ existiert bereits")
        if count >= MAX_USERS:
            raise AuthError(f"Es sind bereits {MAX_USERS} Konten angelegt")
        cur = conn.execute(
            "INSERT INTO users (login, display_name, created_at) VALUES (?, ?, ?)",
            (login, display_name, now))
    return cur.lastrowid


def get_user(conn: sqlite3.Connection, login: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM users WHERE login = ?", (login,)).fetchone()


def issue_token(conn: sqlite3.Connection, login: str, purpose: str, now: int) -> str:
    """Create a one-time link token; any older link of this account stops working.

    A reset also ends all sessions and clears the password, so a leaked password
    is useless from the moment the reset is issued.
    """
    if purpose not in LINK_PURPOSES:
        raise AuthError("Unbekannter Linktyp")
    raw = secrets.token_urlsafe(32)
    with db.transaction(conn):
        user = conn.execute(
            "SELECT id, password_hash FROM users WHERE login = ?", (login,)).fetchone()
        if user is None:
            raise AuthError(f"Kein Konto „{login}“")
        if purpose == "invite" and user["password_hash"] is not None:
            raise AuthError(f"„{login}“ hat schon ein Passwort – nutze reset")
        conn.execute("DELETE FROM tokens WHERE user_id = ?", (user["id"],))
        if purpose == "reset":
            conn.execute("DELETE FROM sessions WHERE user_id = ?", (user["id"],))
            conn.execute("UPDATE users SET password_hash = NULL WHERE id = ?", (user["id"],))
        conn.execute(
            "INSERT INTO tokens (token_hash, user_id, purpose, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (sha256(raw), user["id"], purpose, now, now + TOKEN_TTL))
    return raw


def peek_token(conn: sqlite3.Connection, raw: str, now: int) -> sqlite3.Row | None:
    """Look a link up without using it — messenger previews fetch links too."""
    return conn.execute(
        "SELECT t.purpose, u.login, u.display_name FROM tokens t "
        "JOIN users u ON u.id = t.user_id "
        "WHERE t.token_hash = ? AND t.used_at IS NULL AND t.expires_at > ?",
        (sha256(raw), now)).fetchone()


def _new_session(conn: sqlite3.Connection, user_id: int, now: int) -> str:
    raw = secrets.token_urlsafe(32)  # 32 random bytes
    conn.execute(
        "INSERT INTO sessions (id_hash, user_id, csrf_token, created_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (sha256(raw), user_id, secrets.token_urlsafe(32), now, now + SESSION_TTL))
    return raw


def redeem_token(conn: sqlite3.Connection, raw: str, password: str, now: int) -> str:
    """Set the password and log in, all in one transaction. Returns the session id."""
    with db.transaction(conn):
        row = conn.execute(
            "SELECT t.token_hash, u.id, u.login FROM tokens t JOIN users u ON u.id = t.user_id "
            "WHERE t.token_hash = ? AND t.used_at IS NULL AND t.expires_at > ?",
            (sha256(raw), now)).fetchone()
        if row is None:
            raise InvalidLink("Dieser Link ist ungültig oder abgelaufen")
        problem = password_problem(password, row["login"])
        if problem:
            raise PasswordRejected(problem)
        conn.execute("UPDATE tokens SET used_at = ? WHERE token_hash = ?", (now, row["token_hash"]))
        conn.execute(
            "UPDATE users SET password_hash = ?, last_login_at = ? WHERE id = ?",
            (_hasher.hash(password), now, row["id"]))
        return _new_session(conn, row["id"], now)


def _locked_until(conn: sqlite3.Connection, name: str) -> int | None:
    row = conn.execute(
        "SELECT locked_until FROM login_failures WHERE login = ?", (name,)).fetchone()
    return row["locked_until"] if row else None


def login(conn: sqlite3.Connection, login: str, password: str, ip: str, now: int) -> LoginResult:
    """Failures and locks are keyed by the name typed, so unknown names behave the same."""
    name = login.strip().lower()
    with db.transaction(conn):
        conn.execute("DELETE FROM login_attempts WHERE at <= ?", (now - IP_WINDOW,))
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
        conn.execute(
            "DELETE FROM login_failures WHERE locked_until IS NOT NULL AND locked_until <= ?",
            (now,))
        (recent,) = conn.execute(
            "SELECT COUNT(*) FROM login_attempts WHERE ip = ?", (ip,)).fetchone()
        if recent >= IP_LIMIT:
            return LoginResult(None, TOO_MANY)
        conn.execute("INSERT INTO login_attempts (ip, at) VALUES (?, ?)", (ip, now))
        locked = _locked_until(conn, name)
        user = conn.execute(
            "SELECT id, password_hash FROM users WHERE login = ?", (name,)).fetchone()

    if locked is not None and locked > now:
        _verify(_dummy_hash(), password)
        return LoginResult(None, TOO_MANY)
    stored = user["password_hash"] if user is not None else None
    if stored is None:
        _verify(_dummy_hash(), password)
        ok = False
    else:
        ok = _verify(stored, password)

    with db.transaction(conn):
        # Re-read: a reset or a lock may have landed while the hash was checked.
        locked = _locked_until(conn, name)
        if locked is not None and locked > now:
            return LoginResult(None, TOO_MANY)
        current = conn.execute(
            "SELECT id, password_hash FROM users WHERE login = ?", (name,)).fetchone()
        if not ok or current is None or current["password_hash"] != stored:
            conn.execute(
                "INSERT INTO login_failures (login, failures) VALUES (?, 1) "
                "ON CONFLICT(login) DO UPDATE SET failures = failures + 1", (name,))
            conn.execute(
                "UPDATE login_failures SET failures = 0, locked_until = ? "
                "WHERE login = ? AND failures >= ?",
                (now + LOCK_SECONDS, name, LOCK_AFTER))
            return LoginResult(None, GENERIC_LOGIN_ERROR)
        conn.execute("DELETE FROM login_failures WHERE login = ?", (name,))
        if _hasher.check_needs_rehash(stored):
            conn.execute("UPDATE users SET password_hash = ? WHERE id = ?",
                         (_hasher.hash(password), current["id"]))
        conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (now, current["id"]))
        return LoginResult(_new_session(conn, current["id"], now), None)


def load_session(conn: sqlite3.Connection, raw: str, now: int) -> SessionInfo | None:
    row = conn.execute(
        "SELECT s.user_id, s.csrf_token, u.login, u.display_name FROM sessions s "
        "JOIN users u ON u.id = s.user_id WHERE s.id_hash = ? AND s.expires_at > ?",
        (sha256(raw), now)).fetchone()
    if row is None:
        return None
    return SessionInfo(row["user_id"], row["login"], row["display_name"], row["csrf_token"])


def logout(conn: sqlite3.Connection, raw: str) -> None:
    conn.execute("DELETE FROM sessions WHERE id_hash = ?", (sha256(raw),))


def logout_all(conn: sqlite3.Connection, user_id: int) -> int:
    return conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,)).rowcount


def list_users(conn: sqlite3.Connection, now: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT u.login, u.display_name, u.password_hash IS NOT NULL AS has_password, "
        "u.last_login_at, (SELECT COUNT(*) FROM sessions s "
        " WHERE s.user_id = u.id AND s.expires_at > ?) AS sessions "
        "FROM users u ORDER BY u.login", (now,)).fetchall()
