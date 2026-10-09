"""Accounts, passwords, sessions and one-time links.

Secrets that grant access (session ids, link tokens) are only ever stored as
SHA-256 hashes, so a copy of the database file does not let anyone log in.
"""

import hashlib
import re
import secrets
import sqlite3
from dataclasses import dataclass
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
LOGIN_RE = re.compile(r"[a-z0-9-]{2,32}")

GENERIC_LOGIN_ERROR = "Name oder Passwort falsch"
TOO_MANY = "Zu viele Versuche – bitte in 15 Minuten erneut"

_hasher = PasswordHasher()  # argon2-cffi defaults: Argon2id
# Unknown names are checked against this hash so they take as long as real ones.
_DUMMY_HASH = _hasher.hash("timing-dummy-not-a-real-password")
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
    if not 1 <= len(display_name) <= 40 or any(ord(c) < 32 for c in display_name):
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
            "UPDATE users SET password_hash = ?, failed_logins = 0, locked_until = NULL, "
            "last_login_at = ? WHERE id = ?",
            (_hasher.hash(password), now, row["id"]))
        return _new_session(conn, row["id"], now)


def login(conn: sqlite3.Connection, login: str, password: str, ip: str, now: int) -> LoginResult:
    login = login.strip().lower()
    with db.transaction(conn):
        conn.execute("DELETE FROM login_attempts WHERE at <= ?", (now - IP_WINDOW,))
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
        (recent,) = conn.execute(
            "SELECT COUNT(*) FROM login_attempts WHERE ip = ?", (ip,)).fetchone()
        if recent >= IP_LIMIT:
            return LoginResult(None, TOO_MANY)
        conn.execute("INSERT INTO login_attempts (ip, at) VALUES (?, ?)", (ip, now))
        user = conn.execute(
            "SELECT id, password_hash, locked_until FROM users WHERE login = ?",
            (login,)).fetchone()

    if user is None or user["password_hash"] is None:
        _verify(_DUMMY_HASH, password)
        return LoginResult(None, GENERIC_LOGIN_ERROR)
    if user["locked_until"] is not None and user["locked_until"] > now:
        return LoginResult(None, TOO_MANY)

    ok = _verify(user["password_hash"], password)
    with db.transaction(conn):
        if not ok:
            conn.execute(
                "UPDATE users SET failed_logins = failed_logins + 1 WHERE id = ?", (user["id"],))
            conn.execute(
                "UPDATE users SET failed_logins = 0, locked_until = ? "
                "WHERE id = ? AND failed_logins >= ?",
                (now + LOCK_SECONDS, user["id"], LOCK_AFTER))
            return LoginResult(None, GENERIC_LOGIN_ERROR)
        if _hasher.check_needs_rehash(user["password_hash"]):
            conn.execute("UPDATE users SET password_hash = ? WHERE id = ?",
                         (_hasher.hash(password), user["id"]))
        conn.execute(
            "UPDATE users SET failed_logins = 0, locked_until = NULL, last_login_at = ? "
            "WHERE id = ?", (now, user["id"]))
        return LoginResult(_new_session(conn, user["id"], now), None)


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
