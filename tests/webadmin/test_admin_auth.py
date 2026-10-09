"""Accounts, sessions and one-time links — the rules people actually hit."""

import sqlite3

import pytest

from admin import auth, db

NOW = 1_791_540_000
PW = "Wasserball-Essen-2026!"


@pytest.fixture
def conn(tmp_path):
    c = db.connect(str(tmp_path / "a.sqlite3"))
    db.init_schema(c)
    yield c
    c.close()


def _account(conn, login="trainer", name="Trainer", password=PW, now=NOW):
    auth.create_user(conn, login, name, now)
    raw = auth.issue_token(conn, login, "invite", now)
    return auth.redeem_token(conn, raw, password, now)


def test_wal_mode_is_on(conn):
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


@pytest.mark.parametrize("pw, problem", [
    ("kurz", "Mindestens 12 Zeichen"),
    ("x" * 129, "Höchstens 128 Zeichen"),
    ("trainer-hat-ein-langes-pw", "Das Passwort darf deinen Namen nicht enthalten"),
])
def test_password_rules(pw, problem):
    assert auth.password_problem(pw, "trainer") == problem


def test_common_password_is_rejected():
    long_common = sorted(p for p in auth._COMMON if len(p) >= 12)
    assert long_common, "the bundled list must contain passwords of 12+ characters"
    assert auth.password_problem(long_common[0], "x") == \
        "Dieses Passwort ist zu verbreitet – bitte ein anderes wählen"


@pytest.mark.parametrize("pw", ["passwort1234", "qwertzuiop12", "fussball2026", "QWERTZUIOP12"])
def test_club_and_german_common_passwords_are_rejected(pw):
    assert auth.password_problem(pw, "x") == \
        "Dieses Passwort ist zu verbreitet – bitte ein anderes wählen"


def test_seclists_long_common_password_is_rejected():
    assert auth.password_problem("password123456789", "x") == \
        "Dieses Passwort ist zu verbreitet – bitte ein anderes wählen"


def test_random_passphrase_is_accepted():
    assert auth.password_problem("vT9#qL2mXr8!bNwz", "trainer") is None


def test_good_password_passes():
    assert auth.password_problem(PW, "trainer") is None


def test_login_rules_and_user_limit(conn):
    with pytest.raises(auth.AuthError, match="Login"):
        auth.create_user(conn, "Trainer!", "Trainer", NOW)
    for login in ("a1", "b2", "c3"):
        auth.create_user(conn, login, login.upper(), NOW)
    with pytest.raises(auth.AuthError, match="bereits 3 Konten"):
        auth.create_user(conn, "d4", "D4", NOW)


def test_duplicate_login_is_refused(conn):
    auth.create_user(conn, "trainer", "Trainer", NOW)
    with pytest.raises(auth.AuthError, match="existiert bereits"):
        auth.create_user(conn, "trainer", "Trainer", NOW)


def test_tokens_are_stored_hashed_and_single_use(conn):
    auth.create_user(conn, "trainer", "Trainer", NOW)
    raw = auth.issue_token(conn, "trainer", "invite", NOW)
    stored = conn.execute("SELECT token_hash FROM tokens").fetchone()[0]
    assert stored == auth.sha256(raw) and raw not in stored
    assert auth.peek_token(conn, raw, NOW)["display_name"] == "Trainer"
    assert auth.peek_token(conn, raw, NOW) is not None, "peeking must not consume"
    auth.redeem_token(conn, raw, PW, NOW)
    with pytest.raises(auth.InvalidLink):
        auth.redeem_token(conn, raw, PW, NOW)


def test_token_expires_after_48_hours(conn):
    auth.create_user(conn, "trainer", "Trainer", NOW)
    raw = auth.issue_token(conn, "trainer", "invite", NOW)
    assert auth.peek_token(conn, raw, NOW + auth.TOKEN_TTL) is None
    with pytest.raises(auth.InvalidLink):
        auth.redeem_token(conn, raw, PW, NOW + auth.TOKEN_TTL)


def test_rejected_password_keeps_the_link_usable(conn):
    auth.create_user(conn, "trainer", "Trainer", NOW)
    raw = auth.issue_token(conn, "trainer", "invite", NOW)
    with pytest.raises(auth.PasswordRejected, match="Mindestens"):
        auth.redeem_token(conn, raw, "kurz", NOW)
    assert auth.redeem_token(conn, raw, PW, NOW)


def test_new_link_replaces_old_one(conn):
    auth.create_user(conn, "trainer", "Trainer", NOW)
    first = auth.issue_token(conn, "trainer", "invite", NOW)
    second = auth.issue_token(conn, "trainer", "invite", NOW)
    assert auth.peek_token(conn, first, NOW) is None
    assert auth.peek_token(conn, second, NOW) is not None


def test_invite_for_account_with_password_is_refused(conn):
    _account(conn)
    with pytest.raises(auth.AuthError, match="reset"):
        auth.issue_token(conn, "trainer", "invite", NOW)


def test_reset_ends_sessions_and_old_password(conn):
    session = _account(conn)
    auth.issue_token(conn, "trainer", "reset", NOW)
    assert auth.load_session(conn, session, NOW) is None
    assert auth.login(conn, "trainer", PW, "1.1.1.1", NOW).error == auth.GENERIC_LOGIN_ERROR


def test_login_success_creates_hashed_session(conn):
    _account(conn)
    result = auth.login(conn, "trainer", PW, "1.1.1.1", NOW)
    assert result.error is None and result.session
    stored = [r[0] for r in conn.execute("SELECT id_hash FROM sessions")]
    assert auth.sha256(result.session) in stored and result.session not in stored
    info = auth.load_session(conn, result.session, NOW)
    assert (info.login, info.display_name) == ("trainer", "Trainer") and info.csrf_token


def test_login_name_is_case_insensitive(conn):
    _account(conn)
    assert auth.login(conn, " Trainer ", PW, "1.1.1.1", NOW).session


def test_unknown_user_gets_generic_error_and_still_hashes(conn, monkeypatch):
    calls = []
    real = auth._verify
    monkeypatch.setattr(auth, "_verify", lambda h, p: calls.append(h) or real(h, p))
    result = auth.login(conn, "niemand", PW, "1.1.1.1", NOW)
    assert result.error == auth.GENERIC_LOGIN_ERROR
    assert calls == [auth._dummy_hash()]


def test_dummy_hash_is_computed_on_first_use_and_cached(monkeypatch):
    assert not hasattr(auth, "_DUMMY_HASH"), "no Argon2 work at import time"
    auth._dummy_hash.cache_clear()
    made = []
    real = auth._hasher

    class Counting:
        def hash(self, password):
            made.append(password)
            return real.hash(password)

    monkeypatch.setattr(auth, "_hasher", Counting())
    first = auth._dummy_hash()
    assert auth._dummy_hash() == first and len(made) == 1


def test_account_locks_after_ten_failures_for_15_minutes(conn):
    _account(conn)
    for i in range(auth.LOCK_AFTER):
        assert auth.login(conn, "trainer", "falsch", f"10.0.0.{i}", NOW).error == auth.GENERIC_LOGIN_ERROR
    assert auth.login(conn, "trainer", PW, "10.0.1.1", NOW).error == auth.TOO_MANY
    assert auth.login(conn, "trainer", PW, "10.0.1.1", NOW + auth.LOCK_SECONDS).session


def test_success_resets_failure_counter(conn):
    _account(conn)
    for _ in range(auth.LOCK_AFTER - 1):
        auth.login(conn, "trainer", "falsch", "10.0.0.1", NOW)
    assert auth.login(conn, "trainer", PW, "10.0.0.2", NOW).session
    assert conn.execute("SELECT COUNT(*) FROM login_failures").fetchone()[0] == 0


def test_ip_limit_is_20_per_15_minutes_and_persisted(conn, tmp_path):
    for _ in range(auth.IP_LIMIT):
        auth.login(conn, "niemand", "x", "9.9.9.9", NOW)
    reopened = db.connect(str(tmp_path / "a.sqlite3"))
    assert auth.login(reopened, "niemand", "x", "9.9.9.9", NOW).error == auth.TOO_MANY
    assert auth.login(reopened, "niemand", "x", "9.9.9.9", NOW + auth.IP_WINDOW).error == auth.GENERIC_LOGIN_ERROR
    reopened.close()


def test_session_lasts_30_days(conn):
    session = _account(conn)
    assert auth.load_session(conn, session, NOW + auth.SESSION_TTL - 1)
    assert auth.load_session(conn, session, NOW + auth.SESSION_TTL) is None


def test_logout_and_logout_all(conn):
    first = _account(conn)
    second = auth.login(conn, "trainer", PW, "1.1.1.1", NOW).session
    auth.logout(conn, first)
    assert auth.load_session(conn, first, NOW) is None
    assert auth.load_session(conn, second, NOW)
    user_id = auth.load_session(conn, second, NOW).user_id
    assert auth.logout_all(conn, user_id) == 1
    assert auth.load_session(conn, second, NOW) is None


def test_list_users(conn):
    _account(conn)
    auth.create_user(conn, "kapitaen", "Kapitän", NOW)
    rows = {r["login"]: r for r in auth.list_users(conn, NOW)}
    assert rows["trainer"]["has_password"] == 1 and rows["trainer"]["sessions"] == 1
    assert rows["kapitaen"]["has_password"] == 0


class _FlakyConn:
    """Stands in for a connection; the named statements raise on purpose."""

    def __init__(self, conn, fail_on):
        self.conn = conn
        self.fail_on = fail_on
        self.seen = []

    def execute(self, sql, *args):
        self.seen.append(sql)
        if sql in self.fail_on:
            raise sqlite3.OperationalError(f"{sql} failed")
        return self.conn.execute(sql, *args)


def test_commit_failure_rolls_back_and_reraises(conn):
    flaky = _FlakyConn(conn, fail_on={"COMMIT"})
    with pytest.raises(sqlite3.OperationalError, match="COMMIT failed"), db.transaction(flaky):
        conn.execute("INSERT INTO login_attempts (ip, at) VALUES ('x', 1)")
    assert "ROLLBACK" in flaky.seen
    assert conn.execute("SELECT COUNT(*) FROM login_attempts").fetchone()[0] == 0


def test_failing_rollback_does_not_hide_the_original_error(conn):
    flaky = _FlakyConn(conn, fail_on={"ROLLBACK"})
    with pytest.raises(ValueError, match="original"), db.transaction(flaky):
        raise ValueError("original")
    conn.execute("ROLLBACK")


def test_unknown_token_purpose_is_refused(conn):
    auth.create_user(conn, "trainer", "Trainer", NOW)
    with pytest.raises(auth.AuthError, match="Linktyp"):
        auth.issue_token(conn, "trainer", "hack", NOW)


@pytest.mark.parametrize("name", ["Tr‮ainer", "A\x7fB", "A B"])
def test_display_name_rejects_control_and_format_characters(conn, name):
    with pytest.raises(auth.AuthError, match="Anzeigename"):
        auth.create_user(conn, "trainer", name, NOW)


def test_login_may_not_start_with_a_hyphen(conn):
    with pytest.raises(auth.AuthError, match="Login"):
        auth.create_user(conn, "-x", "Trainer", NOW)


def test_invited_but_unredeemed_account_gets_generic_error(conn):
    auth.create_user(conn, "trainer", "Trainer", NOW)
    auth.issue_token(conn, "trainer", "invite", NOW)
    assert auth.login(conn, "trainer", PW, "1.1.1.1", NOW).error == auth.GENERIC_LOGIN_ERROR


def test_reset_link_sets_new_password_and_old_one_stops_working(conn):
    _account(conn)
    raw = auth.issue_token(conn, "trainer", "reset", NOW)
    new_pw = "Neues-Passwort-Essen-2027!"
    auth.redeem_token(conn, raw, new_pw, NOW)
    assert auth.login(conn, "trainer", new_pw, "1.1.1.1", NOW).session
    assert auth.login(conn, "trainer", PW, "1.1.1.1", NOW).error == auth.GENERIC_LOGIN_ERROR


def test_expired_sessions_and_old_attempts_are_cleaned_up(conn):
    _account(conn)
    auth.create_user(conn, "kapitaen", "Kapitän", NOW)
    auth.login(conn, "kapitaen", "falsch", "5.5.5.5", NOW)
    auth.login(conn, "kapitaen", "falsch", "6.6.6.6", NOW + auth.SESSION_TTL)
    assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM login_attempts WHERE ip = '5.5.5.5'").fetchone()[0] == 0


def test_unknown_login_locks_the_same_way(conn):
    for i in range(auth.LOCK_AFTER):
        assert auth.login(conn, "niemand", "falsch", f"10.1.0.{i}", NOW).error == \
            auth.GENERIC_LOGIN_ERROR
    assert auth.login(conn, "niemand", "falsch", "10.1.1.1", NOW).error == auth.TOO_MANY


def test_reset_during_password_check_cancels_the_login(conn, monkeypatch):
    _account(conn)
    real = auth._verify

    def verify_then_reset(stored, password):
        ok = real(stored, password)
        auth.issue_token(conn, "trainer", "reset", NOW)
        return ok

    monkeypatch.setattr(auth, "_verify", verify_then_reset)
    result = auth.login(conn, "trainer", PW, "1.1.1.1", NOW)
    assert result.session is None
    assert result.error == auth.GENERIC_LOGIN_ERROR


def test_lock_during_password_check_cancels_the_login(conn, monkeypatch):
    _account(conn)
    real = auth._verify

    def verify_then_lock(stored, password):
        ok = real(stored, password)
        conn.execute(
            "INSERT INTO login_failures (login, failures, locked_until) VALUES ('trainer', 0, ?)",
            (NOW + auth.LOCK_SECONDS,))
        return ok

    monkeypatch.setattr(auth, "_verify", verify_then_lock)
    result = auth.login(conn, "trainer", PW, "1.1.1.1", NOW)
    assert result.session is None
    assert result.error == auth.TOO_MANY
