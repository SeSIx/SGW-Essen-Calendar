"""Accounts, sessions and one-time links — the rules people actually hit."""

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
    assert calls == [auth._DUMMY_HASH]


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
    assert conn.execute("SELECT failed_logins FROM users").fetchone()[0] == 0


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
