"""The server-side command for accounts and the token."""

import io
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from admin import auth, cli, db
from admin.fake_store import FakeGitHubStore
from admin.github_store import Unauthorized
from webadmin.testdata import NOW, PW

BIN = Path(__file__).resolve().parents[2] / "admin" / "bin" / "sgw-admin"


@pytest.fixture
def env(tmp_path, fake_dir):
    return {"SECRET_KEY": "s" * 40, "SGW_ADMIN_ENV": "test", "SGW_ADMIN_FAKE_GITHUB": "1",
            "SGW_ADMIN_FAKE_DIR": str(fake_dir), "SGW_ADMIN_HOST": "localhost:8099",
            "SGW_ADMIN_SCHEME": "http", "SGW_ADMIN_DB": str(tmp_path / "cli.db")}


def run(env, *argv, now=NOW):
    out = io.StringIO()
    code = cli.main(list(argv), environ=env, out=out, now=now)
    return code, out.getvalue()


def token_of(output):
    return output.strip().splitlines()[-1].rsplit("/", 1)[1]


def test_invite_needs_a_name_for_new_accounts(env):
    code, out = run(env, "invite", "trainer")
    assert code == 1 and "--name" in out


def test_invite_prints_a_working_link(env):
    code, out = run(env, "invite", "trainer", "--name", "Trainer")
    assert code == 0
    last = out.strip().splitlines()[-1]
    assert last.startswith("http://localhost:8099/einladung/")
    conn = db.connect(env["SGW_ADMIN_DB"])
    assert auth.peek_token(conn, token_of(out), NOW)["display_name"] == "Trainer"
    conn.close()


def test_new_invite_replaces_pending_one(env):
    _, first = run(env, "invite", "trainer", "--name", "Trainer")
    _, second = run(env, "invite", "trainer")
    conn = db.connect(env["SGW_ADMIN_DB"])
    assert auth.peek_token(conn, token_of(first), NOW) is None
    assert auth.peek_token(conn, token_of(second), NOW) is not None
    conn.close()


def test_reset_after_password_and_invite_refused(env):
    _, out = run(env, "invite", "trainer", "--name", "Trainer")
    conn = db.connect(env["SGW_ADMIN_DB"])
    session = auth.redeem_token(conn, token_of(out), PW, NOW)
    code, out = run(env, "invite", "trainer")
    assert code == 1 and "reset" in out
    code, out = run(env, "reset", "trainer")
    assert code == 0 and "/einladung/" in out
    assert auth.load_session(conn, session, NOW) is None
    conn.close()


def test_users_and_logout_all(env):
    _, out = run(env, "invite", "trainer", "--name", "Trainer")
    run(env, "invite", "kapitaen", "--name", "Kapitän")
    conn = db.connect(env["SGW_ADMIN_DB"])
    auth.redeem_token(conn, token_of(out), PW, NOW)
    conn.close()
    code, listing = run(env, "users")
    assert code == 0 and "trainer" in listing and "Passwort gesetzt" in listing and "Einladung offen" in listing
    code, out = run(env, "logout-all", "trainer")
    assert code == 0 and out.startswith("1 Sitzung")
    assert run(env, "logout-all", "niemand")[0] == 1


def test_token_status(env, monkeypatch, fake_dir):
    assert run(env, "token-status") == (0, "GitHub-Token gültig, kein Ablaufdatum gemeldet.\n")
    fake = FakeGitHubStore(fake_dir)
    fake.token_expiry = datetime.fromtimestamp(NOW, UTC) + timedelta(days=30, hours=1)
    monkeypatch.setattr(cli, "build_store", lambda _settings: fake)
    code, out = run(env, "token-status")
    assert code == 0 and "noch 30 Tage" in out

    class Broken(FakeGitHubStore):
        def check_token(self):
            raise Unauthorized("401")

    monkeypatch.setattr(cli, "build_store", lambda _settings: Broken(fake_dir))
    assert run(env, "token-status")[0] == 2


def test_wrapper_script_is_executable():
    assert BIN.read_text().strip().endswith('exec python -m admin.cli "$@"')
    assert os.stat(BIN).st_mode & stat.S_IXUSR
