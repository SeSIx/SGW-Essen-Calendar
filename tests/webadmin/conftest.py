"""Fixtures for the admin app tests."""

import pytest
from admin.app import create_app
from admin.settings import Settings

import custom_events
from admin.fake_store import FakeGitHubStore
from webadmin.testdata import (
    EVENT_MULTI,
    EVENT_TIMED,
    HOST,
    TEAM_GAMES,
    Clock,
    HttpsClient,
    make_user,
    team_ics,
)


@pytest.fixture
def fake_dir(tmp_path):
    d = tmp_path / "fake-github"
    d.mkdir()
    (d / "custom_events.json").write_text(
        custom_events.serialize([EVENT_TIMED, EVENT_MULTI]), encoding="utf-8")
    for team, games in TEAM_GAMES.items():
        (d / f"sgw_essen_{team}.ics").write_text(team_ics(games), encoding="utf-8")
    return d


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def settings(tmp_path, fake_dir):
    return Settings(env="test", secret_key="t" * 40, host=HOST, scheme="https",
                    db_path=str(tmp_path / "admin.db"), github_token=None,
                    repo="SeSIx/SGW-Essen-Calendar", branch="main", fake_github_dir=str(fake_dir))


@pytest.fixture
def store(fake_dir):
    return FakeGitHubStore(fake_dir)


@pytest.fixture
def app(settings, store, clock):
    flask_app = create_app(settings, store=store, clock=clock)
    flask_app.test_client_class = HttpsClient
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def user_client(app, client):
    client.set_cookie("__Host-sid", make_user(app), domain=HOST)
    return client
