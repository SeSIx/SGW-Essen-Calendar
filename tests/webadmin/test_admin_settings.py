"""Unsafe configurations must refuse to start, not run half-protected."""

import pytest

from admin.settings import PRODUCTION_HOST, from_env

KEY = "abcdefghijklmnopqrstuvwxyz0123456789ABCD"
GOOD = {"SECRET_KEY": KEY, "GITHUB_TOKEN": "tok"}
FAKE = {"SGW_ADMIN_FAKE_GITHUB": "1", "SGW_ADMIN_FAKE_DIR": "/fake"}


def test_production_defaults():
    s = from_env(GOOD)
    assert (s.env, s.host, s.scheme, s.db_path, s.repo, s.branch, s.fake_github_dir) == (
        "production", PRODUCTION_HOST, "https", "/data/sgw-admin.db", "SeSIx/SGW-Essen-Calendar", "main", None)
    assert s.origin == "https://sgw-admin.srv1136792.hstgr.cloud"


def test_db_path_comes_from_env():
    assert from_env({**GOOD, "SGW_ADMIN_DB": "/data/x.db"}).db_path == "/data/x.db"


@pytest.mark.parametrize("environ, message", [
    ({"GITHUB_TOKEN": "tok"}, "SECRET_KEY"),
    ({"SECRET_KEY": "kurz", "GITHUB_TOKEN": "tok"}, "SECRET_KEY"),
    ({"SECRET_KEY": KEY}, "GITHUB_TOKEN"),
    ({**GOOD, "SGW_ADMIN_ENV": "prod"}, "SGW_ADMIN_ENV"),
    ({**GOOD, "SGW_ADMIN_ENV": "Production"}, "SGW_ADMIN_ENV"),
    ({"SECRET_KEY": "a" * 32, "GITHUB_TOKEN": "tok"}, "SECRET_KEY"),
    ({**GOOD, **FAKE}, "production"),
    ({**GOOD, "SGW_ADMIN_HOST": "evil.example"}, "production"),
    ({**GOOD, "SGW_ADMIN_SCHEME": "http"}, "production"),
    ({**GOOD, **FAKE, "SGW_ADMIN_ENV": "e2e"}, "localhost"),
    ({**GOOD, "SGW_ADMIN_ENV": "e2e", "SGW_ADMIN_FAKE_GITHUB": "1",
      "SGW_ADMIN_HOST": "localhost:8099"}, "SGW_ADMIN_FAKE_DIR"),
])
def test_unsafe_configurations_refuse_to_start(environ, message):
    with pytest.raises(RuntimeError, match=message):
        from_env(environ)


def test_e2e_fake_mode():
    s = from_env({"SECRET_KEY": KEY, "SGW_ADMIN_ENV": "e2e", **FAKE,
                  "SGW_ADMIN_HOST": "localhost:8099", "SGW_ADMIN_SCHEME": "http"})
    assert (s.fake_github_dir, s.origin, s.github_token) == ("/fake", "http://localhost:8099", None)


def test_hand_built_production_settings_refuse_fake_store(settings):
    from dataclasses import replace

    from admin.app import create_app

    with pytest.raises(RuntimeError, match="production"):
        create_app(replace(settings, env="production"))


JULIUS = "julius=Julius Gerecke <76214201+SeSIx@users.noreply.github.com>"


def test_git_authors_default_to_empty():
    assert from_env(GOOD).git_authors == {}
    assert from_env({**GOOD, "SGW_ADMIN_GIT_AUTHORS": ""}).git_authors == {}
    assert from_env({**GOOD, "SGW_ADMIN_GIT_AUTHORS": "  "}).git_authors == {}


def test_git_authors_are_parsed():
    s = from_env({**GOOD, "SGW_ADMIN_GIT_AUTHORS": f"{JULIUS}; max-k=Max K <max@example.org>;"})
    assert s.git_authors == {
        "julius": ("Julius Gerecke", "76214201+SeSIx@users.noreply.github.com"),
        "max-k": ("Max K", "max@example.org"),
    }


@pytest.mark.parametrize("value", [
    "julius",
    "Julius=Julius <a@b.de>",
    "j=Julius <a@b.de>",
    "julius=<a@b.de>",
    "julius= <a@b.de>",
    "julius=Julius a@b.de",
    "julius=Julius <a@b.de",
    "julius=Julius <a@@b.de>",
    "julius=Julius <ab.de>",
    "julius=Julius <a<b@c.de>",
    "julius=Julius <a@b.de> x",
    "julius=Ju\tlius <a@b.de>",
    "julius=Ju\nlius <a@b.de>",
    "julius=Julius <a@b.de>;julius=Julius <c@d.de>",
])
def test_malformed_git_authors_refuse_to_start(value):
    with pytest.raises(RuntimeError, match="SGW_ADMIN_GIT_AUTHORS") as exc:
        from_env({**GOOD, "SGW_ADMIN_GIT_AUTHORS": value})
    assert value not in str(exc.value)
    assert "a@b.de" not in str(exc.value)
