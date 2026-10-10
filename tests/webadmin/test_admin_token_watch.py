"""Token-Wächter: warnt per WhatsApp, bevor der GitHub-Token der Admin-App abläuft."""

import importlib.util
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[2] / "admin" / "ops" / "token_watch.py"
_spec = importlib.util.spec_from_file_location("token_watch", _MODULE_PATH)
tw = importlib.util.module_from_spec(_spec)
sys.modules["token_watch"] = tw
_spec.loader.exec_module(tw)

BERLIN = ZoneInfo("Europe/Berlin")
NOW = datetime(2027, 9, 28, 9, 0, tzinfo=BERLIN)
TOKEN = "github_pat_TESTTOKEN123"
API_KEY = "evo-test-key-123"
NUMBER = "4915112345678"  # fiktiv, nie eine echte Nummer ins Repo


@pytest.mark.parametrize(("days", "stage"), [
    (30, None), (15, None), (14, 14), (10, 14), (7, 7), (5, 7), (3, 3),
    (2, 3), (1, 1), (0, 0), (-2, 0),
])
def test_due_stage_is_smallest_reached_stage(days, stage):
    assert tw.due_stage(days) == stage


def test_parse_expiry_github_utc_format():
    parsed = tw.parse_expiry("2027-10-12 09:00:00 UTC")
    assert parsed.utcoffset().total_seconds() == 0
    assert (parsed.year, parsed.month, parsed.day, parsed.hour) == (2027, 10, 12, 9)


def test_parse_expiry_numeric_offset():
    parsed = tw.parse_expiry("2027-10-12 23:30:00 -0200")
    assert parsed.astimezone(BERLIN).date() == date(2027, 10, 13)


@pytest.mark.parametrize("raw", ["", "morgen", "2027-13-40 00:00:00 UTC", "2027-10-12"])
def test_parse_expiry_rejects_garbage(raw):
    with pytest.raises(ValueError):
        tw.parse_expiry(raw)


def test_days_left_uses_berlin_calendar_day():
    # 23:30 UTC ist in Berlin schon der nächste Tag
    expiry = tw.parse_expiry("2027-10-12 23:30:00 UTC")
    assert tw.days_left(expiry, date(2027, 9, 28)) == 15


def test_expiry_message_has_date_days_and_all_renew_steps():
    text = tw.expiry_message(date(2027, 10, 12), 14)
    assert "12.10.2027" in text
    assert "in 14 Tagen" in text
    assert "github.com/settings/personal-access-tokens" in text
    assert "nano /root/sgw-admin/admin/.env" in text
    assert "cd /root/sgw-admin && docker compose -f admin/compose.yml up -d" in text
    assert "docker exec sgw-admin sgw-admin token-status" in text


@pytest.mark.parametrize(("days", "phrase"), [(1, "(morgen)"), (0, "(heute)"), (5, "(in 5 Tagen)")])
def test_expiry_message_day_phrases(days, phrase):
    assert phrase in tw.expiry_message(date(2027, 10, 12), days)


def test_invalid_message_mentions_locked_saving_and_steps():
    text = tw.invalid_message()
    assert "ungültig" in text
    assert "docker exec sgw-admin sgw-admin token-status" in text


def test_mask_number_shows_only_last_four():
    assert tw.mask_number(NUMBER) == "…5678"


def test_load_config_minimal(tmp_path):
    conf = tmp_path / "sgw-token-watch.conf"
    conf.write_text(f"# Empfänger\nRECIPIENT={NUMBER}\n", encoding="utf-8")
    cfg = tw.load_config(conf)
    assert cfg.recipient == NUMBER
    assert cfg.github_env_file == Path("/root/sgw-admin/admin/.env")
    assert cfg.evolution_compose == Path("/root/evolution/docker-compose.yml")
    assert cfg.evolution_url == "http://127.0.0.1:8080"
    assert cfg.evolution_instance == "SGW Bot"
    assert cfg.state_file == Path("/var/lib/sgw-token-watch/state.json")


def test_load_config_overrides(tmp_path):
    conf = tmp_path / "c.conf"
    conf.write_text(
        f"RECIPIENT={NUMBER}\nSTATE_FILE={tmp_path}/s.json\nEVOLUTION_INSTANCE='Test Bot'\n",
        encoding="utf-8")
    cfg = tw.load_config(conf)
    assert cfg.state_file == tmp_path / "s.json"
    assert cfg.evolution_instance == "Test Bot"


@pytest.mark.parametrize("content", ["", "RECIPIENT=\n", "RECIPIENT=+49 170 1234\n", "RECIPIENT=abc\n"])
def test_load_config_rejects_bad_recipient(tmp_path, content):
    conf = tmp_path / "c.conf"
    conf.write_text(content, encoding="utf-8")
    with pytest.raises(tw.ConfigError) as exc:
        tw.load_config(conf)
    assert "RECIPIENT" in str(exc.value)


def test_load_config_missing_file(tmp_path):
    with pytest.raises(tw.ConfigError):
        tw.load_config(tmp_path / "fehlt.conf")


def test_read_env_value_handles_quotes_export_and_comments(tmp_path):
    env = tmp_path / ".env"
    env.write_text(f'# kommentar\nexport SECRET_KEY=x\nGITHUB_TOKEN="{TOKEN}"\n', encoding="utf-8")
    assert tw.read_env_value(env, "GITHUB_TOKEN") == TOKEN
    assert tw.read_env_value(env, "SECRET_KEY") == "x"


def test_read_env_value_missing_key_does_not_leak_other_values(tmp_path):
    env = tmp_path / ".env"
    env.write_text("SECRET_KEY=topsecret\n", encoding="utf-8")
    with pytest.raises(tw.ConfigError) as exc:
        tw.read_env_value(env, "GITHUB_TOKEN")
    assert "topsecret" not in str(exc.value)


def test_read_evolution_key_list_style(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text(
        "services:\n  api:\n    environment:\n      - SERVER_PORT=8080\n"
        f"      - AUTHENTICATION_API_KEY={API_KEY}\n"
        "      - AUTHENTICATION_EXPOSE_IN_FETCH_INSTANCES=false\n", encoding="utf-8")
    assert tw.read_evolution_key(compose) == API_KEY


def test_read_evolution_key_map_style_quoted(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text(f'    environment:\n      AUTHENTICATION_API_KEY: "{API_KEY}"\n',
                       encoding="utf-8")
    assert tw.read_evolution_key(compose) == API_KEY


def test_read_evolution_key_ignores_commented_line(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text(
        "      # - AUTHENTICATION_API_KEY=alt\n"
        f"      - AUTHENTICATION_API_KEY={API_KEY}\n", encoding="utf-8")
    assert tw.read_evolution_key(compose) == API_KEY


@pytest.mark.parametrize("content", [
    "      - SERVER_PORT=8080\n",
    "      - AUTHENTICATION_API_KEY=${EVO_KEY}\n",
    "      # - AUTHENTICATION_API_KEY=alt\n",
])
def test_read_evolution_key_missing_or_indirect_is_config_error(tmp_path, content):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text(content, encoding="utf-8")
    with pytest.raises(tw.ConfigError):
        tw.read_evolution_key(compose)
