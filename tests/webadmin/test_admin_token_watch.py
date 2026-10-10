"""Token-Wächter: warnt per WhatsApp, bevor der GitHub-Token der Admin-App abläuft."""

import importlib.util
import json
import socket
import sys
import threading
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[2] / "admin" / "ops" / "token_watch.py"
_spec = importlib.util.spec_from_file_location("token_watch", _MODULE_PATH)
tw = importlib.util.module_from_spec(_spec)
sys.modules["token_watch"] = tw
_spec.loader.exec_module(tw)

BERLIN = ZoneInfo("Europe/Berlin")
TOKEN = "github_pat_TESTTOKEN123"
API_KEY = "evo-test-key-123"
NOW = datetime(2027, 9, 28, 9, 0, tzinfo=ZoneInfo("Europe/Berlin"))
NUMBER = "4915112345678"  # fiktiv, nie eine echte Nummer ins Repo


EXPIRY_NOON = datetime(2027, 10, 12, 12, 0, tzinfo=BERLIN)
EXPIRY_MIDNIGHT = datetime(2027, 9, 10, 0, 0, tzinfo=BERLIN)


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
    text = tw.expiry_message(EXPIRY_NOON, 14)
    assert "am 12.10.2027 um 12:00 Uhr ab" in text
    assert "in 14 Tagen" in text
    assert "github.com/settings/personal-access-tokens" in text
    assert "nano /root/sgw-admin/admin/.env" in text
    assert "cd /root/sgw-admin && docker compose -f admin/compose.yml up -d" in text
    assert "docker exec sgw-admin sgw-admin token-status" in text


@pytest.mark.parametrize(("days", "phrase"), [(1, "(morgen)"), (0, "(heute)"), (5, "(in 5 Tagen)")])
def test_expiry_message_day_phrases(days, phrase):
    assert phrase in tw.expiry_message(EXPIRY_NOON, days)


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


def test_read_evolution_key_trailing_text_is_config_error(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text("      - AUTHENTICATION_API_KEY=a b\n", encoding="utf-8")
    with pytest.raises(tw.ConfigError):
        tw.read_evolution_key(compose)


def test_read_evolution_key_trailing_comment_is_allowed(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_text("      - AUTHENTICATION_API_KEY=abc # kommentar\n", encoding="utf-8")
    assert tw.read_evolution_key(compose) == "abc"


def test_unreadable_config_is_config_error_without_contents(tmp_path):
    conf = tmp_path / "c.conf"
    conf.write_bytes(b"RECIPIENT=\xff\xfe-geheim\n")
    with pytest.raises(tw.ConfigError) as exc:
        tw.load_config(conf)
    assert "geheim" not in str(exc.value)


def test_read_env_invalid_utf8_is_config_error(tmp_path):
    env = tmp_path / ".env"
    env.write_bytes(b"GITHUB_TOKEN=\xff\n")
    with pytest.raises(tw.ConfigError):
        tw.read_env_value(env, "GITHUB_TOKEN")


def test_read_evolution_key_invalid_utf8_is_config_error(tmp_path):
    compose = tmp_path / "docker-compose.yml"
    compose.write_bytes(b"      - AUTHENTICATION_API_KEY=\xff\n")
    with pytest.raises(tw.ConfigError):
        tw.read_evolution_key(compose)


def test_load_config_tolerates_bom(tmp_path):
    conf = tmp_path / "c.conf"
    conf.write_bytes("﻿RECIPIENT=".encode() + NUMBER.encode() + b"\n")
    assert tw.load_config(conf).recipient == NUMBER


def test_read_env_value_tolerates_bom(tmp_path):
    env = tmp_path / ".env"
    env.write_bytes("﻿GITHUB_TOKEN=".encode() + TOKEN.encode() + b"\n")
    assert tw.read_env_value(env, "GITHUB_TOKEN") == TOKEN


def test_expiry_message_after_expiry_says_expired():
    text = tw.expiry_message(EXPIRY_NOON, -2)
    assert "12.10.2027" in text
    assert "abgelaufen (vor 2 Tagen)" in text
    assert "github.com/settings/personal-access-tokens" in text
    assert "docker exec sgw-admin sgw-admin token-status" in text


@pytest.mark.parametrize(("value", "masked"), [("", "…"), ("1234", "…"), ("12345", "…2345")])
def test_mask_number_never_shows_whole_short_number(value, masked):
    assert tw.mask_number(value) == masked


def test_probe_message_is_a_test_text_without_number():
    text = tw.probe_message()
    assert "Test" in text
    assert NUMBER not in text


def test_parse_expiry_colon_offset():
    parsed = tw.parse_expiry("2027-10-12 09:00:00 +02:00")
    assert parsed.astimezone(BERLIN).date() == date(2027, 10, 12)
    assert parsed.utcoffset().total_seconds() == 2 * 3600


def test_parse_expiry_surrounding_whitespace():
    parsed = tw.parse_expiry("  2027-10-12 09:00:00 UTC \n")
    assert (parsed.year, parsed.month, parsed.day, parsed.hour) == (2027, 10, 12, 9)


def test_days_left_near_midnight_just_before_berlin_day_change():
    # 21:59:59 UTC = 23:59:59 CEST am 12.10. -> noch der 12.10.
    expiry = tw.parse_expiry("2027-10-12 21:59:59 UTC")
    assert tw.days_left(expiry, date(2027, 9, 28)) == 14


def test_days_left_near_midnight_just_after_berlin_day_change():
    # 22:00:00 UTC = 00:00 CEST am 13.10. -> letzter gültiger Moment liegt am 12.10.
    expiry = tw.parse_expiry("2027-10-12 22:00:00 UTC")
    assert tw.days_left(expiry, date(2027, 9, 28)) == 14
    expiry = tw.parse_expiry("2027-10-12 22:00:01 UTC")
    assert tw.days_left(expiry, date(2027, 9, 28)) == 15


def test_days_left_across_dst_change_day():
    # Die Sommerzeit endet am 31.10.2027 (letzter Sonntag im Oktober)
    expiry = tw.parse_expiry("2027-10-31 00:30:00 UTC")
    assert tw.days_left(expiry, date(2027, 10, 31)) == 0
    assert tw.days_left(expiry, date(2027, 10, 28)) == 3


class FakeHttp:
    """Answers GitHub and Evolution calls from queues; records every call."""

    def __init__(self, github=None, evolution=None):
        self.github = list(github or [])
        self.evolution = list(evolution or [])
        self.calls = []

    def __call__(self, method, url, headers, body):
        self.calls.append((method, url, headers, body))
        queue = self.github if "api.github.com" in url else self.evolution
        item = queue.pop(0) if queue else tw.HttpResponse(200, {}, b"{}")
        if isinstance(item, Exception):
            raise item
        return item

    def posts(self):
        return [c for c in self.calls if c[0] == "POST"]

    def sent_texts(self):
        return [json.loads(body)["text"] for _, _, _, body in self.posts()]


def github_expiring(day="2027-10-12"):
    return tw.HttpResponse(
        200, {"github-authentication-token-expiration": f"{day} 09:00:00 UTC"}, b"{}")


def test_check_token_reads_expiry_header_and_sends_bearer():
    http = FakeHttp(github=[github_expiring()])
    status = tw.check_token(http, TOKEN)
    assert status.kind == "expires"
    assert status.expiry.astimezone(BERLIN).date() == date(2027, 10, 12)
    method, url, headers, body = http.calls[0]
    assert (method, url, body) == ("GET", "https://api.github.com/rate_limit", None)
    assert headers["Authorization"] == f"Bearer {TOKEN}"
    assert headers["User-Agent"] == "sgw-token-watch"


def test_check_token_without_header_is_no_expiry():
    http = FakeHttp(github=[tw.HttpResponse(200, {}, b"{}")])
    assert tw.check_token(http, TOKEN).kind == "no_expiry"


def test_check_token_401_is_invalid():
    http = FakeHttp(github=[tw.HttpResponse(401, {}, b"{}")])
    assert tw.check_token(http, TOKEN).kind == "invalid"


@pytest.mark.parametrize("answer", [tw.HttpResponse(502, {}, b""), OSError("timed out")])
def test_check_token_outage_is_unavailable(answer):
    with pytest.raises(tw.GitHubUnavailable):
        tw.check_token(FakeHttp(github=[answer]), TOKEN)


def test_check_token_unparseable_header_is_unavailable():
    answer = tw.HttpResponse(200, {"github-authentication-token-expiration": "bald"}, b"{}")
    with pytest.raises(tw.GitHubUnavailable):
        tw.check_token(FakeHttp(github=[answer]), TOKEN)


def _send(http, sleeps, logs):
    return tw.send_whatsapp(
        http, sleeps.append, logs.append, base_url="http://127.0.0.1:8080/",
        instance="SGW Bot", api_key=API_KEY, number=NUMBER, text="Hallo")


def test_send_whatsapp_posts_expected_request():
    http, sleeps, logs = FakeHttp(evolution=[tw.HttpResponse(201, {}, b"{}")]), [], []
    assert _send(http, sleeps, logs) is True
    method, url, headers, body = http.calls[0]
    assert method == "POST"
    assert url == "http://127.0.0.1:8080/message/sendText/SGW%20Bot"
    assert headers["apikey"] == API_KEY
    assert json.loads(body) == {"number": NUMBER, "text": "Hallo", "linkPreview": False}
    assert sleeps == []


def test_send_whatsapp_retries_transient_failures_with_backoff():
    http = FakeHttp(evolution=[tw.HttpResponse(503, {}, b""), OSError("reset"),
                               tw.HttpResponse(200, {}, b"{}")])
    sleeps, logs = [], []
    assert _send(http, sleeps, logs) is True
    assert sleeps == [2, 4]
    assert len(http.posts()) == 3


def test_send_whatsapp_gives_up_after_three_attempts():
    http = FakeHttp(evolution=[tw.HttpResponse(429, {}, b"")] * 3)
    sleeps, logs = [], []
    assert _send(http, sleeps, logs) is False
    assert sleeps == [2, 4]
    assert len(http.posts()) == 3


@pytest.mark.parametrize("status", [400, 401, 404])
def test_send_whatsapp_permanent_4xx_does_not_retry(status):
    http = FakeHttp(evolution=[tw.HttpResponse(status, {}, f"bad {NUMBER}".encode())])
    sleeps, logs = [], []
    assert _send(http, sleeps, logs) is False
    assert sleeps == []
    assert len(http.posts()) == 1
    joined = "\n".join(logs)
    assert API_KEY not in joined and NUMBER not in joined


def _responder(hits, status, extra_headers=(), body=b"{}"):
    """Local HTTP handler: records every request it gets, answers with a fixed status."""

    class Handler(BaseHTTPRequestHandler):
        def _answer(self):
            length = int(self.headers.get("Content-Length") or 0)
            payload = self.rfile.read(length) if length else b""
            hits.append((self.command, self.path, dict(self.headers), payload))
            self.send_response(status)
            for name, value in extra_headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = _answer
        do_POST = _answer

        def log_message(self, *args):
            pass

    return Handler


@pytest.fixture
def local_server():
    """Starts threaded HTTP servers on 127.0.0.1 with ephemeral ports; stops them after the test."""
    servers = []

    def start(handler_cls):
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture
def raw_server():
    """Accepts one connection on 127.0.0.1, sends fixed bytes, closes."""
    listener = socket.create_server(("127.0.0.1", 0))

    def serve(reply):
        def run():
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            with conn:
                conn.recv(65536)
                conn.sendall(reply)

        threading.Thread(target=run, daemon=True).start()
        return listener.getsockname()[1]

    yield serve
    listener.close()


def test_urllib_http_does_not_follow_redirects_and_keeps_secrets_home(local_server):
    sink_hits = []
    sink = local_server(_responder(sink_hits, 200))
    origin_hits = []
    origin = local_server(_responder(origin_hits, 302, [("Location", sink + "/catch")]))
    response = tw.urllib_http("POST", origin + "/x", {"apikey": API_KEY}, b"{}")
    assert response.status == 302
    assert response.headers["location"] == sink + "/catch"
    assert len(origin_hits) == 1
    received = {name.lower(): value for name, value in origin_hits[0][2].items()}
    assert received["apikey"] == API_KEY
    assert sink_hits == []


def test_urllib_http_plain_get_and_503_round_trip(local_server):
    ok = local_server(_responder([], 200, [("X-Test", "yes")], b'{"a": 1}'))
    response = tw.urllib_http("GET", ok + "/", {"Accept": "application/json"}, None)
    assert response.status == 200
    assert response.body == b'{"a": 1}'
    assert response.headers["x-test"] == "yes"
    down = local_server(_responder([], 503, body=b"busy"))
    response = tw.urllib_http("GET", down + "/", {}, None)
    assert (response.status, response.body) == (503, b"busy")


def test_urllib_http_garbage_status_line_is_connection_error(raw_server):
    port = raw_server(b"garbage\r\n\r\n")
    with pytest.raises(ConnectionError, match="BadStatusLine"):
        tw.urllib_http("GET", f"http://127.0.0.1:{port}/", {}, None)


@pytest.mark.parametrize("status", [301, 302, 303, 307])
def test_check_token_redirect_is_unavailable(status):
    with pytest.raises(tw.GitHubUnavailable):
        tw.check_token(FakeHttp(github=[tw.HttpResponse(status, {}, b"")]), TOKEN)


def test_check_token_403_is_unavailable():
    with pytest.raises(tw.GitHubUnavailable):
        tw.check_token(FakeHttp(github=[tw.HttpResponse(403, {}, b"{}")]), TOKEN)


@pytest.mark.parametrize("answer", [
    tw.HttpResponse(403, {}, b""),
    tw.HttpResponse(502, {}, b""),
    tw.HttpResponse(200, {"github-authentication-token-expiration": "bald " + TOKEN}, b""),
    OSError("timed out"),
])
def test_github_unavailable_never_carries_token(answer):
    with pytest.raises(tw.GitHubUnavailable) as caught:
        tw.check_token(FakeHttp(github=[answer]), TOKEN)
    assert TOKEN not in str(caught.value)
    assert TOKEN not in repr(caught.value.args)


def test_check_token_sends_github_api_headers():
    http = FakeHttp(github=[github_expiring()])
    tw.check_token(http, TOKEN)
    headers = http.calls[0][2]
    assert headers["Accept"] == "application/vnd.github+json"
    assert headers["X-GitHub-Api-Version"] == "2022-11-28"


@pytest.mark.parametrize("status", [301, 302, 303, 307])
def test_send_whatsapp_redirect_is_permanent_failure(status):
    http = FakeHttp(evolution=[tw.HttpResponse(status, {"location": "http://evil/"}, b"")])
    sleeps, logs = [], []
    assert _send(http, sleeps, logs) is False
    assert sleeps == []
    assert len(http.posts()) == 1


def test_send_whatsapp_logs_never_contain_key_or_number_on_retry_paths():
    secret_error = OSError(f"reset {API_KEY} {NUMBER}")
    http = FakeHttp(evolution=[
        secret_error,
        tw.HttpResponse(503, {}, f"{API_KEY} {NUMBER}".encode()),
        tw.HttpResponse(502, {}, f"{API_KEY} {NUMBER}".encode()),
    ])
    sleeps, logs = [], []
    assert _send(http, sleeps, logs) is False
    assert len(logs) == 3
    joined = "\n".join(logs)
    assert API_KEY not in joined and NUMBER not in joined


@pytest.fixture
def cfg(tmp_path):
    env = tmp_path / ".env"
    env.write_text(f"SECRET_KEY=x\nGITHUB_TOKEN={TOKEN}\n", encoding="utf-8")
    compose = tmp_path / "docker-compose.yml"
    compose.write_text("services:\n  api:\n    environment:\n"
                       f"      - AUTHENTICATION_API_KEY={API_KEY}\n", encoding="utf-8")
    return tw.Config(recipient=NUMBER, github_env_file=env, evolution_compose=compose,
                     state_file=tmp_path / "state" / "state.json")


def run(cfg, http, now=NOW, mode="send"):
    logs, sleeps = [], []
    code = tw.run(cfg, http=http, now=now, sleep=sleeps.append, log=logs.append, mode=mode)
    return code, logs


def state_keys(cfg):
    return set(json.loads(cfg.state_file.read_text(encoding="utf-8"))["sent"])


def test_sends_stage_14_once_per_expiry(cfg):
    code, _ = run(cfg, FakeHttp(github=[github_expiring()]))  # NOW + 14 Tage
    assert code == tw.EXIT_OK
    assert state_keys(cfg) == {"2027-10-12:14"}
    http = FakeHttp(github=[github_expiring()])
    code, _ = run(cfg, http)
    assert code == tw.EXIT_OK
    assert http.posts() == []


def test_message_text_matches_stage(cfg):
    http = FakeHttp(github=[github_expiring()])
    run(cfg, http)
    [text] = http.sent_texts()
    assert "12.10.2027" in text and "in 14 Tagen" in text


def test_nothing_due_between_stages(cfg):
    run(cfg, FakeHttp(github=[github_expiring()]))
    http = FakeHttp(github=[github_expiring()])
    code, logs = run(cfg, http, now=NOW + timedelta(days=1))  # 13 Tage
    assert code == tw.EXIT_OK and http.posts() == []
    assert any("Keine Warnung fällig" in line for line in logs)


def test_catch_up_sends_one_message_and_marks_larger_stages(cfg):
    http = FakeHttp(github=[github_expiring()])
    run(cfg, http, now=NOW + timedelta(days=9))  # 5 Tage übrig → Stufe 7
    assert len(http.posts()) == 1
    assert "in 5 Tagen" in http.sent_texts()[0]
    assert state_keys(cfg) == {"2027-10-12:14", "2027-10-12:7"}


def test_downtime_jump_sends_only_smallest_reached_stage(cfg):
    run(cfg, FakeHttp(github=[github_expiring()]))  # Stufe 14 gesendet
    http = FakeHttp(github=[github_expiring()])
    run(cfg, http, now=NOW + timedelta(days=12))  # 2 Tage übrig → Stufe 3, 7 übersprungen
    assert len(http.posts()) == 1 and "in 2 Tagen" in http.sent_texts()[0]
    assert state_keys(cfg) == {f"2027-10-12:{s}" for s in (14, 7, 3)}
    http = FakeHttp(github=[github_expiring()])
    run(cfg, http, now=NOW + timedelta(days=12, hours=3))  # gleicher Tag: nichts
    assert http.posts() == []
    for days_later, phrase in ((13, "(morgen)"), (14, "(heute)")):
        http = FakeHttp(github=[github_expiring()])
        run(cfg, http, now=NOW + timedelta(days=days_later))
        assert len(http.posts()) == 1 and phrase in http.sent_texts()[0]


def test_renewed_token_starts_fresh_stage_cycle(cfg):
    run(cfg, FakeHttp(github=[github_expiring()]), now=NOW + timedelta(days=14))
    assert "2027-10-12:0" in state_keys(cfg)
    http = FakeHttp(github=[github_expiring("2028-10-12")])
    run(cfg, http, now=NOW + timedelta(days=15))
    assert http.posts() == []
    http = FakeHttp(github=[github_expiring("2028-10-12")])
    run(cfg, http, now=datetime(2028, 9, 28, 9, 0, tzinfo=BERLIN))
    assert len(http.posts()) == 1
    assert state_keys(cfg) == {"2028-10-12:14"}, "old expiry keys are pruned"


def test_no_expiry_header_sends_nothing(cfg):
    http = FakeHttp(github=[tw.HttpResponse(200, {}, b"{}")])
    code, logs = run(cfg, http)
    assert code == tw.EXIT_OK and http.posts() == []
    assert any("kein Ablaufdatum" in line for line in logs)


def test_invalid_token_warns_at_most_once_per_day(cfg):
    http = FakeHttp(github=[tw.HttpResponse(401, {}, b"")])
    assert run(cfg, http)[0] == tw.EXIT_OK
    assert "ungültig" in http.sent_texts()[0]
    http = FakeHttp(github=[tw.HttpResponse(401, {}, b"")])
    run(cfg, http, now=NOW + timedelta(hours=5))
    assert http.posts() == []
    http = FakeHttp(github=[tw.HttpResponse(401, {}, b"")])
    run(cfg, http, now=NOW + timedelta(days=1))
    assert len(http.posts()) == 1
    assert state_keys(cfg) == {"invalid:2027-09-29"}


def test_github_outage_exits_1_without_message_or_state(cfg):
    http = FakeHttp(github=[OSError("down")])
    code, _ = run(cfg, http)
    assert code == tw.EXIT_FAILED and http.posts() == []
    assert not cfg.state_file.exists()


def test_failed_send_keeps_stage_pending(cfg):
    http = FakeHttp(github=[github_expiring()], evolution=[tw.HttpResponse(503, {}, b"")] * 3)
    code, _ = run(cfg, http)
    assert code == tw.EXIT_FAILED
    assert not cfg.state_file.exists()
    http = FakeHttp(github=[github_expiring()])
    assert run(cfg, http)[0] == tw.EXIT_OK
    assert len(http.posts()) == 1


def test_corrupt_state_file_is_survived(cfg):
    cfg.state_file.parent.mkdir(parents=True)
    cfg.state_file.write_text("{kaputt", encoding="utf-8")
    http = FakeHttp(github=[github_expiring()])
    code, logs = run(cfg, http)
    assert code == tw.EXIT_OK and len(http.posts()) == 1
    assert any("Statusdatei" in line for line in logs)
    assert state_keys(cfg) == {"2027-10-12:14"}


def test_state_file_is_private(cfg):
    run(cfg, FakeHttp(github=[github_expiring()]))
    assert cfg.state_file.stat().st_mode & 0o777 == 0o600


def test_dry_run_sends_and_writes_nothing(cfg):
    http = FakeHttp(github=[github_expiring()])
    code, logs = run(cfg, http, mode="dry-run")
    assert code == tw.EXIT_OK and http.posts() == []
    assert not cfg.state_file.exists()
    assert any("12.10.2027" in line for line in logs)


def test_status_reports_without_side_effects(cfg):
    http = FakeHttp(github=[github_expiring()])
    code, logs = run(cfg, http, now=NOW - timedelta(days=10), mode="status")
    assert code == tw.EXIT_OK and http.posts() == []
    assert not cfg.state_file.exists()
    joined = "\n".join(logs)
    assert "12.10.2027" in joined and "24 Tage" in joined
    assert "28.09.2027" in joined, "date of the next stage (14 days before)"


def test_test_message_skips_github_and_state(cfg):
    http = FakeHttp()
    code, _ = run(cfg, http, mode="test-message")
    assert code == tw.EXIT_OK
    assert [c[0] for c in http.calls] == ["POST"]
    assert "Test" in http.sent_texts()[0]
    assert not cfg.state_file.exists()


def test_config_error_makes_no_http_calls(cfg):
    cfg.evolution_compose.write_text("      - SERVER_PORT=8080\n", encoding="utf-8")
    http = FakeHttp(github=[github_expiring()])
    code, _ = run(cfg, http)
    assert code == tw.EXIT_CONFIG and http.calls == []


def test_missing_github_token_is_config_error(cfg):
    cfg.github_env_file.write_text("SECRET_KEY=x\n", encoding="utf-8")
    http = FakeHttp()
    assert run(cfg, http)[0] == tw.EXIT_CONFIG and http.calls == []


def test_secrets_never_logged(cfg):
    all_logs = []
    for http, mode in [
        (FakeHttp(github=[github_expiring()]), "send"),
        (FakeHttp(github=[github_expiring()]), "status"),
        (FakeHttp(github=[tw.HttpResponse(401, {}, b"")]), "send"),
        (FakeHttp(evolution=[tw.HttpResponse(400, {}, NUMBER.encode())]), "test-message"),
    ]:
        all_logs += run(cfg, http, now=NOW + timedelta(days=2), mode=mode)[1]
    joined = "\n".join(all_logs)
    for secret in (TOKEN, API_KEY, NUMBER):
        assert secret not in joined
    assert "…5678" in joined


def test_main_rejects_combined_flags(capsys):
    with pytest.raises(SystemExit) as exc:
        tw.main(["--status", "--dry-run"])
    assert exc.value.code == 2
    capsys.readouterr()


def test_main_bad_config_exits_2(tmp_path):
    conf = tmp_path / "c.conf"
    conf.write_text("RECIPIENT=\n", encoding="utf-8")
    assert tw.main(["--config", str(conf)]) == tw.EXIT_CONFIG


def test_save_state_failure_after_send_exits_1_and_logs_masked(cfg):
    cfg.state_file.parent.mkdir(parents=True)
    cfg.state_file.with_name("state.json.tmp").mkdir()  # saving fails, reading does not
    http = FakeHttp(github=[github_expiring()])
    code, logs = run(cfg, http)
    assert code == tw.EXIT_FAILED and len(http.posts()) == 1
    line = next(entry for entry in logs if "Statusdatei nicht gespeichert" in entry)
    assert "…5678" in line and "erneut kommen" in line
    for secret in (TOKEN, API_KEY, NUMBER):
        assert secret not in "\n".join(logs)


def test_save_state_removes_tmp_on_failure(tmp_path, monkeypatch):
    path = tmp_path / "state.json"

    def boom(*_args):
        raise OSError("disk")

    monkeypatch.setattr(tw.os, "replace", boom)
    with pytest.raises(OSError):
        tw.save_state(path, {"sent": []})
    assert list(tmp_path.iterdir()) == []


def test_run_rejects_unknown_mode(cfg):
    http = FakeHttp(github=[github_expiring()])
    with pytest.raises(ValueError):
        run(cfg, http, mode="sned")
    assert http.calls == []


@pytest.mark.parametrize(("argv", "mode"), [
    ([], "send"), (["--status"], "status"), (["--dry-run"], "dry-run"),
    (["--test-message"], "test-message"),
])
def test_main_maps_flags_to_modes(monkeypatch, argv, mode):
    seen = []
    monkeypatch.setattr(tw, "load_config", lambda _path: "cfg")
    monkeypatch.setattr(tw, "run", lambda cfg, **kw: seen.append((cfg, kw["mode"])) or 0)
    assert tw.main(argv) == 0
    assert seen == [("cfg", mode)]


def test_status_shows_due_stage_not_yet_sent(cfg):
    http = FakeHttp(github=[github_expiring()])
    _, logs = run(cfg, http, mode="status")
    assert any("Fällig: Stufe 14 (noch nicht gesendet)" in line for line in logs)


def test_status_when_due_today(cfg):
    code, logs = run(cfg, FakeHttp(github=[github_expiring()]),
                     now=NOW + timedelta(days=14), mode="status")
    assert code == tw.EXIT_OK
    assert any("Fällig: Stufe 0" in line for line in logs)


def test_status_when_already_expired(cfg):
    code, logs = run(cfg, FakeHttp(github=[github_expiring()]),
                     now=NOW + timedelta(days=16), mode="status")
    assert code == tw.EXIT_OK
    assert any("Fällig: Stufe 0" in line for line in logs)


def test_status_sorts_sent_stages_numerically(cfg):
    cfg.state_file.parent.mkdir(parents=True)
    keys = [f"2027-10-12:{s}" for s in (14, 7, 3, 1, 0)]
    cfg.state_file.write_text(json.dumps({"sent": keys}), encoding="utf-8")
    _, logs = run(cfg, FakeHttp(github=[github_expiring()]),
                  now=NOW + timedelta(days=14), mode="status")
    assert any("2027-10-12:0, 2027-10-12:1, 2027-10-12:3, 2027-10-12:7, 2027-10-12:14" in line
               for line in logs)


def test_invalid_already_reported_today_is_logged(cfg):
    run(cfg, FakeHttp(github=[tw.HttpResponse(401, {}, b"")]))
    _, logs = run(cfg, FakeHttp(github=[tw.HttpResponse(401, {}, b"")]))
    assert any("heute bereits gemeldet" in line for line in logs)


@pytest.mark.parametrize("mode", ["dry-run", "status"])
def test_invalid_token_in_non_send_modes_sends_and_writes_nothing(cfg, mode):
    http = FakeHttp(github=[tw.HttpResponse(401, {}, b"")])
    code, _ = run(cfg, http, mode=mode)
    assert code == tw.EXIT_OK and http.posts() == []
    assert not cfg.state_file.exists()


@pytest.mark.parametrize("content", ['{"sent": "x"}', "[1]", '{"sent": [1]}'])
def test_load_state_wrong_shape_is_survived(tmp_path, content):
    path = tmp_path / "s.json"
    path.write_text(content, encoding="utf-8")
    logs = []
    assert tw.load_state(path, logs.append) == {"sent": []}
    assert any("Statusdatei" in line for line in logs)


# --- final review fixes ---


def test_conf_inline_comment_is_stripped(tmp_path):
    conf = tmp_path / "c.conf"
    conf.write_text(f"RECIPIENT={NUMBER}   # Kommentar\n", encoding="utf-8")
    assert tw.load_config(conf).recipient == NUMBER


def test_env_inline_comment_is_stripped_but_hash_without_space_and_quoted_hash_stay(tmp_path):
    env = tmp_path / ".env"
    env.write_text("GITHUB_TOKEN=abc # x\nA=\"p#q  # r\"  # c\nB='s # t'\nC=ab#cd\n",
                   encoding="utf-8")
    assert tw.read_env_value(env, "GITHUB_TOKEN") == "abc"
    values = tw._read_key_values(env)
    assert values["A"] == "p#q  # r"
    assert values["B"] == "s # t"
    assert values["C"] == "ab#cd"


def test_midnight_expiry_counts_from_last_valid_moment():
    assert tw.days_left(EXPIRY_MIDNIGHT, date(2027, 9, 8)) == 1
    assert tw.days_left(EXPIRY_MIDNIGHT, date(2027, 9, 9)) == 0


def test_midnight_expiry_message_states_exact_time():
    text = tw.expiry_message(EXPIRY_MIDNIGHT, 1)
    assert "läuft am 10.09.2027 um 00:00 Uhr ab (morgen)" in text
    text = tw.expiry_message(EXPIRY_MIDNIGHT, 0)
    assert "läuft heute Nacht um 00:00 Uhr ab" in text


def midnight_github():
    return tw.HttpResponse(
        200, {"github-authentication-token-expiration": "2027-09-09 22:00:00 UTC"}, b"{}")


def test_midnight_expiry_runs_stage_1_on_8th_and_stage_0_on_9th(cfg):
    http = FakeHttp(github=[midnight_github()])
    run(cfg, http, now=datetime(2027, 9, 8, 9, 0, tzinfo=BERLIN))
    assert "um 00:00 Uhr ab (morgen)" in http.sent_texts()[0]
    http = FakeHttp(github=[midnight_github()])
    run(cfg, http, now=datetime(2027, 9, 9, 9, 0, tzinfo=BERLIN))
    assert "heute Nacht um 00:00 Uhr" in http.sent_texts()[0]
    assert "2027-09-10:0" in state_keys(cfg)


def test_status_and_log_show_the_time(cfg):
    _, logs = run(cfg, FakeHttp(github=[midnight_github()]),
                  now=datetime(2027, 9, 8, 9, 0, tzinfo=BERLIN), mode="status")
    assert any("10.09.2027 um 00:00 Uhr" in line and "noch 1 Tag)" in line for line in logs)


def test_german_singular_days():
    assert "vor 1 Tag)" in tw.expiry_message(EXPIRY_NOON, -1)
    assert "vor 2 Tagen)" in tw.expiry_message(EXPIRY_NOON, -2)


def test_http_proxy_env_is_ignored(local_server, monkeypatch):
    proxy_hits, target_hits = [], []
    proxy = local_server(_responder(proxy_hits, 200))
    target = local_server(_responder(target_hits, 200))
    monkeypatch.setenv("http_proxy", proxy)
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    assert tw.urllib_http("GET", target + "/x", {}, None).status == 200
    assert len(target_hits) == 1 and proxy_hits == []


def test_unreadable_state_file_exits_1_without_sending(cfg):
    cfg.state_file.mkdir(parents=True)  # reading a directory raises OSError, not FileNotFound
    http = FakeHttp(github=[github_expiring()])
    code, logs = run(cfg, http)
    assert code == tw.EXIT_FAILED and http.posts() == []
    assert any(str(cfg.state_file) in line and "IsADirectoryError" in line for line in logs)
