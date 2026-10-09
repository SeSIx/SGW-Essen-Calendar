"""The GitHub client at HTTP level: what is sent, and how answers are mapped."""

import base64
import json
from datetime import UTC, datetime

import pytest
import requests

import custom_events
from admin import github_store as gs
from webadmin.testdata import EVENT_MULTI, EVENT_TIMED


class StubHttp:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def resp(status=200, body=None, text="", headers=None):
    r = requests.Response()
    r.status_code = status
    r.headers.update(headers or {})
    r._content = (json.dumps(body) if body is not None else text).encode("utf-8")
    r.encoding = "utf-8"
    return r


def contents_body(text, sha="abc123"):
    encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
    # GitHub wraps base64 at 60 characters; decoding must cope with the newlines.
    wrapped = "\n".join(encoded[i:i + 60] for i in range(0, len(encoded), 60))
    return {"content": wrapped, "sha": sha, "encoding": "base64"}


def store(*responses):
    http = StubHttp(*responses)
    return gs.GitHubStore("tok123", "SeSIx/SGW-Essen-Calendar", "main", http=http), http


def test_load_decodes_file_and_returns_sha():
    text = custom_events.serialize([EVENT_TIMED, EVENT_MULTI])
    s, http = store(resp(body=contents_body(text)))
    snap = s.load()
    assert snap.sha == "abc123"
    assert [e["title"] for e in snap.result.valid] == ["Kampfrichter-Lehrgang", "Trainingslager Duisburg"]
    method, url, kw = http.calls[0]
    assert (method, url) == ("GET", "https://api.github.com/repos/SeSIx/SGW-Essen-Calendar/contents/custom_events.json")
    assert kw["params"] == {"ref": "main"}
    assert kw["headers"]["Authorization"] == "Bearer tok123"
    assert kw["timeout"] == 10


def test_load_of_non_list_file_is_corrupt():
    s, _ = store(resp(body=contents_body("{kaputt")))
    with pytest.raises(gs.CorruptFile):
        s.load()


def test_save_sends_content_sha_branch_and_author():
    s, http = store(resp(status=200, body={"content": {"sha": "new"}}))
    s.save([EVENT_TIMED], "abc123", "Trainer: „Kampfrichter-Lehrgang“ angelegt",
           gs.Author("Trainer", "admin+trainer@sgw-essen.local"))
    method, url, kw = http.calls[0]
    assert method == "PUT" and url.endswith("/contents/custom_events.json")
    payload = kw["json"]
    assert base64.b64decode(payload["content"]).decode("utf-8") == custom_events.serialize([EVENT_TIMED])
    assert payload["sha"] == "abc123" and payload["branch"] == "main"
    assert payload["message"] == "Trainer: „Kampfrichter-Lehrgang“ angelegt"
    assert payload["author"] == {"name": "Trainer", "email": "admin+trainer@sgw-essen.local"}


@pytest.mark.parametrize("status, headers, exc", [
    (401, {}, gs.Unauthorized),
    (403, {"X-RateLimit-Remaining": "0"}, gs.RateLimited),
    (403, {"Retry-After": "30"}, gs.RateLimited),
    (429, {}, gs.RateLimited),
    (403, {}, gs.Unauthorized),
    (409, {}, gs.Conflict),
    (422, {}, gs.Conflict),
    (500, {}, gs.Unavailable),
    (502, {}, gs.Unavailable),
    (404, {}, gs.StoreError),
])
def test_status_mapping(status, headers, exc):
    s, _ = store(resp(status=status, body={"message": "x"}, headers=headers))
    with pytest.raises(exc):
        s.save([], "sha", "m", gs.Author("A", "a@b"))


def test_network_error_is_unavailable():
    s, _ = store(requests.ConnectionError("down"))
    with pytest.raises(gs.Unavailable):
        s.load()


def test_unauthorized_flag_is_set_and_cleared():
    text = custom_events.serialize([])
    s, _ = store(resp(status=401, body={}), resp(body=contents_body(text)))
    with pytest.raises(gs.Unauthorized):
        s.load()
    assert s.unauthorized is True
    s.load()
    assert s.unauthorized is False


@pytest.mark.parametrize("value, expected", [
    ("2027-10-12 09:00:00 UTC", datetime(2027, 10, 12, 9, 0, tzinfo=UTC)),
    ("2027-10-12 11:00:00 +0200", datetime(2027, 10, 12, 9, 0, tzinfo=UTC)),
    ("", None),
    (None, None),
    ("irgendwann", None),
])
def test_parse_expiry(value, expected):
    assert gs.parse_expiry(value) == expected


def test_expiry_header_is_remembered():
    text = custom_events.serialize([])
    s, _ = store(resp(body=contents_body(text),
                      headers={"GitHub-Authentication-Token-Expiration": "2027-10-12 09:00:00 UTC"}))
    s.load()
    assert s.token_expiry == datetime(2027, 10, 12, 9, 0, tzinfo=UTC)


def test_read_text_asks_for_raw_content():
    s, http = store(resp(text="BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"))
    assert s.read_text("sgw_essen_herren_1.ics").startswith("BEGIN:VCALENDAR")
    _, url, kw = http.calls[0]
    assert url.endswith("/contents/sgw_essen_herren_1.ics")
    assert kw["headers"]["Accept"] == "application/vnd.github.raw+json"


def test_last_author_reads_newest_commit_of_the_file():
    s, http = store(resp(body=[{"commit": {"author": {"name": "Kapitän"}}}]), resp(body=[]))
    assert s.last_author() == "Kapitän"
    assert http.calls[0][2]["params"] == {"path": "custom_events.json", "sha": "main", "per_page": 1}
    assert s.last_author() is None


def test_check_token_calls_rate_limit():
    s, http = store(resp(body={}, headers={"GitHub-Authentication-Token-Expiration": "2027-10-12 09:00:00 UTC"}))
    assert s.check_token() == datetime(2027, 10, 12, 9, 0, tzinfo=UTC)
    assert http.calls[0][1] == "https://api.github.com/rate_limit"


def _non_json_200():
    return resp(status=200, text="<html>Wartungsarbeiten</html>")


def test_load_non_json_body_is_unavailable():
    s, _ = store(_non_json_200())
    with pytest.raises(gs.Unavailable):
        s.load()


@pytest.mark.parametrize("body", [
    {"sha": "abc"},                                                     # no content
    {"content": None, "sha": "abc"},                                    # wrong type
    {"content": "", "encoding": "none"},                                # no sha
    [{"content": "x"}],                                                 # listing, not a file
    {"content": "%%%not-base64%%%", "sha": "abc"},                      # bad base64
    {"content": base64.b64encode(b"\xff\xfe").decode(), "sha": "abc"},  # not UTF-8
])
def test_load_malformed_200_body_is_corrupt(body):
    s, _ = store(resp(status=200, body=body))
    with pytest.raises(gs.CorruptFile):
        s.load()


def test_last_author_unexpected_shape_is_none():
    s, _ = store(resp(body={"message": "surprise"}), resp(body=[{"commit": None}]),
                 resp(body=[]), _non_json_200())
    assert [s.last_author() for _ in range(4)] == [None, None, None, None]


def test_save_ignores_non_json_2xx_body():
    s, _ = store(_non_json_200())
    s.save([], "sha", "m", gs.Author("A", "a@b"))


def test_read_text_non_utf8_is_corrupt():
    undecodable = resp(text="")
    undecodable._content = b"\xff\xfe\x00"
    s, _ = store(undecodable)
    with pytest.raises(gs.CorruptFile):
        s.read_text("sgw_essen_damen.ics")


def test_non_ascii_save_payload_decodes_to_the_same_event():
    event = {**EVENT_TIMED, "title": "„Weihnachtsfeier“ in Düsseldorf",
             "location": "Löwenbad Düsseldorf"}
    s, http = store(resp(status=200, body={"content": {"sha": "new"}}))
    s.save([event], "abc123", "Trainer: „Weihnachtsfeier“ angelegt",
           gs.Author("Trainer", "admin+trainer@sgw-essen.local"))
    raw = base64.b64decode(http.calls[0][2]["json"]["content"]).decode("utf-8")
    parsed = custom_events.parse(raw).valid[0]
    assert parsed["title"] == "„Weihnachtsfeier“ in Düsseldorf"
    assert parsed["location"] == "Löwenbad Düsseldorf"


def test_token_never_appears_in_repr_or_errors():
    token = "ghp_GEHEIM_42"
    http = StubHttp(requests.ConnectionError(f"verbindung mit {token} abgebrochen"))
    s = gs.GitHubStore(token, "SeSIx/SGW-Essen-Calendar", "main", http=http)
    assert token not in repr(s) and token not in str(s)
    with pytest.raises(gs.StoreError) as info:
        s.load()
    assert token not in str(info.value) and token not in repr(info.value)
    assert token not in repr(info.value.args)
