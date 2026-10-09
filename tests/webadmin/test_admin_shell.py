"""Checks every request passes and headers every response carries."""

import pytest

from admin import security
from admin.app import create_app
from webadmin.testdata import BASE, HOST, HttpsClient

CSP = ("default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; "
       "form-action 'self'; frame-ancestors 'none'; base-uri 'none'")


@pytest.fixture
def probe(settings, store, clock):
    """An app with two extra routes to exercise the CSRF machinery directly."""
    flask_app = create_app(settings, store=store, clock=clock)
    flask_app.test_client_class = HttpsClient
    flask_app.add_url_rule("/_probe", "probe_get", lambda: {"csrf": security.csrf_token()})
    flask_app.add_url_rule("/_probe", "probe_post", lambda: "ok", methods=["POST"])
    return flask_app.test_client()


def test_healthz(client):
    r = client.get("/healthz")
    assert (r.status_code, r.get_data(as_text=True)) == (200, "ok")


def test_security_headers(client):
    h = client.get("/healthz").headers
    assert h["Content-Security-Policy"] == CSP
    assert h["Strict-Transport-Security"] == "max-age=31536000"
    assert h["X-Content-Type-Options"] == "nosniff"
    assert h["Referrer-Policy"] == "no-referrer"
    assert h["Cache-Control"] == "no-store"


def test_unknown_host_is_rejected(client):
    assert client.get("/healthz", base_url="https://evil.example").status_code == 400


def test_host_forwarded_by_traefik_is_accepted(client):
    r = client.get("/healthz", base_url="http://sgw-admin:8000",
                   headers={"X-Forwarded-Host": HOST, "X-Forwarded-Proto": "https"})
    assert r.status_code == 200


def test_pre_session_csrf_round_trip(probe):
    token = probe.get("/_probe").get_json()["csrf"]
    ok = probe.post("/_probe", data={"csrf_token": token}, headers={"Origin": BASE})
    assert ok.status_code == 200


def test_pre_session_cookie_flags(probe):
    cookie = next(c for c in probe.get("/_probe").headers.getlist("Set-Cookie")
                  if c.startswith("__Host-pre="))
    for flag in ("Secure", "HttpOnly", "SameSite=Lax", "Path=/"):
        assert flag in cookie


@pytest.mark.parametrize("data, headers, status", [
    ({}, {"Origin": BASE}, 400),
    ({"csrf_token": "ä"}, {"Origin": BASE}, 400),
    ({"csrf_token": "TOKEN"}, {"Sec-Fetch-Site": "same-site", "Origin": BASE}, 403),
    ({"csrf_token": "TOKEN"}, {"Sec-Fetch-Site": "none", "Origin": BASE}, 403),
    ({"csrf_token": "falsch"}, {"Origin": BASE}, 400),
    ({"csrf_token": "TOKEN"}, {"Origin": "https://evil.example"}, 403),
    ({"csrf_token": "TOKEN"}, {"Sec-Fetch-Site": "cross-site", "Origin": BASE}, 403),
    ({"csrf_token": "TOKEN"}, {}, 403),
    # Older Safari sends no Sec-Fetch-Site; the CSRF token is still required.
    ({"csrf_token": "TOKEN"}, {"Origin": "null"}, 200),
    ({"csrf_token": "falsch"}, {"Origin": "null"}, 400),
    ({"csrf_token": "TOKEN"}, {"Sec-Fetch-Site": "same-origin"}, 200),
    # What Chrome really sends for a form post under Referrer-Policy: no-referrer:
    ({"csrf_token": "TOKEN"}, {"Sec-Fetch-Site": "same-origin", "Origin": "null"}, 200),
    ({"csrf_token": "TOKEN"}, {"Sec-Fetch-Site": "same-origin", "Origin": "https://evil.example"}, 403),
])
def test_post_checks(probe, data, headers, status):
    token = probe.get("/_probe").get_json()["csrf"]
    data = {k: token if v == "TOKEN" else v for k, v in data.items()}
    assert probe.post("/_probe", data=data, headers=headers).status_code == status


def test_token_of_another_browser_is_rejected(probe, settings, store, clock):
    other = create_app(settings, store=store, clock=clock)
    other.test_client_class = HttpsClient
    other.add_url_rule("/_probe", "probe_get", lambda: {"csrf": security.csrf_token()})
    foreign = other.test_client().get("/_probe").get_json()["csrf"]
    probe.get("/_probe")
    assert probe.post("/_probe", data={"csrf_token": foreign}, headers={"Origin": BASE}).status_code == 400


def test_put_never_reaches_handler(settings, store, clock):
    flask_app = create_app(settings, store=store, clock=clock)
    flask_app.test_client_class = HttpsClient
    reached = []
    flask_app.add_url_rule("/_put", "put", lambda: reached.append(1) or "ok", methods=["PUT", "DELETE", "PATCH"])
    c = flask_app.test_client()
    for method in (c.put, c.delete, c.patch):
        assert method("/_put").status_code in (400, 403)
        assert method("/_put", headers={"Origin": BASE}).status_code == 400
    assert reached == []
    assert c.put("/healthz").status_code == 403


def test_error_responses_carry_headers(client):
    for r in (client.get("/healthz", base_url="https://evil.example"), client.post("/healthz")):
        assert r.status_code in (400, 403)
        assert r.headers["Cache-Control"] == "no-store"
        assert r.headers["Content-Security-Policy"] == CSP


def test_no_hsts_over_http(settings, store, clock):
    from dataclasses import replace
    s = replace(settings, scheme="http", host="localhost:8099")
    c = create_app(s, store=store, clock=clock).test_client()
    r = c.get("/healthz", base_url="http://localhost:8099")
    assert r.status_code == 200 and "Strict-Transport-Security" not in r.headers


def test_static_is_cacheable(client):
    assert "no-store" not in client.get("/static/nothing.css").headers.get("Cache-Control", "")


def test_pre_session_token_dies_at_login(app, client):
    from webadmin.testdata import make_user

    app.add_url_rule("/_probe", "probe_get", lambda: {"csrf": security.csrf_token()})
    app.add_url_rule("/_probe", "probe_post", lambda: "ok", methods=["POST"])
    pre = client.get("/_probe").get_json()["csrf"]
    client.set_cookie("__Host-sid", make_user(app), domain=HOST)
    assert client.post("/_probe", data={"csrf_token": pre}, headers={"Origin": BASE}).status_code == 400
    sess = client.get("/_probe").get_json()["csrf"]
    assert sess != pre
    assert client.post("/_probe", data={"csrf_token": sess}, headers={"Origin": BASE}).status_code == 200
