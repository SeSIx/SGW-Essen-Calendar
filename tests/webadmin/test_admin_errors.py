"""Errors get a friendly German page; details stay in the log."""

import re

from admin.app import create_app
from webadmin.testdata import BASE, HttpsClient


def test_unexpected_error_page_hides_details(settings, store, clock, caplog):
    app = create_app(settings, store=store, clock=clock)
    app.test_client_class = HttpsClient

    def boom():
        raise RuntimeError("geheimes Detail")

    app.add_url_rule("/_boom", "boom", boom)
    r = app.test_client().get("/_boom")
    body = r.get_data(as_text=True)
    assert r.status_code == 500 and "Da ist etwas schiefgelaufen" in body
    assert "geheimes Detail" not in body and "Traceback" not in body
    ref = re.search(r"Fehler-ID: ([0-9a-f]{8})", body).group(1)
    assert ref in caplog.text


def test_not_found_page(user_client):
    r = user_client.get("/gibt-es-nicht")
    assert r.status_code == 404 and "Nicht gefunden" in r.get_data(as_text=True)


def test_csrf_failure_page(client):
    client.get("/login")
    r = client.post("/login", data={"login": "x"}, headers={"Origin": BASE})
    assert r.status_code == 400 and "bitte neu laden" in r.get_data(as_text=True).lower()


def test_bad_host_page(client):
    r = client.get("/login", base_url="https://evil.example")
    assert r.status_code == 400 and "Anfrage ungültig" in r.get_data(as_text=True)


def test_too_large_request(client):
    r = client.post("/login", data={"x": "a" * 70_000}, headers={"Origin": BASE})
    assert r.status_code == 413 and "zu lang" in r.get_data(as_text=True)


def test_error_pages_keep_security_headers(user_client):
    r = user_client.get("/gibt-es-nicht")
    assert r.status_code == 404
    assert r.headers["Cache-Control"] == "no-store"
    assert r.headers["Content-Security-Policy"].startswith("default-src 'self'")
    assert r.headers["X-Content-Type-Options"] == "nosniff"
