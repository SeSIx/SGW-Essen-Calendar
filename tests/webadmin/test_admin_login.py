"""Login, invite links and logout through the real routes."""

import re
from pathlib import Path

import pytest

from admin import auth, db
from webadmin.testdata import BASE, PW, csrf_of, make_user, post_form

TEMPLATES = Path(__file__).resolve().parents[2] / "admin" / "templates"


def cookie(resp, name):
    return next(h for h in resp.headers.getlist("Set-Cookie") if h.startswith(name + "="))


def invite_link(app, login="trainer", name="Trainer"):
    svc = app.extensions["sgw"]
    conn = db.connect(svc.settings.db_path)
    try:
        auth.create_user(conn, login, name, svc.now())
        return "/einladung/" + auth.issue_token(conn, login, "invite", svc.now())
    finally:
        conn.close()


def login(client, password=PW):
    return post_form(client, "/login", {"login": "julius", "password": password})


def test_login_page_has_csrf_and_pre_cookie(client):
    r = client.get("/login")
    assert r.status_code == 200 and csrf_of(r)
    c = cookie(r, "__Host-pre")
    for flag in ("Secure", "HttpOnly", "SameSite=Lax", "Path=/"):
        assert flag in c
    assert "Domain=" not in c


def test_anonymous_start_page_redirects_to_login(client):
    r = client.get("/")
    assert r.status_code == 302 and r.headers["Location"].endswith("/login")


def test_login_success_sets_session_cookie(app, client):
    make_user(app)
    r = login(client)
    assert r.status_code == 302 and r.headers["Location"].endswith("/")
    c = cookie(r, "__Host-sid")
    for flag in ("Secure", "HttpOnly", "SameSite=Lax", "Path=/", "Max-Age=2592000"):
        assert flag in c
    assert "Domain=" not in c
    assert "Hallo Julius" in client.get("/").get_data(as_text=True)


@pytest.mark.parametrize("name, password", [("julius", "falsch-falsch-falsch"), ("niemand", PW)])
def test_login_failure_is_generic(app, client, name, password):
    make_user(app)
    r = post_form(client, "/login", {"login": name, "password": password})
    assert r.status_code == 401
    assert "Name oder Passwort falsch" in r.get_data(as_text=True)


def test_ip_limit_uses_the_address_forwarded_by_traefik(client):
    token = csrf_of(client.get("/login"))

    def attempt(ip, name="x"):
        return client.post("/login", data={"login": name, "password": "y", "csrf_token": token},
                           headers={"Origin": BASE, "X-Forwarded-For": ip})

    # Failures lock per name, so each attempt types a different one; only the IP is shared.
    for i in range(auth.IP_LIMIT):
        attempt("203.0.113.5", f"x{i}")
    assert "Zu viele Versuche – bitte in 15 Minuten erneut" in attempt("203.0.113.5", "y").get_data(as_text=True)
    assert "Name oder Passwort falsch" in attempt("203.0.113.6", "y").get_data(as_text=True)


def test_login_without_csrf_or_from_foreign_origin(client):
    token = csrf_of(client.get("/login"))
    assert client.post("/login", data={"login": "x", "password": "y"},
                       headers={"Origin": BASE}).status_code == 400
    assert client.post("/login", data={"login": "x", "password": "y", "csrf_token": token},
                       headers={"Origin": "https://evil.example"}).status_code == 403


def test_logged_in_user_skips_login_page(user_client):
    r = user_client.get("/login")
    assert r.status_code == 302 and r.headers["Location"].endswith("/")


def test_head_and_get_do_not_consume_invite(app, client):
    link = invite_link(app)
    assert client.head(link).status_code == 200
    page = client.get(link)
    assert "Hallo Trainer" in page.get_data(as_text=True)
    r = post_form(client, link, {"password": PW, "password2": PW}, page=client.get(link))
    assert r.status_code == 302
    assert cookie(r, "__Host-sid")
    assert client.get(link).status_code == 404, "used links are dead"


def test_invite_rejects_mismatch_and_weak_password_but_stays_usable(app, client):
    link = invite_link(app)
    r = post_form(client, link, {"password": PW, "password2": PW + "x"})
    assert r.status_code == 400 and "stimmen nicht überein" in r.get_data(as_text=True)
    r = post_form(client, link, {"password": "kurz", "password2": "kurz"})
    assert r.status_code == 400 and "Mindestens 12 Zeichen" in r.get_data(as_text=True)
    assert post_form(client, link, {"password": PW, "password2": PW}).status_code == 302


def test_expired_or_bogus_invite_is_404(app, client, clock):
    link = invite_link(app)
    clock.t += auth.TOKEN_TTL
    assert client.get(link).status_code == 404
    assert client.get("/einladung/" + "x" * 43).status_code == 404
    assert client.get("/einladung/kurz").status_code == 404


def test_cross_site_navigation_keeps_session(user_client):
    r = user_client.get("/", headers={"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "navigate"})
    assert r.status_code == 200 and "Hallo Julius" in r.get_data(as_text=True)


def test_logout_ends_this_session_only(app, client):
    make_user(app)
    other = app.test_client()
    login(client)
    login(other)
    r = post_form(client, "/abmelden", {}, page=client.get("/"))
    assert r.status_code == 302 and r.headers["Location"].endswith("/login")
    assert "Max-Age=0" in cookie(r, "__Host-sid")
    assert client.get("/").status_code == 302
    assert other.get("/").status_code == 200


def test_logout_everywhere(app, client):
    make_user(app)
    other = app.test_client()
    login(client)
    login(other)
    r = post_form(client, "/abmelden/ueberall", {}, page=client.get("/"))
    assert r.status_code == 302 and r.headers["Location"].endswith("/login")
    assert "Max-Age=0" in cookie(r, "__Host-sid")
    assert client.get("/").status_code == 302
    assert other.get("/").status_code == 302


def test_start_page_greets_inside_main(user_client):
    html = user_client.get("/").get_data(as_text=True)
    assert re.search(r"<main>.*Angemeldet\..*</main>", html, re.S)


def _heights(css, selector):
    found = []
    for sel, body in re.findall(r"([^{}]+)\{([^}]*)\}", css):
        if selector in [s.strip() for s in sel.split(",")]:
            found += [int(v) for v in re.findall(r"(?<![\w-])(?:min-)?height:\s*(\d+)px", body)]
    return found


@pytest.mark.parametrize("selector", [".chip", "button", ".more", ".menu summary", ".fab", ".form input"])
def test_touch_targets_are_at_least_44px(selector):
    css = (TEMPLATES.parent / "static" / "app.css").read_text(encoding="utf-8")
    heights = _heights(css, selector)
    assert heights and max(heights) >= 44, selector


def test_stale_session_post_redirects_to_login(app, client):
    make_user(app)
    other = app.test_client()
    login(client)
    login(other)
    page = other.get("/")
    post_form(client, "/abmelden/ueberall", {}, page=client.get("/"))
    r = other.post("/abmelden", data={"csrf_token": csrf_of(page)},
                   headers={"Origin": BASE, "Sec-Fetch-Site": "same-origin"})
    assert r.status_code == 303 and r.headers["Location"].endswith("/login")
    assert "Max-Age=0" in cookie(r, "__Host-sid")


def test_bad_csrf_without_session_cookie_stays_400(client):
    r = client.post("/login", data={"login": "a", "password": "b", "csrf_token": "x"},
                    headers={"Origin": BASE, "Sec-Fetch-Site": "same-origin"})
    assert r.status_code == 400


def test_static_files_are_cacheable_and_protected(client):
    r = client.get("/static/app.css")
    assert r.status_code == 200 and "no-store" not in r.headers.get("Cache-Control", "")
    assert r.headers["Content-Security-Policy"].startswith("default-src 'self'")
    assert client.get("/static/manifest.webmanifest").mimetype == "application/manifest+json"


def test_templates_have_no_inline_script_or_style():
    for path in TEMPLATES.glob("*.html"):
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"<script(?![^>]*\bsrc=)", text), path.name
        assert "style=" not in text and "<style" not in text, path.name
        assert not re.search(r"\son[a-z]+=", text), path.name
