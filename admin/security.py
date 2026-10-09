"""Checks every request passes, and headers every response carries.

POSTs need both a same-origin browser signal and a CSRF token. Logged-in pages
take the token from the session row; before login it is an HMAC over a random
value in the __Host-pre cookie.
"""

import hashlib
import hmac
import secrets
from collections.abc import Callable

from flask import Flask, abort, g, request
from werkzeug.wrappers import Response

from admin.auth import SESSION_TTL
from admin.services import services

CSP = ("default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; "
       "form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
SESSION_COOKIE = "__Host-sid"
PRE_COOKIE = "__Host-pre"
PRE_COOKIE_TTL = 24 * 3600


def init_app(app: Flask, load_user: Callable[[], None]) -> None:
    app.before_request(_check_host)
    app.before_request(load_user)
    app.before_request(_check_post)
    app.after_request(_set_headers)
    app.jinja_env.globals["csrf_token"] = csrf_token


def _check_host() -> None:
    if request.host != services().settings.host:
        abort(400)


def _origin_ok() -> bool:
    # Referrer-Policy: no-referrer makes browsers send "Origin: null" on form posts,
    # so Fetch Metadata is the main signal; Origin decides only for browsers without it.
    expected = services().settings.origin
    site = request.headers.get("Sec-Fetch-Site")
    origin = request.headers.get("Origin")
    if site is not None:
        return site == "same-origin" and origin in (None, "null", expected)
    return origin == expected


def _check_post() -> None:
    if request.method != "POST":
        return
    if not _origin_ok():
        abort(403)
    sent = request.form.get("csrf_token", "")
    if not sent or not hmac.compare_digest(sent, csrf_token()):
        abort(400)


def csrf_token() -> str:
    user = g.get("user")
    if user is not None:
        return user.csrf_token
    raw = request.cookies.get(PRE_COOKIE, "")
    if not 20 <= len(raw) <= 100:
        raw = g.get("new_pre") or secrets.token_urlsafe(32)
        g.new_pre = raw
    key = services().settings.secret_key.encode("utf-8")
    return hmac.new(key, b"pre:" + raw.encode("utf-8"), hashlib.sha256).hexdigest()


def set_session_cookie(resp: Response, raw: str) -> None:
    resp.set_cookie(SESSION_COOKIE, raw, max_age=SESSION_TTL, secure=True, httponly=True,
                    samesite="Lax", path="/")


def clear_session_cookie(resp: Response) -> None:
    resp.delete_cookie(SESSION_COOKIE, path="/", secure=True, httponly=True, samesite="Lax")


def _set_headers(resp: Response) -> Response:
    resp.headers["Content-Security-Policy"] = CSP
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer"
    if services().settings.scheme == "https":
        resp.headers["Strict-Transport-Security"] = "max-age=31536000"
    if not request.path.startswith("/static/"):
        resp.headers["Cache-Control"] = "no-store"
    new_pre = g.get("new_pre")
    if new_pre:
        resp.set_cookie(PRE_COOKIE, new_pre, max_age=PRE_COOKIE_TTL, secure=True,
                        httponly=True, samesite="Lax", path="/")
    return resp
