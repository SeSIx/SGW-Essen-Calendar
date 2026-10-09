"""Routes."""

import functools
import re

from flask import Blueprint, g, redirect, render_template, request, url_for

from admin import auth, security
from admin.security import SESSION_COOKIE
from admin.services import get_db, services

bp = Blueprint("main", __name__)

TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{20,100}")
DEAD_LINK = "Dieser Link ist ungültig oder abgelaufen. Bitte Julius um einen neuen."


def load_user() -> None:
    g.user = None
    if request.path.startswith("/static/"):
        return
    raw = request.cookies.get(SESSION_COOKIE, "")
    if 20 <= len(raw) <= 100:
        g.user = auth.load_session(get_db(), raw, services().now())


def login_required(view):
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("main.login"))
        return view(*args, **kwargs)
    return wrapped


@bp.get("/healthz")
def healthz():
    return "ok", 200, {"Content-Type": "text/plain; charset=utf-8"}


@bp.get("/")
@login_required
def index():
    return render_template("start.html")


@bp.route("/login", methods=["GET", "POST"])
def login():
    if g.user is not None:
        return redirect(url_for("main.index"))
    if request.method != "POST":  # GET or HEAD
        return render_template("login.html", error=None, login_name="")
    name = request.form.get("login", "")[:64]
    result = auth.login(get_db(), name, request.form.get("password", "")[:256],
                        request.remote_addr or "unknown", services().now())
    if result.error:
        return render_template("login.html", error=result.error, login_name=name), 401
    resp = redirect(url_for("main.index"))
    security.set_session_cookie(resp, result.session)
    return resp


@bp.route("/einladung/<token>", methods=["GET", "POST"])
def invite(token):
    conn, now = get_db(), services().now()
    link = auth.peek_token(conn, token, now) if TOKEN_RE.fullmatch(token) else None
    if link is None:
        return render_template("message.html", title="Link ungültig", text=DEAD_LINK), 404
    if request.method != "POST":  # GET and HEAD (link previews) never consume the link
        return render_template("invite.html", link=link, error=None)
    password = request.form.get("password", "")[:256]
    if password != request.form.get("password2", "")[:256]:
        return render_template("invite.html", link=link,
                               error="Die beiden Passwörter stimmen nicht überein"), 400
    try:
        session = auth.redeem_token(conn, token, password, now)
    except auth.PasswordRejected as exc:
        return render_template("invite.html", link=link, error=str(exc)), 400
    except auth.InvalidLink:
        return render_template("message.html", title="Link ungültig", text=DEAD_LINK), 404
    resp = redirect(url_for("main.index"))
    security.set_session_cookie(resp, session)
    return resp


@bp.post("/abmelden")
@login_required
def logout():
    auth.logout(get_db(), request.cookies.get(security.SESSION_COOKIE, ""))
    resp = redirect(url_for("main.login"))
    security.clear_session_cookie(resp)
    return resp


@bp.post("/abmelden/ueberall")
@login_required
def logout_everywhere():
    auth.logout_all(get_db(), g.user.user_id)
    resp = redirect(url_for("main.login"))
    security.clear_session_cookie(resp)
    return resp
