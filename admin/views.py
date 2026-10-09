"""Routes."""

from flask import Blueprint, g, request

from admin import auth
from admin.security import SESSION_COOKIE
from admin.services import get_db, services

bp = Blueprint("main", __name__)


def load_user() -> None:
    g.user = None
    if request.path.startswith("/static/"):
        return
    raw = request.cookies.get(SESSION_COOKIE, "")
    if 20 <= len(raw) <= 100:
        g.user = auth.load_session(get_db(), raw, services().now())


@bp.get("/healthz")
def healthz():
    return "ok", 200, {"Content-Type": "text/plain; charset=utf-8"}
