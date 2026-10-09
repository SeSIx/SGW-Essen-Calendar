"""Application factory."""

import functools
import mimetypes
import secrets
import time
from collections.abc import Callable

from flask import Flask, current_app, render_template
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix

from admin import db, security, views
from admin.games import GameCache
from admin.github_store import Store
from admin.services import Services, build_store, close_db
from admin.settings import Settings, from_env

mimetypes.add_type("application/manifest+json", ".webmanifest")


ERROR_PAGES = {
    400: ("Anfrage ungültig", "Die Seite ist veraltet oder die Sitzung abgelaufen – bitte neu laden und nochmal versuchen."),
    403: ("Nicht erlaubt", "Diese Anfrage kam nicht von dieser Seite."),
    404: ("Nicht gefunden", "Diese Seite oder dieser Termin existiert nicht (mehr)."),
    405: ("Nicht möglich", "Diese Aktion ist so nicht möglich."),
    413: ("Zu groß", "Die Eingaben sind zu lang."),
}


def _http_error(_exc, *, title: str, text: str, code: int):
    return render_template("message.html", title=title, text=text), code


def _unexpected(exc: Exception):
    if isinstance(exc, HTTPException):
        return exc
    ref = secrets.token_hex(4)
    current_app.logger.error("Unerwarteter Fehler, Fehler-ID %s", ref, exc_info=exc)
    return render_template("error.html", ref=ref), 500


def create_app(settings: Settings | None = None, store: Store | None = None,
               clock: Callable[[], float] = time.time) -> Flask:
    settings = settings or from_env()
    store = store or build_store(settings)
    app = Flask(__name__)
    app.config.update(SECRET_KEY=settings.secret_key, MAX_CONTENT_LENGTH=64 * 1024)
    app.extensions["sgw"] = Services(settings=settings, store=store, games=GameCache(store), clock=clock)
    # Exactly one proxy (Traefik) sits in front; trust one hop of X-Forwarded-*.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

    conn = db.connect(settings.db_path)
    try:
        db.init_schema(conn)
    finally:
        conn.close()

    security.init_app(app, views.load_user)
    app.register_blueprint(views.bp)
    app.teardown_appcontext(close_db)
    for code, (title, text) in ERROR_PAGES.items():
        app.register_error_handler(code, functools.partial(_http_error, title=title, text=text, code=code))
    app.register_error_handler(Exception, _unexpected)
    return app
