"""Application factory."""

import mimetypes
import time
from collections.abc import Callable

from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from admin import db, security, views
from admin.games import GameCache
from admin.github_store import Store
from admin.services import Services, build_store, close_db
from admin.settings import Settings, from_env

mimetypes.add_type("application/manifest+json", ".webmanifest")


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
    return app
