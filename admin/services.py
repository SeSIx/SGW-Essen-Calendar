"""Objects shared by all requests of one app instance."""

import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass

from flask import current_app, g

from admin import db
from admin.fake_store import FakeGitHubStore
from admin.games import GameCache
from admin.github_store import GitHubStore, Store
from admin.settings import Settings


@dataclass
class Services:
    settings: Settings
    store: Store
    games: GameCache
    clock: Callable[[], float] = time.time

    def now(self) -> int:
        return int(self.clock())


def services() -> Services:
    return current_app.extensions["sgw"]


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = db.connect(services().settings.db_path)
    return g.db


def close_db(_exc: BaseException | None = None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def build_store(settings: Settings) -> Store:
    if settings.fake_github_dir:
        return FakeGitHubStore(settings.fake_github_dir)
    return GitHubStore(settings.github_token, settings.repo, settings.branch)
