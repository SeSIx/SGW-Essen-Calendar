"""Runtime configuration, read once from the environment.

Every combination that would run the app half-protected refuses to start.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass

PRODUCTION_HOST = "sgw-admin.srv1136792.hstgr.cloud"


@dataclass(frozen=True)
class Settings:
    env: str
    secret_key: str
    host: str
    scheme: str
    db_path: str
    github_token: str | None
    repo: str
    branch: str
    fake_github_dir: str | None

    @property
    def origin(self) -> str:
        return f"{self.scheme}://{self.host}"


def from_env(environ: Mapping[str, str] = os.environ) -> Settings:
    env = environ.get("SGW_ADMIN_ENV", "production")
    secret = environ.get("SECRET_KEY", "")
    if len(secret) < 32:
        raise RuntimeError("SECRET_KEY fehlt oder ist kürzer als 32 Zeichen")
    host = environ.get("SGW_ADMIN_HOST", PRODUCTION_HOST)
    scheme = environ.get("SGW_ADMIN_SCHEME", "https")
    fake = environ.get("SGW_ADMIN_FAKE_GITHUB") == "1"
    if env == "production":
        if fake:
            raise RuntimeError("SGW_ADMIN_FAKE_GITHUB ist in production verboten")
        if host != PRODUCTION_HOST or scheme != "https":
            raise RuntimeError(f"production läuft nur unter https://{PRODUCTION_HOST}")
    fake_dir = None
    if fake:
        if host.split(":")[0] not in ("localhost", "127.0.0.1"):
            raise RuntimeError("Fake-GitHub nur mit Host localhost oder 127.0.0.1")
        fake_dir = environ.get("SGW_ADMIN_FAKE_DIR")
        if not fake_dir:
            raise RuntimeError("SGW_ADMIN_FAKE_DIR fehlt")
    token = environ.get("GITHUB_TOKEN") or None
    if not fake and not token:
        raise RuntimeError("GITHUB_TOKEN fehlt")
    return Settings(
        env=env, secret_key=secret, host=host, scheme=scheme,
        db_path=environ.get("SGW_ADMIN_DB", "/data/sgw-admin.db"),
        github_token=token,
        repo=environ.get("SGW_ADMIN_REPO", "SeSIx/SGW-Essen-Calendar"),
        branch=environ.get("SGW_ADMIN_BRANCH", "main"),
        fake_github_dir=fake_dir)
