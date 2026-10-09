"""File-backed stand-in for GitHub, used by the tests and the local E2E container.

State lives in files, not in memory: every gunicorn worker is its own process
and all of them must see the same "repository".
"""

import fcntl
import hashlib
from datetime import datetime
from pathlib import Path

import custom_events
from admin.github_store import (
    EVENTS_PATH,
    Author,
    Conflict,
    CorruptFile,
    Snapshot,
    StoreError,
    Unauthorized,
)


class FakeGitHubStore:
    def __init__(self, directory: str | Path):
        self._dir = Path(directory)
        self.token_expiry: datetime | None = None
        self.unauthorized = False
        self.fail: dict[str, list[StoreError]] = {"load": [], "save": [], "read_text": []}
        self.saves = 0

    @staticmethod
    def _sha(data: bytes) -> str:
        return hashlib.sha1(data).hexdigest()

    def _maybe_fail(self, method: str) -> None:
        if self.fail[method]:
            exc = self.fail[method].pop(0)
            if isinstance(exc, Unauthorized):
                self.unauthorized = True
            raise exc
        self.unauthorized = False

    def load(self) -> Snapshot:
        self._maybe_fail("load")
        data = (self._dir / EVENTS_PATH).read_bytes()
        try:
            result = custom_events.parse(data.decode("utf-8"))
        except ValueError as exc:
            raise CorruptFile(str(exc)) from exc
        return Snapshot(result=result, sha=self._sha(data))

    def save(self, entries: list, sha: str, message: str, author: Author) -> None:
        self._maybe_fail("save")
        path = self._dir / EVENTS_PATH
        with open(self._dir / ".lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self._sha(path.read_bytes()) != sha:
                raise Conflict("409 (fake)")
            path.write_text(custom_events.serialize(entries), encoding="utf-8")
            (self._dir / ".last-commit").write_text(
                f"{author.name}\n{author.email}\n{message}\n", encoding="utf-8")
        self.saves += 1

    def read_text(self, path: str) -> str:
        self._maybe_fail("read_text")
        try:
            return (self._dir / path).read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise StoreError(f"{path} fehlt") from exc

    def last_commit(self) -> tuple[str, str, str] | None:
        marker = self._dir / ".last-commit"
        if not marker.exists():
            return None
        name, email, message = marker.read_text(encoding="utf-8").splitlines()[:3]
        return name, email, message

    def last_author(self) -> str | None:
        commit = self.last_commit()
        return commit[0] if commit else None

    def check_token(self) -> datetime | None:
        return self.token_expiry
