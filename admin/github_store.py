"""custom_events.json and the team calendars, read and written through the GitHub API.

The Contents API is used instead of raw.githubusercontent.com because the raw
host caches for up to five minutes: right after saving, people would see the
old state.
"""

import base64
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import requests

import custom_events

API = "https://api.github.com"
EVENTS_PATH = "custom_events.json"
TIMEOUT = 10


class StoreError(Exception):
    pass


class Conflict(StoreError):
    """The file changed between our read and our write (HTTP 409/422)."""


class Unauthorized(StoreError):
    """Token missing, expired, revoked or without the needed permission."""


class RateLimited(StoreError):
    pass


class Unavailable(StoreError):
    pass


class CorruptFile(StoreError):
    """custom_events.json is not a JSON list at all, so nothing in it can be trusted."""


@dataclass(frozen=True)
class Snapshot:
    result: custom_events.ParseResult
    sha: str


@dataclass(frozen=True)
class Author:
    name: str
    email: str


class Store(Protocol):
    token_expiry: datetime | None
    unauthorized: bool

    def load(self) -> Snapshot: ...
    def save(self, entries: list, sha: str, message: str, author: Author) -> None: ...
    def read_text(self, path: str) -> str: ...
    def last_author(self) -> str | None: ...
    def check_token(self) -> datetime | None: ...


def parse_expiry(value: str | None) -> datetime | None:
    """Read GitHub-Authentication-Token-Expiration, e.g. '2027-10-12 09:00:00 UTC'."""
    if not value:
        return None
    text = value.strip().replace(" UTC", " +0000")
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S %z").astimezone(UTC)
    except ValueError:
        return None


class GitHubStore:
    def __init__(self, token: str, repo: str, branch: str, http: requests.Session | None = None):
        self._repo = repo
        self._branch = branch
        self._http = http or requests.Session()
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "sgw-admin",
        }
        self.token_expiry: datetime | None = None
        self.unauthorized = False

    def _contents_url(self, path: str) -> str:
        return f"{API}/repos/{self._repo}/contents/{path}"

    def _call(self, method: str, url: str, *, accept: str | None = None, **kwargs) -> requests.Response:
        headers = dict(self._headers)
        if accept:
            headers["Accept"] = accept
        try:
            resp = self._http.request(method, url, headers=headers, timeout=TIMEOUT, **kwargs)
        except requests.RequestException as exc:
            raise Unavailable(str(exc)) from exc
        expiry = parse_expiry(resp.headers.get("GitHub-Authentication-Token-Expiration"))
        if expiry is not None:
            self.token_expiry = expiry
        status = resp.status_code
        if status == 401:
            self.unauthorized = True
            raise Unauthorized("GitHub hat den Token abgelehnt")
        if status == 429 or (status == 403 and (
                resp.headers.get("X-RateLimit-Remaining") == "0" or "Retry-After" in resp.headers)):
            raise RateLimited("GitHub-Rate-Limit erreicht")
        if status == 403:
            self.unauthorized = True
            raise Unauthorized("Dem Token fehlt die Berechtigung")
        if status in (409, 422):
            raise Conflict(f"GitHub meldet {status}")
        if status >= 500:
            raise Unavailable(f"GitHub antwortet mit {status}")
        if status >= 400:
            raise StoreError(f"Unerwartete Antwort von GitHub: {status}")
        self.unauthorized = False
        return resp

    def load(self) -> Snapshot:
        body = self._call("GET", self._contents_url(EVENTS_PATH), params={"ref": self._branch}).json()
        text = base64.b64decode(body["content"]).decode("utf-8")
        try:
            result = custom_events.parse(text)
        except ValueError as exc:
            raise CorruptFile(str(exc)) from exc
        return Snapshot(result=result, sha=body["sha"])

    def save(self, entries: list, sha: str, message: str, author: Author) -> None:
        content = custom_events.serialize(entries).encode("utf-8")
        self._call("PUT", self._contents_url(EVENTS_PATH), json={
            "message": message,
            "content": base64.b64encode(content).decode("ascii"),
            "sha": sha,
            "branch": self._branch,
            "author": {"name": author.name, "email": author.email},
        })

    def read_text(self, path: str) -> str:
        resp = self._call("GET", self._contents_url(path), params={"ref": self._branch},
                          accept="application/vnd.github.raw+json")
        return resp.content.decode("utf-8")

    def last_author(self) -> str | None:
        commits = self._call("GET", f"{API}/repos/{self._repo}/commits", params={
            "path": EVENTS_PATH, "sha": self._branch, "per_page": 1}).json()
        return commits[0]["commit"]["author"]["name"] if commits else None

    def check_token(self) -> datetime | None:
        self._call("GET", f"{API}/rate_limit")
        return self.token_expiry
