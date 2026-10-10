#!/usr/bin/env python3
"""Warn Julius over WhatsApp before the SGW-Admin GitHub token expires.

Runs daily on the host from sgw-token-watch.timer, not in the app container,
so the internet-facing app never holds the Evolution key. Standard library
only: the host's python3 runs it without a virtualenv. Secrets are read from
the files that already hold them and are never copied or logged.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
STAGES = (14, 7, 3, 1, 0)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2

GITHUB_RATE_LIMIT_URL = "https://api.github.com/rate_limit"
EXPIRY_HEADER = "github-authentication-token-expiration"
SEND_BACKOFF_SECONDS = (2, 4)
TIMEOUT_SECONDS = 10

RENEW_STEPS = (
    "So erneuerst du ihn (ca. 2 Min.):\n"
    "1. github.com/settings/personal-access-tokens → „SGW-Admin“ → Regenerate (366 Tage)\n"
    "2. Auf dem Server: nano /root/sgw-admin/admin/.env → GITHUB_TOKEN ersetzen\n"
    "3. cd /root/sgw-admin && docker compose -f admin/compose.yml up -d\n"
    "4. Prüfen: docker exec sgw-admin sgw-admin token-status"
)

_RECIPIENT = re.compile(r"\d{8,15}")
_COMPOSE_KEY = re.compile(
    r"""^\s*-?\s*["']?AUTHENTICATION_API_KEY["']?\s*[=:]\s*["']?([^\s"'#]+)""")
_COMPOSE_TAIL = re.compile(r"""["']?\s*(#.*)?""")


class ConfigError(Exception):
    """A required setting or secret is missing or malformed."""


@dataclass(frozen=True)
class Config:
    recipient: str
    github_env_file: Path = Path("/root/sgw-admin/admin/.env")
    evolution_compose: Path = Path("/root/evolution/docker-compose.yml")
    evolution_url: str = "http://127.0.0.1:8080"
    evolution_instance: str = "SGW Bot"
    state_file: Path = Path("/var/lib/sgw-token-watch/state.json")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigError(f"{path} nicht lesbar ({exc.__class__.__name__})") from exc


def _read_key_values(path: Path) -> dict[str, str]:
    text = _read_text(path)
    values = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def load_config(path: Path) -> Config:
    values = _read_key_values(path)
    recipient = values.get("RECIPIENT", "")
    if not _RECIPIENT.fullmatch(recipient):
        raise ConfigError(f"RECIPIENT in {path} fehlt oder ist keine Nummer im Format 49…")
    overrides = {}
    for key, field, convert in (
        ("GITHUB_ENV_FILE", "github_env_file", Path),
        ("EVOLUTION_COMPOSE", "evolution_compose", Path),
        ("EVOLUTION_URL", "evolution_url", str),
        ("EVOLUTION_INSTANCE", "evolution_instance", str),
        ("STATE_FILE", "state_file", Path),
    ):
        if values.get(key):
            overrides[field] = convert(values[key])
    return Config(recipient=recipient, **overrides)


def read_env_value(path: Path, key: str) -> str:
    value = _read_key_values(path).get(key, "")
    if not value:
        raise ConfigError(f"{key} fehlt in {path}")
    return value


def read_evolution_key(compose_path: Path) -> str:
    text = _read_text(compose_path)
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        match = _COMPOSE_KEY.match(line)
        if not match:
            continue
        if match.group(1).startswith("$"):
            raise ConfigError(
                f"AUTHENTICATION_API_KEY in {compose_path} ist eine Variablen-Referenz")
        if not _COMPOSE_TAIL.fullmatch(line[match.end():]):
            raise ConfigError(
                f"AUTHENTICATION_API_KEY in {compose_path} hat unerwarteten Text hinter dem Wert")
        return match.group(1)
    raise ConfigError(f"AUTHENTICATION_API_KEY nicht in {compose_path} gefunden")


def parse_expiry(value: str) -> datetime:
    text = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S %z"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    raise ValueError(f"unbekanntes Format des Ablaufdatums: {value!r}")


def days_left(expiry: datetime, today: date) -> int:
    return (expiry.astimezone(BERLIN).date() - today).days


def due_stage(days: int) -> int | None:
    reached = [stage for stage in STAGES if days <= stage]
    return min(reached) if reached else None


def _days_phrase(days: int) -> str:
    if days == 0:
        return "heute"
    if days == 1:
        return "morgen"
    return f"in {days} Tagen"


def expiry_message(expiry_day: date, days: int) -> str:
    if days < 0:
        return (f"⚠️ SGW-Admin: Der GitHub-Zugang ist am {expiry_day:%d.%m.%Y} abgelaufen "
                f"(vor {-days} Tagen).\n\n{RENEW_STEPS}")
    return (f"⚠️ SGW-Admin: Der GitHub-Zugang läuft am {expiry_day:%d.%m.%Y} ab "
            f"({_days_phrase(days)}).\n\n{RENEW_STEPS}")


def invalid_message() -> str:
    return ("⛔ SGW-Admin: Der GitHub-Zugang ist ungültig oder abgelaufen. Speichern in der "
            f"App ist gesperrt, bis ein neuer Token eingetragen ist.\n\n{RENEW_STEPS}")


def probe_message() -> str:
    return ("✅ SGW-Admin: Test des Token-Wächters. Die Warnungen vor dem Ablauf des "
            "GitHub-Zugangs kommen genauso hier an.")


def mask_number(number: str) -> str:
    if len(number) <= 4:
        return "…"
    return f"…{number[-4:]}"


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: bytes


Http = Callable[[str, str, dict[str, str], bytes | None], HttpResponse]


def urllib_http(method: str, url: str, headers: dict[str, str],
                body: bytes | None) -> HttpResponse:
    """Plain urllib call; HTTP errors come back as responses, network errors raise OSError."""
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return HttpResponse(response.status,
                                {k.lower(): v for k, v in response.headers.items()},
                                response.read())
    except urllib.error.HTTPError as err:
        return HttpResponse(err.code, {k.lower(): v for k, v in err.headers.items()},
                            err.read())


class GitHubUnavailable(Exception):
    """GitHub could not tell us the token's state this time."""


@dataclass(frozen=True)
class TokenStatus:
    kind: str  # "expires" | "no_expiry" | "invalid"
    expiry: datetime | None = None


def check_token(http: Http, token: str) -> TokenStatus:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "sgw-token-watch",
    }
    try:
        response = http("GET", GITHUB_RATE_LIMIT_URL, headers, None)
    except OSError as exc:
        raise GitHubUnavailable(f"GitHub nicht erreichbar ({exc.__class__.__name__})") from exc
    if response.status == 401:
        return TokenStatus("invalid")
    if response.status != 200:
        raise GitHubUnavailable(f"GitHub antwortete mit HTTP {response.status}")
    raw = response.headers.get(EXPIRY_HEADER)
    if not raw:
        return TokenStatus("no_expiry")
    try:
        return TokenStatus("expires", parse_expiry(raw))
    except ValueError as exc:
        raise GitHubUnavailable(str(exc)) from exc


def send_whatsapp(http: Http, sleep: Callable[[float], None], log: Callable[[str], None], *,
                  base_url: str, instance: str, api_key: str, number: str,
                  text: str) -> bool:
    url = f"{base_url.rstrip('/')}/message/sendText/{urllib.parse.quote(instance, safe='')}"
    # linkPreview off: Evolution would otherwise fetch every URL in the text itself
    body = json.dumps({"number": number, "text": text, "linkPreview": False}).encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8", "apikey": api_key}
    attempts = len(SEND_BACKOFF_SECONDS) + 1
    for attempt in range(attempts):
        try:
            response = http("POST", url, headers, body)
        except OSError as exc:
            log(f"WhatsApp-Versand Versuch {attempt + 1}/{attempts}: Netzfehler "
                f"({exc.__class__.__name__})")
        else:
            if 200 <= response.status < 300:
                return True
            if response.status < 500 and response.status != 429:
                log(f"WhatsApp-Versand abgelehnt (HTTP {response.status}), kein neuer Versuch")
                return False
            log(f"WhatsApp-Versand Versuch {attempt + 1}/{attempts}: HTTP {response.status}")
        if attempt < attempts - 1:
            sleep(SEND_BACKOFF_SECONDS[attempt])
    return False
