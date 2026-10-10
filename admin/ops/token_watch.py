#!/usr/bin/env python3
"""Warn Julius over WhatsApp before the SGW-Admin GitHub token expires.

Runs daily on the host from sgw-token-watch.timer, not in the app container,
so the internet-facing app never holds the Evolution key. Standard library
only: the host's python3 runs it without a virtualenv. Secrets are read from
the files that already hold them and are never copied or logged.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
STAGES = (14, 7, 3, 1, 0)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2

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


def _read_key_values(path: Path) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"{path} nicht lesbar ({exc.__class__.__name__})") from exc
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
    try:
        text = compose_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"{compose_path} nicht lesbar ({exc.__class__.__name__})") from exc
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        match = _COMPOSE_KEY.match(line)
        if not match:
            continue
        if match.group(1).startswith("$"):
            raise ConfigError(
                f"AUTHENTICATION_API_KEY in {compose_path} ist eine Variablen-Referenz")
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
    return (f"⚠️ SGW-Admin: Der GitHub-Zugang läuft am {expiry_day:%d.%m.%Y} ab "
            f"({_days_phrase(days)}).\n\n{RENEW_STEPS}")


def invalid_message() -> str:
    return ("⛔ SGW-Admin: Der GitHub-Zugang ist ungültig oder abgelaufen. Speichern in der "
            f"App ist gesperrt, bis ein neuer Token eingetragen ist.\n\n{RENEW_STEPS}")


def test_message() -> str:
    return ("✅ SGW-Admin: Test des Token-Wächters. Die Warnungen vor dem Ablauf des "
            "GitHub-Zugangs kommen genauso hier an.")


def mask_number(number: str) -> str:
    return f"…{number[-4:]}"
