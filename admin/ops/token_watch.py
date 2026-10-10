#!/usr/bin/env python3
"""Warn Julius over WhatsApp before the SGW-Admin GitHub token expires.

Runs daily on the host from sgw-token-watch.timer, not in the app container,
so the internet-facing app never holds the Evolution key. Standard library
only: the host's python3 runs it without a virtualenv. Secrets are read from
the files that already hold them and are never copied or logged.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from http.client import HTTPException
from pathlib import Path
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
STAGES = (14, 7, 3, 1, 0)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2
EXIT_SENT_UNSAVED = 3  # warning sent but state not saved: systemd must not retry (would resend)
MODES = ("send", "dry-run", "status", "test-message")

GITHUB_RATE_LIMIT_URL = "https://api.github.com/rate_limit"
EXPIRY_HEADER = "github-authentication-token-expiration"
SEND_BACKOFF_SECONDS = (2, 4)
TIMEOUT_SECONDS = 10
CONFIG_PATH = Path("/etc/sgw-token-watch.conf")

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
        values[key] = _parse_value(value)
    return values


def _parse_value(raw: str) -> str:
    """Like dotenv/compose: quotes protect a '#', unquoted ' #…' is a comment."""
    value = raw.strip()
    if value[:1] in ("'", '"'):
        end = value.find(value[0], 1)
        if end != -1 and re.fullmatch(r"(\s+#.*)?", value[end + 1:]):
            return value[1:end]
    return re.sub(r"\s+#.*$", "", value).strip()


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


def last_valid_day(expiry: datetime) -> date:
    """Berlin day of the last valid moment: a 00:00 expiry ends the day before."""
    return (expiry - timedelta(seconds=1)).astimezone(BERLIN).date()


def days_left(expiry: datetime, today: date) -> int:
    return (last_valid_day(expiry) - today).days


def due_stage(days: int) -> int | None:
    reached = [stage for stage in STAGES if days <= stage]
    return min(reached) if reached else None


def _tage(count: int) -> str:
    return "1 Tag" if count == 1 else f"{count} Tage"


def _days_phrase(days: int) -> str:
    if days == 0:
        return "heute"
    if days == 1:
        return "morgen"
    return f"in {days} Tagen"


def expiry_message(expiry: datetime, days: int) -> str:
    local = expiry.astimezone(BERLIN)
    when = f"{local:%d.%m.%Y} um {local:%H:%M} Uhr"
    if days < 0:
        count = -days
        ago = "1 Tag" if count == 1 else f"{count} Tagen"
        return (f"⚠️ SGW-Admin: Der GitHub-Zugang ist am {when} abgelaufen "
                f"(vor {ago}).\n\n{RENEW_STEPS}")
    if days == 0 and (local.hour, local.minute) == (0, 0):
        return (f"⚠️ SGW-Admin: Der GitHub-Zugang läuft heute Nacht um 00:00 Uhr ab "
                f"(am {local:%d.%m.%Y}).\n\n{RENEW_STEPS}")
    return (f"⚠️ SGW-Admin: Der GitHub-Zugang läuft am {when} ab "
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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: the token and key must not follow a 3xx to another host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ARG002 (signature)
        return None


def _opener() -> urllib.request.OpenerDirector:
    # ProxyHandler({}): ignore http_proxy & co.; Evolution is on loopback, GitHub is direct
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect)


def urllib_http(method: str, url: str, headers: dict[str, str],
                body: bytes | None) -> HttpResponse:
    """Plain urllib call; HTTP errors (3xx included) come back as responses.

    Network errors and malformed replies raise OSError (ConnectionError for the latter).
    """
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with _opener().open(request, timeout=TIMEOUT_SECONDS) as response:
            return HttpResponse(response.status,
                                {k.lower(): v for k, v in response.headers.items()},
                                response.read())
    except urllib.error.HTTPError as err:
        return HttpResponse(err.code, {k.lower(): v for k, v in err.headers.items()},
                            err.read())
    except HTTPException as exc:
        raise ConnectionError(type(exc).__name__) from exc


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
        # Not str(exc): it echoes the raw header, which must never reach a log or message
        raise GitHubUnavailable("Ablaufdatum des GitHub-Tokens nicht lesbar") from exc


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


def load_state(path: Path, log: Callable[[str], None]) -> dict:
    """Other OSErrors propagate: guessing "nothing sent" could resend a warning."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"sent": []}
    except ValueError as exc:
        log(f"Statusdatei {path} unlesbar ({exc.__class__.__name__}) – starte mit leerem Status")
        return {"sent": []}
    sent = data.get("sent") if isinstance(data, dict) else None
    if not isinstance(sent, list) or not all(isinstance(key, str) for key in sent):
        log(f"Statusdatei {path} hat ein unerwartetes Format – starte mit leerem Status")
        return {"sent": []}
    return {"sent": sent}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(state, indent=2, sort_keys=True) + "\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def _status_report(log: Callable[[str], None], expiry_day: date, end_day: date, days: int,
                   sent: list[str]) -> None:
    upcoming = [stage for stage in STAGES if stage < days]
    if upcoming:
        nxt = max(upcoming)
        log(f"Nächste Warnstufe: {nxt} Tage vorher, am "
            f"{end_day - timedelta(days=nxt):%d.%m.%Y}")
    prefix = f"{expiry_day.isoformat()}:"
    stage = due_stage(days)
    if stage is not None and f"{prefix}{stage}" not in sent:
        log(f"Fällig: Stufe {stage} (noch nicht gesendet)")
    done = sorted((key for key in sent if key.startswith(prefix)),
                  key=lambda key: int(key.rsplit(":", 1)[1]) if key.rsplit(":", 1)[1].isdigit()
                  else -1)
    log(f"Bereits gesendet: {', '.join(done) if done else 'nichts'}")


def run(cfg: Config, *, http: Http, now: datetime, sleep: Callable[[float], None],
        log: Callable[[str], None], mode: str) -> int:
    if mode not in MODES:
        raise ValueError(f"unbekannter Modus: {mode!r}")
    today = now.astimezone(BERLIN).date()
    try:
        api_key = read_evolution_key(cfg.evolution_compose)
        token = (None if mode == "test-message"
                 else read_env_value(cfg.github_env_file, "GITHUB_TOKEN"))
    except ConfigError as exc:
        log(f"Konfigurationsfehler: {exc}")
        return EXIT_CONFIG

    def deliver(text: str) -> bool:
        return send_whatsapp(http, sleep, log, base_url=cfg.evolution_url,
                             instance=cfg.evolution_instance, api_key=api_key,
                             number=cfg.recipient, text=text)

    if mode == "test-message":
        if deliver(probe_message()):
            log(f"Testnachricht an {mask_number(cfg.recipient)} gesendet")
            return EXIT_OK
        log("Testnachricht konnte nicht gesendet werden")
        return EXIT_FAILED

    try:
        status = check_token(http, token)
    except GitHubUnavailable as exc:
        log(f"{exc} – nächster Lauf versucht es erneut")
        return EXIT_FAILED

    try:
        sent = load_state(cfg.state_file, log)["sent"]
    except OSError as exc:
        log(f"Statusdatei {cfg.state_file} nicht lesbar ({exc.__class__.__name__}) – "
            "nichts gesendet, nächster Lauf versucht es erneut")
        return EXIT_FAILED

    if status.kind == "no_expiry":
        log("Token hat kein Ablaufdatum – keine Warnung nötig")
        return EXIT_OK

    if status.kind == "invalid":
        key = f"invalid:{today.isoformat()}"
        log("GitHub meldet 401: Token ungültig oder abgelaufen")
        if mode == "status":
            log(f"Heutige Meldung: {'bereits gesendet' if key in sent else 'noch offen'}")
            return EXIT_OK
        if key in sent:
            log("Token ungültig – heute bereits gemeldet")
            return EXIT_OK
        text = invalid_message()
        new_sent = [k for k in sent if not k.startswith("invalid:")] + [key]
    else:
        local = status.expiry.astimezone(BERLIN)
        expiry_day = local.date()
        end_day = last_valid_day(status.expiry)
        days = (end_day - today).days
        log(f"Token läuft am {local:%d.%m.%Y} um {local:%H:%M} Uhr ab (noch {_tage(days)})")
        if mode == "status":
            _status_report(log, expiry_day, end_day, days, sent)
            return EXIT_OK
        stage = due_stage(days)
        prefix = f"{expiry_day.isoformat()}:"
        if stage is None or f"{prefix}{stage}" in sent:
            log("Keine Warnung fällig")
            return EXIT_OK
        key = f"{prefix}{stage}"
        text = expiry_message(status.expiry, days)
        # Catching up on stage N also settles every larger stage for this expiry date;
        # keys of earlier expiry dates are dropped, so a renewed token starts afresh.
        new_sent = sorted({k for k in sent if k.startswith(prefix)}
                          | {f"{prefix}{s}" for s in STAGES if s >= stage})

    if mode == "dry-run":
        log("Trockenlauf – diese Nachricht würde gesendet:")
        log(text)
        return EXIT_OK
    if not deliver(text):
        log("Warnung konnte nicht gesendet werden – nächster Lauf versucht es erneut")
        return EXIT_FAILED
    try:
        save_state(cfg.state_file, {"sent": new_sent})
    except OSError as exc:
        log(f"Warnung an {mask_number(cfg.recipient)} gesendet ({key}), aber Statusdatei "
            f"nicht gespeichert ({exc.__class__.__name__}) – "
            "Nachricht kann beim nächsten Lauf erneut kommen")
        return EXIT_SENT_UNSAVED
    log(f"Warnung an {mask_number(cfg.recipient)} gesendet ({key})")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Warnt per WhatsApp, bevor der GitHub-Token der SGW-Admin-App abläuft.")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--status", action="store_true",
                       help="nur Ablaufdatum und Warnstufen anzeigen")
    modes.add_argument("--dry-run", action="store_true",
                       help="fällige Nachricht anzeigen, nichts senden oder speichern")
    modes.add_argument("--test-message", action="store_true",
                       help="eine Testnachricht senden (ohne GitHub-Prüfung)")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    args = parser.parse_args(argv)
    mode = ("status" if args.status else "dry-run" if args.dry_run
            else "test-message" if args.test_message else "send")

    def log(message: str) -> None:
        print(message, flush=True)

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        log(f"Konfigurationsfehler: {exc}")
        return EXIT_CONFIG
    return run(cfg, http=urllib_http, now=datetime.now(tz=BERLIN), sleep=time.sleep,
               log=log, mode=mode)


if __name__ == "__main__":
    sys.exit(main())
