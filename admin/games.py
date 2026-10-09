"""DSV fixtures from the six team calendars in the repo: shown, never edited."""

import re
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from icalendar import Calendar

from admin.github_store import Store, StoreError

BERLIN = ZoneInfo("Europe/Berlin")
TEAMS = (
    ("herren_1", "Herren I"),
    ("herren_2", "Herren II"),
    ("damen", "Damen"),
    ("u16", "U16"),
    ("u14", "U14"),
    ("u12", "U12"),
)
TEAM_LABELS = dict(TEAMS)
DEFAULT_TEAMS = ("herren_1", "herren_2")
TTL = 300
RETRY_AFTER = 60
GAME_UID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")


@dataclass(frozen=True)
class Game:
    uid: str  # without "@sgw-essen.local"
    team: str
    summary: str
    start: datetime | date
    end: datetime | date
    location: str | None
    description: str | None

    @property
    def team_label(self) -> str:
        return TEAM_LABELS[self.team]

    @property
    def all_day(self) -> bool:
        return not isinstance(self.start, datetime)

    @property
    def start_date(self) -> date:
        return self.start.date() if isinstance(self.start, datetime) else self.start


def team_file(team: str) -> str:
    return f"sgw_essen_{team}.ics"


def _text(component, key: str) -> str | None:
    value = component.get(key)
    return str(value) if value else None


def _berlin(value: datetime) -> datetime:
    # Floating times in the feed are Berlin wall-clock time, not the server's zone.
    if value.tzinfo is None:
        return value.replace(tzinfo=BERLIN)
    return value.astimezone(BERLIN)


def _vevent(comp, team: str) -> Game:
    """One VEVENT; raises on anything that makes the game unusable."""
    if not comp.get("UID"):
        raise ValueError("VEVENT ohne UID")
    if comp.get("DTSTART") is None:
        raise ValueError("VEVENT ohne DTSTART")
    start = comp.decoded("DTSTART")
    end = comp.decoded("DTEND") if comp.get("DTEND") else start
    if isinstance(start, datetime) != isinstance(end, datetime):
        raise ValueError("DTSTART und DTEND haben verschiedene Typen")
    if isinstance(start, datetime):
        start, end = _berlin(start), _berlin(end)
    return Game(
        uid=str(comp.get("UID")).removesuffix("@sgw-essen.local"),
        team=team, summary=str(comp.get("SUMMARY", "")), start=start, end=end,
        location=_text(comp, "LOCATION"), description=_text(comp, "DESCRIPTION"))


def parse_ics(text: str, team: str) -> list[Game]:
    """Games of one team file. A broken VEVENT is skipped, the others are kept."""
    found = []
    for comp in Calendar.from_ical(text).walk("VEVENT"):
        try:
            found.append(_vevent(comp, team))
        except (KeyError, ValueError, TypeError, AttributeError):
            continue
    return found


def parse_teams(value: str | None) -> tuple[str, ...] | None:
    """Known team slugs from a comma list, in canonical order; unknown ones are ignored."""
    if value is None:
        return None
    wanted = {part.strip() for part in value.split(",")}
    return tuple(slug for slug, _ in TEAMS if slug in wanted)


NO_TEAMS = "keine"


def parse_selection(value: str | None) -> tuple[str, ...] | None:
    """A stored or requested filter: None = not given or unusable, () = no team on purpose."""
    if value is None:
        return None
    if value.strip() in ("", NO_TEAMS):
        return ()
    return parse_teams(value) or None


class GameCache:
    def __init__(self, store: Store, clock: Callable[[], float] = time.monotonic, ttl: int = TTL):
        self._store = store
        self._clock = clock
        self._ttl = ttl
        # team -> (valid until, games, whether the games are fresh)
        self._cache: dict[str, tuple[float, list[Game], bool]] = {}
        # gunicorn runs threads: guard the dict, never the slow GitHub fetch.
        self._lock = threading.Lock()

    def games(self, teams: Iterable[str]) -> tuple[list[Game], bool]:
        out: list[Game] = []
        stale = False
        for team in teams:
            with self._lock:
                cached = self._cache.get(team)
            if cached is not None and self._clock() < cached[0]:
                out.extend(cached[1])
                stale |= not cached[2]
                continue
            try:
                fresh = parse_ics(self._store.read_text(team_file(team)), team)
            except (StoreError, ValueError, KeyError, AttributeError, TypeError):
                # Outage or unreadable calendar: serve the last good copy and do not
                # retry for RETRY_AFTER seconds, so a down GitHub is not hit per request.
                kept = cached[1] if cached else []
                with self._lock:
                    self._cache[team] = (self._clock() + RETRY_AFTER, kept, False)
                stale = True
                out.extend(kept)
                continue
            with self._lock:
                self._cache[team] = (self._clock() + self._ttl, fresh, True)
            out.extend(fresh)
        return out, stale

    def find(self, uid: str) -> Game | None:
        found, _ = self.games(slug for slug, _ in TEAMS)
        return next((g for g in found if g.uid == uid), None)
