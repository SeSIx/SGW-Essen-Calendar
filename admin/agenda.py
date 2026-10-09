"""Club dates and fixtures, merged into the month groups of the start screen."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from admin.changes import EVENT_ID_RE
from admin.games import Game

WEEKDAYS = ("MO", "DI", "MI", "DO", "FR", "SA", "SO")
MONTHS = ("Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August",
          "September", "Oktober", "November", "Dezember")


@dataclass(frozen=True)
class Item:
    kind: str  # "event" or "game"
    key: str  # event id or game uid, used for the link
    title: str
    day_label: str  # "SA" or "FR–SO"
    date_label: str  # "21" or "27–29"
    detail: str
    start: date
    end: date
    sort_time: str
    team: str | None = None  # game items only


@dataclass(frozen=True)
class Month:
    label: str
    items: list[Item]

    def visible(self, teams: Iterable[str]) -> bool:
        """Whether any item shows for this team selection (club dates always do)."""
        chosen = set(teams)
        return any(i.team is None or i.team in chosen for i in self.items)


@dataclass(frozen=True)
class Broken:
    key: str | None  # the id, if the entry can be opened in the form
    title: str
    problem: str


def _labels(start: date, end: date) -> tuple[str, str]:
    if end == start:
        return WEEKDAYS[start.weekday()], str(start.day)
    days = f"{WEEKDAYS[start.weekday()]}–{WEEKDAYS[end.weekday()]}"
    if (end.year, end.month) == (start.year, start.month):
        return days, f"{start.day}–{end.day}"
    return days, f"{start.day}.{start.month}.–{end.day}.{end.month}."


def event_item(event: dict) -> Item:
    start = date.fromisoformat(event["start_date"])
    end = date.fromisoformat(event["end_date"]) if event["end_date"] else start
    span = f"{(end - start).days + 1} Tage" if end != start else None
    if event["start_time"] is None:
        times = "ganztägig"
    else:
        times = event["start_time"] + (f" – {event['end_time']}" if event["end_time"] else "")
    day_label, date_label = _labels(start, end)
    return Item("event", event["id"], event["title"], day_label, date_label,
                " · ".join(p for p in (span, times, event["location"]) if p),
                start, end, event["start_time"] or "")


def game_item(game: Game) -> Item:
    day = game.start_date
    clock = "" if game.all_day else f"{game.start:%H:%M}"
    return Item("game", game.uid, game.summary, WEEKDAYS[day.weekday()], str(day.day),
                f"{clock or 'ganztägig'} · {game.team_label} · 🔒 DSV", day, day, clock, game.team)


def build(events: Iterable[dict], games: Iterable[Game], today: date, past: bool = False) -> list[Month]:
    items = [event_item(e) for e in events] + [game_item(g) for g in games]
    if past:
        chosen = sorted((i for i in items if i.end < today),
                        key=lambda i: (i.start, i.sort_time), reverse=True)
    else:
        chosen = sorted((i for i in items if i.end >= today),
                        key=lambda i: (i.start, i.sort_time, i.kind != "event"))
    months: list[Month] = []
    for item in chosen:
        label = f"{MONTHS[item.start.month - 1]} {item.start.year}"
        if not months or months[-1].label != label:
            months.append(Month(label, []))
        months[-1].items.append(item)
    return months


def broken_items(invalid) -> list[Broken]:
    out = []
    for entry in invalid:
        raw = entry.raw if isinstance(entry.raw, dict) else {}
        key = raw.get("id")
        title = raw.get("title")
        out.append(Broken(
            key if isinstance(key, str) and EVENT_ID_RE.fullmatch(key) else None,
            title.strip() if isinstance(title, str) and title.strip() else "(ohne Titel)",
            entry.error.message))
    return out
