"""What a create, update or delete does to the event list, and how it reaches GitHub.

Conflicts are judged per event: if someone else changed a *different* event in
the meantime, saving still works. Only a change to the same event (or its
deletion) stops the save.
"""

import re
from collections.abc import Callable

import custom_events
from admin.github_store import Author, Conflict, Snapshot, Store

EVENT_ID_RE = re.compile(r"[A-Za-z0-9-]{1,64}")


class EventConflict(Exception):
    def __init__(self, current: object | None):
        super().__init__("event changed in the meantime")
        self.current = current  # the stored version, None if it was deleted


class SaveConflict(Exception):
    """The file kept changing under us; two attempts failed."""


class DuplicateId(Exception):
    """A new event reused an id that already holds different content."""


def entries(snapshot: Snapshot) -> list:
    """Everything in the file: valid events first, broken entries kept as found."""
    return list(snapshot.result.valid) + [i.raw for i in snapshot.result.invalid]


def _id_of(entry: object) -> str | None:
    if isinstance(entry, dict) and isinstance(entry.get("id"), str):
        return entry["id"]
    return None


def find(snapshot: Snapshot, event_id: str) -> object | None:
    return next((e for e in entries(snapshot) if _id_of(e) == event_id), None)


def _rev(entry: object) -> str:
    return custom_events.event_rev(entry) if isinstance(entry, dict) else ""


def create(snapshot: Snapshot, event: dict) -> list | None:
    existing = find(snapshot, event["id"])
    if existing is not None:
        if existing == event:
            return None  # an earlier attempt already saved it
        raise DuplicateId(event["id"])
    return entries(snapshot) + [event]


def update(snapshot: Snapshot, event_id: str, rev: str, event: dict) -> list:
    current = find(snapshot, event_id)
    if current is None and rev == "":
        return entries(snapshot) + [event]  # "keep my version" of a deleted event
    if current is None or _rev(current) != rev:
        raise EventConflict(current)
    return [event if _id_of(e) == event_id else e for e in entries(snapshot)]


def delete(snapshot: Snapshot, event_id: str, rev: str) -> list | None:
    current = find(snapshot, event_id)
    if current is None:
        return None  # already gone, e.g. a second tap
    if _rev(current) != rev:
        raise EventConflict(current)
    return [e for e in entries(snapshot) if _id_of(e) != event_id]


def commit(store: Store, mutate: Callable[[Snapshot], list | None],
           message: str, author: Author) -> None:
    """Load, apply, save; if GitHub reports a concurrent write, try exactly once more."""
    for attempt in range(2):
        snapshot = store.load()
        new_entries = mutate(snapshot)
        if new_entries is None:
            return
        try:
            store.save(new_entries, snapshot.sha, message, author)
            return
        except Conflict:
            if attempt == 1:
                raise SaveConflict() from None


def message(display_name: str, title: str, verb: str) -> str:
    return f"{display_name}: „{' '.join(title.split())}“ {verb}"


def author_for(login: str, display_name: str) -> Author:
    return Author(display_name, f"admin+{login}@sgw-essen.local")
