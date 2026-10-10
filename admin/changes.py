"""What a create, update or delete does to the event list, and how it reaches GitHub.

Conflicts are judged per event: if someone else changed a *different* event in
the meantime, saving still works. Only a change to the same event (or its
deletion) stops the save.
"""

import re
from collections.abc import Callable, Mapping

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


def _same_event(a: dict, b: dict) -> bool:
    return all(a.get(k) == b.get(k) for k in custom_events.FIELDS)


def create(snapshot: Snapshot, event: dict) -> list | None:
    existing = find(snapshot, event["id"])
    if existing is not None:
        if existing == event:
            return None  # an earlier attempt already saved it
        raise DuplicateId(event["id"])
    return entries(snapshot) + [event]


def update(snapshot: Snapshot, event_id: str, rev: str, event: dict) -> list:
    current = find(snapshot, event_id)
    if isinstance(current, dict) and _same_event(current, event):
        return None  # a re-submitted edit: already stored, nothing to write
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
           message: str | Callable[[], str], author: Author) -> None:
    """Load, apply, save; if GitHub reports a concurrent write, try exactly once more.

    A callable message is evaluated after `mutate` ran, so it can use what mutate saw.
    """
    for attempt in range(2):
        snapshot = store.load()
        new_entries = mutate(snapshot)
        if new_entries is None:
            return
        try:
            store.save(new_entries, snapshot.sha, message() if callable(message) else message, author)
            return
        except Conflict:
            if attempt == 1:
                raise SaveConflict() from None


def message(display_name: str, title: str, verb: str) -> str:
    return f"{display_name}: „{' '.join(title.split())}“ {verb}"


def author_for(login: str, display_name: str,
               mapping: Mapping[str, tuple[str, str]] | None = None) -> Author:
    """The mapped identity if there is one. Otherwise a name no GitHub login can equal.

    GitHub Mobile matches the author *name* to an account, so a bare "Julius" would show
    github.com/julius. The suffix adds a space and parentheses, which logins cannot contain.
    """
    if mapping and login in mapping:
        return Author(*mapping[login])
    return Author(f"{display_name}{AUTHOR_SUFFIX}", f"admin+{login}@sgw-essen.local")


AUTHOR_SUFFIX = " (SGW-Admin)"


def shown_author(raw: str, mapping: Mapping[str, tuple[str, str]],
                 display_name_of: Callable[[str], str | None]) -> str:
    """The app display name for a stored commit author name; foreign authors stay as they are.

    The store only reports the name, so a mapped identity is recognised by its mapped name.
    """
    for login, (name, _email) in mapping.items():
        if raw == name:
            return display_name_of(login) or raw
    return raw.removesuffix(AUTHOR_SUFFIX)
