"""custom_events.py — the club-date model shared by combine.py and the admin app.

custom_events.json is edited by people, directly or through the admin app, so
everything that reads it goes through validate(): one bad entry must never take
the published calendars down with it, and a clean file must survive a
read/write cycle byte for byte.
"""

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime

FIELDS = ("id", "title", "start_date", "start_time", "end_date", "end_time",
          "location", "description")

MAX_TITLE = 120
MAX_LOCATION = 200
MAX_DESCRIPTION = 2000

_ID = re.compile(r"[A-Za-z0-9-]{1,64}")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TIME = re.compile(r"\d{2}:\d{2}")
_LINE_BREAKS = re.compile(r"\r\n|[\r\n\t]")
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_CONTROL_EXCEPT_NEWLINE = re.compile(r"[\x00-\x09\x0b-\x1f\x7f-\x9f]")


class ValidationError(ValueError):
    """`message` is German because the admin app shows it next to the field."""

    def __init__(self, field: str, message: str):
        super().__init__(f"{field}: {message}")
        self.field = field
        self.message = message


@dataclass(frozen=True)
class InvalidEntry:
    raw: object
    error: ValidationError


@dataclass(frozen=True)
class ParseResult:
    valid: list[dict]
    invalid: list[InvalidEntry]


def _text(raw: dict, field: str, max_len: int, *, multiline: bool = False,
          required: bool = False) -> str | None:
    value = raw.get(field)
    if value is None:
        value = ""
    if not isinstance(value, str):
        raise ValidationError(field, "muss Text sein")
    if multiline:
        value = value.replace("\r\n", "\n").replace("\t", " ")
        value = _CONTROL_EXCEPT_NEWLINE.sub("", value)
    else:
        value = _CONTROL.sub("", _LINE_BREAKS.sub(" ", value))
    value = value.strip()
    if not value:
        if required:
            raise ValidationError(field, "darf nicht leer sein")
        return None
    if len(value) > max_len:
        raise ValidationError(field, f"darf höchstens {max_len} Zeichen lang sein")
    return value


def _date(raw: dict, field: str, *, required: bool) -> date | None:
    value = raw.get(field)
    if value is None or value == "":
        if required:
            raise ValidationError(field, "fehlt")
        return None
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        raise ValidationError(field, "muss ein Datum im Format JJJJ-MM-TT sein")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValidationError(field, "ist kein gültiges Datum") from None


def _time(raw: dict, field: str) -> str | None:
    value = raw.get(field)
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not _TIME.fullmatch(value):
        raise ValidationError(field, "muss eine Uhrzeit im Format HH:MM sein")
    try:
        datetime.strptime(value, "%H:%M")
    except ValueError:
        raise ValidationError(field, "ist keine gültige Uhrzeit") from None
    return value


def validate(raw: object) -> dict:
    """Return a cleaned copy with exactly FIELDS, or raise ValidationError."""
    if not isinstance(raw, dict):
        raise ValidationError("id", "Eintrag ist kein Objekt")
    event_id = raw.get("id")
    if not isinstance(event_id, str) or not _ID.fullmatch(event_id):
        raise ValidationError("id", "fehlt oder ist ungültig")

    title = _text(raw, "title", MAX_TITLE, required=True)
    start = _date(raw, "start_date", required=True)
    end = _date(raw, "end_date", required=False)
    start_time = _time(raw, "start_time")
    end_time = _time(raw, "end_time")

    if end is not None and end < start:
        raise ValidationError("end_date", "Ende liegt vor Beginn")
    if end == start:
        end = None
    if start_time is None and end_time is not None:
        raise ValidationError("end_time", "Ganztägige Termine haben keine Endzeit")
    if end is None and start_time and end_time and end_time <= start_time:
        raise ValidationError("end_time", "Ende liegt vor Beginn")

    return {
        "id": event_id,
        "title": title,
        "start_date": start.isoformat(),
        "start_time": start_time,
        "end_date": end.isoformat() if end else None,
        "end_time": end_time,
        "location": _text(raw, "location", MAX_LOCATION),
        "description": _text(raw, "description", MAX_DESCRIPTION, multiline=True),
    }


def parse(text: str) -> ParseResult:
    """Split the file into usable and broken entries; ValueError if it is no JSON list."""
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError("custom_events.json must contain a JSON list")
    valid: list[dict] = []
    invalid: list[InvalidEntry] = []
    seen: set[str] = set()
    for raw in data:
        try:
            event = validate(raw)
            if event["id"] in seen:
                raise ValidationError("id", "kommt doppelt vor")
        except ValidationError as err:
            invalid.append(InvalidEntry(raw, err))
            continue
        seen.add(event["id"])
        valid.append(event)
    return ParseResult(valid, invalid)


def _sort_key(entry: object) -> tuple[str, str]:
    if not isinstance(entry, dict):
        return ("", "")
    start_date, start_time = entry.get("start_date"), entry.get("start_time")
    return (start_date if isinstance(start_date, str) else "",
            start_time if isinstance(start_time, str) else "")


def serialize(entries: list) -> str:
    """The one canonical file layout; broken entries are kept so nothing is lost."""
    return json.dumps(sorted(entries, key=_sort_key), indent=2, ensure_ascii=False) + "\n"


def event_rev(event: dict) -> str:
    """Fingerprint for optimistic locking of a single event."""
    canonical = json.dumps({k: event.get(k) for k in FIELDS}, sort_keys=True,
                           ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def new_id() -> str:
    return str(uuid.uuid4())
