"""Between the HTML form and an event dict.

The two switches decide which fields count, so a person who leaves the time
empty with „Ganztägig“ off gets told, instead of silently getting an all-day event.
"""

from dataclasses import dataclass

import custom_events

FORM_FIELDS = frozenset({"title", "start_date", "start_time", "end_date", "end_time",
                         "location", "description"})


@dataclass
class FormData:
    title: str = ""
    start_date: str = ""
    start_time: str = ""
    end_date: str = ""
    end_time: str = ""
    location: str = ""
    description: str = ""
    all_day: bool = False
    multi_day: bool = False


def _s(value: object) -> str:
    return value if isinstance(value, str) else ""


def from_event(entry: object) -> FormData:
    e = entry if isinstance(entry, dict) else {}
    end_date = _s(e.get("end_date"))
    return FormData(
        title=_s(e.get("title")), start_date=_s(e.get("start_date")),
        start_time=_s(e.get("start_time")), end_date=end_date, end_time=_s(e.get("end_time")),
        location=_s(e.get("location")), description=_s(e.get("description")),
        all_day=bool(e) and e.get("start_time") is None,
        multi_day=bool(end_date) and end_date != e.get("start_date"))


def from_request(form) -> FormData:
    # The caps sit well above the model's limits: they only bound the work, over-long input still fails validation.
    def text(name: str, limit: int) -> str:
        return form.get(name, "")[:limit]

    return FormData(
        title=text("title", 400), start_date=text("start_date", 10), start_time=text("start_time", 5),
        end_date=text("end_date", 10), end_time=text("end_time", 5), location=text("location", 400),
        description=text("description", 4000),
        all_day=form.get("all_day") == "1", multi_day=form.get("multi_day") == "1")


def to_event(data: FormData, event_id: str) -> dict:
    if not data.all_day and not data.start_time:
        raise custom_events.ValidationError("start_time", "Uhrzeit fehlt – oder „Ganztägig“ wählen")
    if not data.multi_day and data.end_date and data.end_date != data.start_date:
        raise custom_events.ValidationError("end_date", "Mehrtägig einschalten oder Ende leeren")
    if data.all_day and (data.start_time or data.end_time):
        field = "start_time" if data.start_time else "end_time"
        raise custom_events.ValidationError(field, "Ganztägig ausschalten oder Uhrzeiten leeren")
    if data.multi_day and not data.end_date:
        raise custom_events.ValidationError("end_date", "Enddatum fehlt – oder „Mehrtägig“ ausschalten")
    return custom_events.validate({
        "id": event_id,
        "title": data.title,
        "start_date": data.start_date,
        "start_time": None if data.all_day else data.start_time,
        "end_date": (data.end_date or None) if data.multi_day else None,
        "end_time": None if data.all_day else (data.end_time or None),
        "location": data.location or None,
        "description": data.description or None,
    })
