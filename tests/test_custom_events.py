"""custom_events.json is edited by people; one bad entry must never reach the
published calendars, and a clean file must survive a read/write untouched."""

import json
import re
from pathlib import Path

import pytest

import custom_events as ce

SNAPSHOT = Path(__file__).parent / "fixtures" / "custom_events_snapshot.json"


def _raw(**overrides):
    base = {
        "id": "e7548548-4818-4bda-ac36-20f8bf0fc1e9", "title": "Mannschaftsbesprechung",
        "start_date": "2026-09-03", "start_time": "19:30", "end_date": None,
        "end_time": "21:30", "location": "Freibad Dellwig", "description": "Saisonstart",
    }
    base.update(overrides)
    return base


def _field_of(raw):
    with pytest.raises(ce.ValidationError) as exc:
        ce.validate(raw)
    return exc.value.field


def test_snapshot_round_trips_byte_for_byte():
    text = SNAPSHOT.read_text(encoding="utf-8")
    result = ce.parse(text)
    assert result.invalid == []
    assert ce.serialize(result.valid) == text


def test_valid_event_is_returned_normalized_with_fixed_key_order():
    event = ce.validate(_raw(title="  Besprechung  ", location="", extra="dropped"))
    assert list(event) == list(ce.FIELDS)
    assert event["title"] == "Besprechung"
    assert event["location"] is None


def test_all_day_multi_day_event_is_valid():
    event = ce.validate(_raw(start_time=None, end_time=None, end_date="2026-09-05"))
    assert event["end_date"] == "2026-09-05"


def test_end_date_equal_to_start_is_normalized_away():
    assert ce.validate(_raw(end_date="2026-09-03"))["end_date"] is None


@pytest.mark.parametrize("overrides, field", [
    ({"id": ""}, "id"),
    ({"id": "has space"}, "id"),
    ({"id": "x" * 65}, "id"),
    ({"title": "   "}, "title"),
    ({"title": "x" * 121}, "title"),
    ({"location": "x" * 201}, "location"),
    ({"description": "x" * 2001}, "description"),
    ({"start_date": None}, "start_date"),
    ({"start_date": "2026-02-30"}, "start_date"),
    ({"start_date": "03.09.2026"}, "start_date"),
    ({"start_time": "24:00"}, "start_time"),
    ({"start_time": "9:30"}, "start_time"),
    ({"end_date": "2026-09-02"}, "end_date"),
    ({"start_time": None, "end_time": "21:30"}, "end_time"),
    ({"end_time": "19:30"}, "end_time"),
    ({"end_time": "18:00"}, "end_time"),
])
def test_invalid_values_name_the_field(overrides, field):
    assert _field_of(_raw(**overrides)) == field


def test_messages_are_german_for_the_admin_app():
    with pytest.raises(ce.ValidationError) as exc:
        ce.validate(_raw(end_date="2026-09-02"))
    assert exc.value.message == "Ende liegt vor Beginn"


def test_non_string_values_are_invalid():
    assert _field_of(_raw(start_time=1900)) == "start_time"
    assert _field_of(_raw(title=None)) == "title"
    assert _field_of(_raw(location=["a"])) == "location"


def test_non_object_entry_is_invalid():
    assert _field_of("not an event") == "id"


def test_control_characters_are_cleaned():
    event = ce.validate(_raw(title="Feier\r\nam\tAbend\x07",
                             description="Zeile 1\r\nZeile 2\x00\tEnde"))
    assert event["title"] == "Feier am Abend"
    assert event["description"] == "Zeile 1\nZeile 2 Ende"


def test_parse_separates_valid_and_invalid_entries():
    text = json.dumps([_raw(), _raw(id="bad", start_date="2026-13-01")])
    result = ce.parse(text)
    assert [e["id"] for e in result.valid] == ["e7548548-4818-4bda-ac36-20f8bf0fc1e9"]
    assert result.invalid[0].raw["id"] == "bad"
    assert result.invalid[0].error.field == "start_date"


def test_duplicate_id_is_invalid():
    result = ce.parse(json.dumps([_raw(), _raw(title="Kopie")]))
    assert len(result.valid) == 1
    assert result.invalid[0].error.field == "id"
    assert result.invalid[0].raw["title"] == "Kopie"


@pytest.mark.parametrize("text", ["{not json", '{"a": 1}', "null"])
def test_parse_rejects_files_that_are_not_a_list(text):
    with pytest.raises(ValueError):
        ce.parse(text)


def test_serialize_keeps_invalid_entries_and_sorts_robustly():
    text = ce.serialize([_raw(start_date="2026-10-01"), "garbage",
                         {"id": "x", "start_date": 5}, _raw(start_date="2026-09-01")])
    data = json.loads(text)
    assert data[0] == "garbage"
    assert data[1] == {"id": "x", "start_date": 5}
    assert [d["start_date"] for d in data[2:]] == ["2026-09-01", "2026-10-01"]
    assert text.endswith("\n")


def test_event_rev_ignores_unknown_keys_but_sees_every_field():
    event = ce.validate(_raw())
    assert ce.event_rev(event) == ce.event_rev({**event, "extra": 1})
    assert ce.event_rev(event) != ce.event_rev({**event, "location": "Anderswo"})
    assert re.fullmatch(r"[0-9a-f]{64}", ce.event_rev(event))


def test_new_id_is_a_valid_id():
    assert ce.validate(_raw(id=ce.new_id()))
