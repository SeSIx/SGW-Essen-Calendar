"""ICS generation must produce calendars real clients accept (RFC 5545)."""


import pytest
from icalendar import Calendar

import db
import ics

GAME_TIMED = {
    "id": "t1", "season": "2025", "competition": "NRW Verbandsliga - Gruppe C",
    "game_date": "2026-09-03", "game_time": "19:30",
    "home_team": "SG Wasserball Essen", "away_team": "TPSK 1925",
    "home_score": None, "away_score": None,
    "venue": 'Freibad Dellwig "Hesse"', "venue_address": "Scheppmannskamp 6, 45357 Essen",
    "status": "scheduled",
}
GAME_ALLDAY = {**GAME_TIMED, "id": "t2", "game_time": "", "away_team": "SV Blau-Weiß Bochum II"}
GAME_PLAYED = {**GAME_TIMED, "id": "t3", "status": "played", "home_score": 22, "away_score": 8}


@pytest.fixture
def written(tmp_path):
    db_path = tmp_path / "t.db"
    conn = db.init_db(str(db_path))
    for g in (GAME_TIMED, GAME_ALLDAY, GAME_PLAYED):
        db.upsert_game(conn, g)
    conn.close()
    ics_path = tmp_path / "t.ics"
    count = ics.write_ics(str(db_path), str(ics_path), calendar_name="Test")
    return count, ics_path


def test_writes_one_event_per_game(written):
    count, _ = written
    assert count == 3


def test_output_parses_as_icalendar(written):
    _, path = written
    cal = Calendar.from_ical(path.read_bytes())
    events = [c for c in cal.walk() if c.name == "VEVENT"]
    assert len(events) == 3
    assert cal.get("VERSION") == "2.0"


def test_crlf_line_endings(written):
    _, path = written
    raw = path.read_bytes()
    assert b"\r\n" in raw
    assert raw.count(b"\n") == raw.count(b"\r\n"), "RFC 5545 requires CRLF on every line"


def test_uids_are_unique(written):
    _, path = written
    cal = Calendar.from_ical(path.read_bytes())
    uids = [str(c["UID"]) for c in cal.walk() if c.name == "VEVENT"]
    assert len(uids) == len(set(uids))


def test_timed_event_carries_timezone(written):
    _, path = written
    text = path.read_text(encoding="utf-8")
    assert "DTSTART;TZID=Europe/Berlin:20260903T193000" in text
    assert "BEGIN:VTIMEZONE" in text, "TZID references must be backed by a VTIMEZONE block"


def test_all_day_event_uses_date_value(written):
    _, path = written
    text = path.read_text(encoding="utf-8")
    assert "DTSTART;VALUE=DATE:20260903" in text


def test_special_characters_are_escaped(written):
    _, path = written
    text = path.read_text(encoding="utf-8")
    assert "Scheppmannskamp 6\\, 45357 Essen" in text, "commas in LOCATION must be escaped"
    assert "Blau-Weiß" in text, "umlauts must survive as UTF-8"


def test_played_game_shows_score(written):
    _, path = written
    cal = Calendar.from_ical(path.read_bytes())
    summaries = [str(c["SUMMARY"]) for c in cal.walk() if c.name == "VEVENT"]
    assert any("22:8" in s for s in summaries)


def test_game_without_date_is_skipped(tmp_path):
    db_path = tmp_path / "t.db"
    conn = db.init_db(str(db_path))
    db.upsert_game(conn, {**GAME_TIMED, "id": "nodate", "game_date": ""})
    conn.close()
    count = ics.write_ics(str(db_path), str(tmp_path / "t.ics"))
    assert count == 0, "a game without a date cannot become a calendar entry"
