"""ICS generation must produce calendars real clients accept (RFC 5545)."""

import sqlite3

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


def test_output_is_byte_stable_when_nothing_changed(tmp_path):
    """The scheduled job commits whatever changed. If DTSTAMP tracked wall-clock
    time, every run would produce a diff and commit noise twice a day forever."""
    db_path = tmp_path / "t.db"
    conn = db.init_db(str(db_path))
    db.upsert_game(conn, GAME_TIMED)
    conn.close()

    first = tmp_path / "a.ics"
    second = tmp_path / "b.ics"
    ics.write_ics(str(db_path), str(first))
    ics.write_ics(str(db_path), str(second))
    assert first.read_bytes() == second.read_bytes()


def test_dtstamp_moves_when_the_data_changes(tmp_path):
    db_path = tmp_path / "t.db"
    conn = db.init_db(str(db_path))
    db.upsert_game(conn, GAME_TIMED)
    conn.close()
    before = tmp_path / "a.ics"
    ics.write_ics(str(db_path), str(before))

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE games SET home_score = 9, away_score = 9, "
                 "last_updated = '2030-01-01 12:00:00' WHERE id = ?", (GAME_TIMED["id"],))
    conn.commit()
    conn.close()

    after = tmp_path / "b.ics"
    ics.write_ics(str(db_path), str(after))
    assert before.read_bytes() != after.read_bytes()
    assert "DTSTAMP:20300101T120000Z" in after.read_text(encoding="utf-8")


def test_file_is_left_alone_when_only_the_timestamp_would_change(tmp_path):
    """Local runs and CI runs keep separate databases, so their last_updated
    values — and therefore DTSTAMP — differ even when the fixtures are identical.
    Rewriting on that alone makes the two churn against each other in git."""
    db_path = tmp_path / "t.db"
    conn = db.init_db(str(db_path))
    db.upsert_game(conn, GAME_TIMED)
    conn.close()

    out = tmp_path / "t.ics"
    ics.write_ics(str(db_path), str(out))
    first = out.read_bytes()

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE games SET last_updated = '2031-06-01 08:00:00'")
    conn.commit()
    conn.close()

    ics.write_ics(str(db_path), str(out))
    assert out.read_bytes() == first, "only DTSTAMP differed — the file must not be rewritten"


def test_file_is_rewritten_when_the_fixture_itself_changes(tmp_path):
    db_path = tmp_path / "t.db"
    conn = db.init_db(str(db_path))
    db.upsert_game(conn, GAME_TIMED)
    conn.close()
    out = tmp_path / "t.ics"
    ics.write_ics(str(db_path), str(out))
    first = out.read_bytes()

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE games SET game_time = '18:00' WHERE id = ?", (GAME_TIMED["id"],))
    conn.commit()
    conn.close()

    ics.write_ics(str(db_path), str(out))
    assert out.read_bytes() != first
    assert "DTSTART;TZID=Europe/Berlin:20260903T180000" in out.read_text(encoding="utf-8")


def test_no_line_exceeds_the_75_octet_limit(written):
    """RFC 5545 §3.1 counts octets including the leading space of a folded
    continuation line. Strict parsers reject longer lines outright."""
    _, path = written
    for i, line in enumerate(path.read_bytes().split(b"\r\n"), start=1):
        assert len(line) <= 75, f"line {i} is {len(line)} octets: {line[:50]!r}"


def test_folding_round_trips_including_umlauts():
    value = "DESCRIPTION:" + "Schiedsrichter: Müller, Jürgen / Weiß, Kätchen. " * 4
    folded = ics._fold(value)
    for line in folded.encode("utf-8").split(b"\r\n"):
        assert len(line) <= 75, f"{len(line)} octets: {line[:40]!r}"
    assert folded.replace("\r\n ", "") == value, "unfolding must restore the original"


def test_folding_never_splits_a_multibyte_character():
    folded = ics._fold("SUMMARY:" + "ä" * 120)
    for line in folded.encode("utf-8").split(b"\r\n"):
        line.decode("utf-8")  # raises if a character was cut in half
