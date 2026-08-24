"""The combined calendar is the one people subscribe to — its contents are a contract."""

import sqlite3
import uuid

import pytest
from icalendar import Calendar

import combine
import db

H1 = {
    "id": "h1-1", "season": "2025", "competition": "NRW Verbandsliga - Gruppe C",
    "game_date": "2026-09-21", "game_time": "20:00",
    "home_team": "SG Wasserball Essen", "away_team": "TPSK 1925", "status": "scheduled",
}
H2 = {**H1, "id": "h2-1", "home_team": "SG Wasserball Essen II",
      "competition": "Ruhrgebietsliga männlich"}
DAMEN = {**H1, "id": "d-1", "competition": "Ruhrgebietsliga weiblich"}


@pytest.fixture
def out(tmp_path, monkeypatch):
    outdir = tmp_path / "output"
    outdir.mkdir()
    for slug, game in [("sgw_essen_herren_1", H1), ("sgw_essen_herren_2", H2),
                       ("sgw_essen_damen", DAMEN)]:
        conn = db.init_db(str(outdir / f"{slug}.db"))
        db.upsert_game(conn, game)
        conn.commit()
        conn.close()
    monkeypatch.setattr(combine, "OUTPUT_DIR", outdir)
    monkeypatch.setattr(combine, "CUSTOM_EVENTS_DB", outdir / "custom_events.db")
    return outdir


def _add_custom(path, **kw):
    conn = sqlite3.connect(str(path))
    conn.executescript(combine._CUSTOM_EVENTS_SCHEMA)
    conn.execute(
        "INSERT INTO events (id, title, start_date, start_time, end_date, end_time, location, description) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (str(uuid.uuid4()), kw["title"], kw["start_date"], kw.get("start_time"),
         kw.get("end_date"), kw.get("end_time"), kw.get("location"), kw.get("description")),
    )
    conn.commit()
    conn.close()


def test_combined_calendar_holds_only_the_two_mens_teams(out):
    games, custom = combine.build_termine_db(out)
    assert games == 2, "Damen must not leak into the default subscription"
    assert custom == 0
    conn = sqlite3.connect(str(out / "sgw_termine.db"))
    ids = {r[0] for r in conn.execute("SELECT id FROM games")}
    conn.close()
    assert ids == {"h1-1", "h2-1"}


def test_custom_events_reach_the_calendar(out):
    _add_custom(out / "custom_events.db", title="Mannschaftsbesprechung",
                start_date="2026-09-03", start_time="19:30", end_time="21:30",
                location='Freibad Dellwig "Hesse", Scheppmannskamp 6, 45357 Essen')
    combine.build_termine_db(out)
    count = combine.write_termine_ics(out)
    assert count == 3, "two fixtures plus the club date"

    cal = Calendar.from_ical((out.parent / "sgw_termine.ics").read_bytes())
    events = {str(c["SUMMARY"]): c for c in cal.walk() if c.name == "VEVENT"}
    besprechung = events["Mannschaftsbesprechung"]
    assert "Scheppmannskamp" in str(besprechung["LOCATION"])
    assert str(besprechung["UID"]).startswith("custom-")


def test_all_day_custom_event_spans_a_single_day(out):
    _add_custom(out / "custom_events.db", title="Trainingsstart", start_date="2026-09-07")
    combine.build_termine_db(out)
    combine.write_termine_ics(out)
    text = (out.parent / "sgw_termine.ics").read_text(encoding="utf-8")
    assert "DTSTART;VALUE=DATE:20260907" in text
    assert "DTEND;VALUE=DATE:20260908" in text


def test_rebuilding_is_idempotent(out):
    combine.build_termine_db(out)
    first = combine.write_termine_ics(out)
    combine.build_termine_db(out)
    second = combine.write_termine_ics(out)
    assert first == second, "a rerun must not duplicate events"
