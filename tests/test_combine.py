"""The combined calendar is the one people subscribe to — its contents are a contract."""

import json
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
    monkeypatch.setattr(combine, "CUSTOM_EVENTS_JSON", tmp_path / "custom_events.json")
    monkeypatch.setattr(combine, "LEGACY_CUSTOM_EVENTS_DB", outdir / "custom_events.db")
    return outdir


def _add_custom(_unused=None, **kw):
    events = combine.load_custom_events()
    events.append({
        "id": str(uuid.uuid4()), "title": kw["title"], "start_date": kw["start_date"],
        "start_time": kw.get("start_time"), "end_date": kw.get("end_date"),
        "end_time": kw.get("end_time"), "location": kw.get("location"),
        "description": kw.get("description"),
    })
    combine.save_custom_events(events)


def test_combined_calendar_holds_only_the_two_mens_teams(out):
    games, custom = combine.build_termine_db(out)
    assert games == 2, "Damen must not leak into the default subscription"
    assert custom == 0
    conn = sqlite3.connect(str(out / "sgw_termine.db"))
    ids = {r[0] for r in conn.execute("SELECT id FROM games")}
    conn.close()
    assert ids == {"h1-1", "h2-1"}


def test_custom_events_reach_the_calendar(out):
    _add_custom(title="Mannschaftsbesprechung",
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
    _add_custom(title="Trainingsstart", start_date="2026-09-07")
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


def test_club_dates_survive_a_rebuild_on_a_fresh_checkout(out):
    """A scheduled run starts from a clean checkout with no databases. If club
    dates lived only in the gitignored SQLite file, the job would quietly delete
    them from the published calendar — which is exactly what happened once."""
    _add_custom(title="Mannschaftsbesprechung", start_date="2026-09-03",
                start_time="19:30", end_time="21:30", location="Freibad Dellwig")
    combine.build_termine_db(out)
    combine.write_termine_ics(out)

    for stale in out.glob("*.db"):          # simulate the fresh runner
        stale.unlink()
    assert combine.CUSTOM_EVENTS_JSON.exists(), "club dates must be version-controlled"

    for slug, game in [("sgw_essen_herren_1", H1), ("sgw_essen_herren_2", H2)]:
        conn = db.init_db(str(out / f"{slug}.db"))
        db.upsert_game(conn, game)
        conn.commit()
        conn.close()
    combine.build_termine_db(out)
    count = combine.write_termine_ics(out)

    text = (out.parent / "sgw_termine.ics").read_text(encoding="utf-8")
    assert "SUMMARY:Mannschaftsbesprechung" in text
    assert count == 3


def test_legacy_database_is_migrated_once(out):
    """Existing installations keep their club dates without manual steps."""
    legacy = out / "custom_events.db"
    conn = sqlite3.connect(str(legacy))
    conn.executescript(combine._CUSTOM_EVENTS_SCHEMA)
    conn.execute(
        "INSERT INTO events (id, title, start_date) VALUES (?,?,?)",
        ("legacy-1", "Weihnachtsfeier", "2026-12-19"),
    )
    conn.commit()
    conn.close()

    events = combine.load_custom_events()
    assert [e["title"] for e in events] == ["Weihnachtsfeier"]
    assert combine.CUSTOM_EVENTS_JSON.exists()
    assert json.loads(combine.CUSTOM_EVENTS_JSON.read_text())[0]["id"] == "legacy-1"


def test_club_dates_are_published_on_their_own(out):
    """Someone who subscribes to a single team still wants the meetings and
    training dates, so they have to be available without the two men's teams
    attached to them."""
    _add_custom(title="Mannschaftsbesprechung", start_date="2026-09-03",
                start_time="19:30", end_time="21:30", location="Freibad Dellwig")
    combine.build_termine_db(out)
    count = combine.write_vereinstermine_ics(out)
    assert count == 1, "club dates only — the fixtures belong to the team feeds"

    path = out.parent / "sgw_vereinstermine.ics"
    cal = Calendar.from_ical(path.read_bytes())
    summaries = {str(c["SUMMARY"]) for c in cal.walk() if c.name == "VEVENT"}
    assert summaries == {"Mannschaftsbesprechung"}
    assert "X-WR-CALNAME:SGW Essen Vereinstermine" in path.read_text(encoding="utf-8"), \
        "a distinct name, or clients show two calendars called the same thing"


def _write_raw(entries):
    combine.CUSTOM_EVENTS_JSON.write_text(json.dumps(entries), encoding="utf-8")


VALID = {"id": "ok-1", "title": "Besprechung", "start_date": "2026-09-03",
         "start_time": "19:30", "end_date": None, "end_time": "21:30",
         "location": None, "description": None}


def test_invalid_club_date_is_skipped_not_fatal(out, capsys):
    _write_raw([VALID, {"id": "bad", "title": "", "start_date": "2026-13-01"}])
    games, custom = combine.build_termine_db(out)
    assert (games, custom) == (2, 1), "fixtures and the good club date still publish"
    assert "skipping invalid custom event bad" in capsys.readouterr().out


def test_unparseable_file_still_fails_loudly(out):
    combine.CUSTOM_EVENTS_JSON.write_text("[{broken", encoding="utf-8")
    with pytest.raises(ValueError):
        combine.build_termine_db(out)


@pytest.mark.usefixtures("out")
def test_save_writes_the_canonical_layout():
    combine.save_custom_events([VALID])
    text = combine.CUSTOM_EVENTS_JSON.read_text(encoding="utf-8")
    assert text == json.dumps([VALID], indent=2, ensure_ascii=False) + "\n"


def _answers(monkeypatch, *values):
    it = iter(values)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(it))


@pytest.mark.usefixtures("out")
def test_add_event_stores_a_valid_date(monkeypatch):
    _answers(monkeypatch, "Feier", "2026-12-19", "18:00", "", "", "Vereinsheim", "")
    combine.cmd_add_event()
    [event] = combine.load_custom_events()
    assert (event["title"], event["start_time"], event["location"]) == \
        ("Feier", "18:00", "Vereinsheim")


@pytest.mark.usefixtures("out")
def test_add_event_rejects_invalid_input(monkeypatch, capsys):
    _answers(monkeypatch, "Feier", "2026-02-30", "", "", "", "", "")
    combine.cmd_add_event()
    assert not combine.CUSTOM_EVENTS_JSON.exists()
    assert "Invalid" in capsys.readouterr().out


@pytest.mark.usefixtures("out")
def test_add_event_refuses_while_the_file_has_broken_entries(monkeypatch, capsys):
    _write_raw([VALID, {"id": "bad"}])
    monkeypatch.setattr("builtins.input", lambda _p="": pytest.fail("prompted"))
    combine.cmd_add_event()
    assert json.loads(combine.CUSTOM_EVENTS_JSON.read_text())[1] == {"id": "bad"}
    assert "fix custom_events.json first" in capsys.readouterr().out


def _rebuild(out):
    combine.build_termine_db(out)
    combine.write_termine_ics(out)
    return combine.write_vereinstermine_ics(out)


def test_deleted_club_date_leaves_both_calendars(out):
    """The termine DB survives between scheduled runs via the actions cache, so
    a date removed from the JSON must be removed from the DB as well."""
    _add_custom(title="Weihnachtsfeier", start_date="2026-12-19")
    _add_custom(title="Mannschaftsbesprechung", start_date="2026-09-03")
    _rebuild(out)

    combine.save_custom_events(
        [e for e in combine.load_custom_events() if e["title"] != "Weihnachtsfeier"])
    assert _rebuild(out) == 1

    for name in ("sgw_termine.ics", "sgw_vereinstermine.ics"):
        text = (out.parent / name).read_text(encoding="utf-8")
        assert "Weihnachtsfeier" not in text, name
        assert "Mannschaftsbesprechung" in text, name


def test_removing_the_last_club_date_empties_the_club_feed(out):
    _add_custom(title="Weihnachtsfeier", start_date="2026-12-19")
    _rebuild(out)
    combine.save_custom_events([])
    assert _rebuild(out) == 0
    assert "BEGIN:VEVENT" not in (out.parent / "sgw_vereinstermine.ics").read_text()
