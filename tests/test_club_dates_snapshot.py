"""Club dates are a published contract: refactoring the code that reads them
must not change a single byte that subscribers receive (DTSTAMP aside, which is
derived from database timestamps)."""

import shutil
import tempfile
from datetime import date
from pathlib import Path

import combine
import db

FIXTURES = Path(__file__).parent / "fixtures"
SNAPSHOT_JSON = FIXTURES / "custom_events_snapshot.json"
SNAPSHOT_TERMINE = FIXTURES / "sgw_termine_snapshot.ics"
SNAPSHOT_VEREIN = FIXTURES / "sgw_vereinstermine_snapshot.ics"
TODAY = date(2026, 10, 9)

H1 = {
    "id": "h1-1", "season": "2025", "competition": "NRW Verbandsliga - Gruppe C",
    "game_date": "2026-09-21", "game_time": "20:00",
    "home_team": "SG Wasserball Essen", "away_team": "TPSK 1925", "status": "scheduled",
}
H2 = {**H1, "id": "h2-1", "home_team": "SG Wasserball Essen II",
      "competition": "Ruhrgebietsliga männlich"}


def _build(workdir: Path) -> tuple[bytes, bytes]:
    outdir = workdir / "output"
    outdir.mkdir()
    for slug, game in (("sgw_essen_herren_1", H1), ("sgw_essen_herren_2", H2)):
        conn = db.init_db(str(outdir / f"{slug}.db"))
        db.upsert_game(conn, game)
        conn.commit()
        conn.close()
    shutil.copy(SNAPSHOT_JSON, workdir / "custom_events.json")

    saved = (combine.OUTPUT_DIR, combine.CUSTOM_EVENTS_JSON, combine.LEGACY_CUSTOM_EVENTS_DB)
    combine.OUTPUT_DIR = outdir
    combine.CUSTOM_EVENTS_JSON = workdir / "custom_events.json"
    combine.LEGACY_CUSTOM_EVENTS_DB = outdir / "custom_events.db"
    try:
        combine.build_termine_db(outdir)
        combine.write_termine_ics(outdir, today=TODAY)
        combine.write_vereinstermine_ics(outdir)
    finally:
        combine.OUTPUT_DIR, combine.CUSTOM_EVENTS_JSON, combine.LEGACY_CUSTOM_EVENTS_DB = saved
    return ((workdir / "sgw_termine.ics").read_bytes(),
            (workdir / "sgw_vereinstermine.ics").read_bytes())


def _without_dtstamp(raw: bytes) -> list[bytes]:
    return [line for line in raw.split(b"\r\n") if not line.startswith(b"DTSTAMP:")]


def regenerate() -> None:
    """Rewrite the .ics snapshots from the current code. Run once, before refactoring."""
    with tempfile.TemporaryDirectory() as tmp:
        termine, verein = _build(Path(tmp))
    SNAPSHOT_TERMINE.write_bytes(termine)
    SNAPSHOT_VEREIN.write_bytes(verein)


def test_combined_calendar_matches_the_snapshot(tmp_path):
    termine, _ = _build(tmp_path)
    assert _without_dtstamp(termine) == _without_dtstamp(SNAPSHOT_TERMINE.read_bytes())


def test_club_calendar_matches_the_snapshot(tmp_path):
    _, verein = _build(tmp_path)
    assert _without_dtstamp(verein) == _without_dtstamp(SNAPSHOT_VEREIN.read_bytes())
