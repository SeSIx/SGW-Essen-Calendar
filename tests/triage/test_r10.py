"""Triage R10: "Spiel am 23.01 ist im Kalender doppelt".

Published sgw_termine.ics / sgw_essen_herren_1.ics carry
"Stolberger SV 1910 : SG Wasserball Essen" on 2027-01-23 18:20 twice, as
LeagueID=196 GameID=52 and GameID=68. The DSV club page at report time lists
only GameID=52 for that date; Game.aspx for GameID=68 answers
"keine Spieldaten vorhanden". The pipeline only ever upserts, so a fixture the
DSV withdrew/renumbered stays in the database and in every calendar.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import combine  # noqa: E402
import main  # noqa: E402
import scraper  # noqa: E402


def _game(game_id: int) -> dict:
    return {
        "id": f"2026_196__{game_id}",
        "season": "2026/2027",
        "competition": "NRW Oberliga männlich",
        "game_number": None,
        "game_date": "2027-01-23",
        "game_time": "18:20",
        "home_team": "Stolberger SV 1910",
        "away_team": "SG Wasserball Essen",
        "home_score": None,
        "away_score": None,
        "status": "scheduled",
        "detail_url": ("https://dsvdaten.dsv.de/Modules/WB/Game.aspx?Season=2026"
                       f"&LeagueID=196&Group=&LeagueKind=L&GameID={game_id}"),
    }


def _events_on(ics_path: Path, day: str) -> int:
    text = ics_path.read_bytes().decode("utf-8")  # keep CRLF; read_text() folds it to \n
    return sum(1 for line in text.split("\r\n") if line.startswith(f"DTSTART:{day}"))


def test_withdrawn_dsv_fixture_is_not_kept_as_duplicate(tmp_path, monkeypatch):
    out = tmp_path / "output"
    monkeypatch.setattr(main, "OUTPUT_DIR", out)
    monkeypatch.setattr(main, "__file__", str(tmp_path / "main.py"))
    monkeypatch.setattr(combine, "CUSTOM_EVENTS_JSON", tmp_path / "custom_events.json")
    monkeypatch.setattr(combine, "LEGACY_CUSTOM_EVENTS_DB", out / "custom_events.db")
    monkeypatch.setattr(scraper, "fetch_club_page", lambda key: f"<club {key}>")

    # Earlier run: the DSV listed the fixture under both ids.
    listing = {"games": [_game(52), _game(68)]}
    monkeypatch.setattr(scraper, "parse_games", lambda html, key: [dict(g) for g in listing["games"]])
    main.cmd_scrape(["2026"], fetch_details=False, write_ics_files=True)

    # Current DSV state (inputs/dsv/club_2026.html): only GameID=52 on 23.01.27.
    listing["games"] = [_game(52)]
    main.cmd_scrape(["2026"], fetch_details=False, write_ics_files=True)

    assert _events_on(tmp_path / "sgw_essen_herren_1.ics", "20270123") == 1
    assert _events_on(tmp_path / "sgw_termine.ics", "20270123") == 1
