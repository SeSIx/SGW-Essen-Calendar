"""Games the DSV withdraws leave the databases — but a thin club page must not wipe real ones."""

import sqlite3

import combine
import db
import main


def _game(n: int, team: str = "SG Wasserball Essen") -> dict:
    return {
        "id": f"2026_196__{n}", "season": "2026/2027", "competition": "NRW Oberliga männlich",
        "game_date": "2027-01-23", "game_time": "18:20",
        "home_team": "Stolberger SV 1910", "away_team": team, "status": "scheduled",
    }


def _ids(path) -> set[str]:
    conn = sqlite3.connect(str(path))
    ids = {r[0] for r in conn.execute("SELECT id FROM games")}
    conn.close()
    return ids


def _team_db(tmp_path, games):
    conn = db.init_db(str(tmp_path / "team.db"))
    for g in games:
        db.upsert_game(conn, g)
    return conn


def test_withdrawn_game_is_removed(tmp_path):
    conn = _team_db(tmp_path, [_game(1), _game(2), _game(3)])
    assert main._prune_withdrawn(conn, "t", [_game(1), _game(2)], ["2026"]) == 1
    conn.commit()
    assert _ids(tmp_path / "team.db") == {"2026_196__1", "2026_196__2"}


def test_empty_listing_for_the_team_prunes_nothing(tmp_path):
    conn = _team_db(tmp_path, [_game(1), _game(2)])
    assert main._prune_withdrawn(conn, "t", [], ["2026"]) == 0


def test_shrunken_listing_prunes_nothing(tmp_path):
    conn = _team_db(tmp_path, [_game(n) for n in range(1, 6)])
    assert main._prune_withdrawn(conn, "t", [_game(1), _game(2)], ["2026"]) == 0
    conn.commit()
    assert len(_ids(tmp_path / "team.db")) == 5


def test_seasons_not_scraped_are_left_alone(tmp_path):
    old = {**_game(9), "id": "2025_196__9", "season": "2025/2026"}
    conn = _team_db(tmp_path, [old, _game(1)])
    assert main._prune_withdrawn(conn, "t", [_game(1)], ["2026"]) == 0


def _write_source(out, slug, games):
    conn = db.init_db(str(out / f"{slug}.db"))
    for g in games:
        db.upsert_game(conn, g)
    conn.close()


def _patch_combine(tmp_path, monkeypatch):
    monkeypatch.setattr(combine, "CUSTOM_EVENTS_JSON", tmp_path / "custom_events.json")
    monkeypatch.setattr(combine, "LEGACY_CUSTOM_EVENTS_DB", tmp_path / "custom_events.db")


def test_combined_db_drops_game_its_team_db_lost(tmp_path, monkeypatch):
    _patch_combine(tmp_path, monkeypatch)
    _write_source(tmp_path, "sgw_essen_herren_1", [_game(1), _game(2)])
    combine.build_termine_db(tmp_path)
    (tmp_path / "sgw_essen_herren_1.db").unlink()
    _write_source(tmp_path, "sgw_essen_herren_1", [_game(1)])
    combine.build_termine_db(tmp_path)
    assert _ids(tmp_path / "sgw_termine.db") == {"2026_196__1"}


def test_combined_db_keeps_games_of_a_missing_team_db(tmp_path, monkeypatch):
    _patch_combine(tmp_path, monkeypatch)
    h2 = _game(7, team="SG Wasserball Essen II")
    _write_source(tmp_path, "sgw_essen_herren_1", [_game(1)])
    _write_source(tmp_path, "sgw_essen_herren_2", [h2])
    combine.build_termine_db(tmp_path)
    (tmp_path / "sgw_essen_herren_2.db").unlink()
    combine.build_termine_db(tmp_path)
    assert _ids(tmp_path / "sgw_termine.db") == {"2026_196__1", "2026_196__7"}
