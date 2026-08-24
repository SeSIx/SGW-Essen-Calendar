"""Health signals: the pipeline must be loud when it produces nothing useful."""

from datetime import date

import db

BASE = {
    "season": "2025", "competition": "NRW Verbandsliga - Gruppe C",
    "home_team": "SG Wasserball Essen", "away_team": "TPSK 1925", "status": "scheduled",
}


def _conn(tmp_path, *games):
    conn = db.init_db(str(tmp_path / "t.db"))
    for g in games:
        db.upsert_game(conn, g)
    conn.commit()
    return conn


def test_counts_upcoming_fixtures(tmp_path):
    conn = _conn(
        tmp_path,
        {**BASE, "id": "past", "game_date": "2026-07-12", "game_time": "20:00"},
        {**BASE, "id": "future", "game_date": "2026-09-21", "game_time": "20:30"},
        {**BASE, "id": "today", "game_date": "2026-08-24", "game_time": "20:00"},
    )
    assert db.count_upcoming(conn, date(2026, 8, 24)) == 2, "today counts as upcoming"


def test_undated_games_are_reported_not_hidden(tmp_path):
    conn = _conn(
        tmp_path,
        {**BASE, "id": "ok", "game_date": "2026-09-21"},
        {**BASE, "id": "nodate", "game_date": ""},
        {**BASE, "id": "nulldate", "game_date": None},
    )
    assert sorted(db.undated_game_ids(conn)) == ["nodate", "nulldate"]


def test_needs_detail_skips_complete_past_games(tmp_path):
    complete = {**BASE, "id": "done", "game_date": "2026-07-12", "status": "played",
                "home_score": 22, "away_score": 8, "venue": "Freibad Dellwig",
                "venue_address": "Scheppmannskamp 6, 45357 Essen"}
    conn = _conn(tmp_path, complete)
    assert db.needs_detail(conn, complete, date(2026, 8, 24)) is False


def test_needs_detail_for_game_without_venue(tmp_path):
    thin = {**BASE, "id": "thin", "game_date": "2026-09-21"}
    conn = _conn(tmp_path, thin)
    assert db.needs_detail(conn, thin, date(2026, 8, 24)) is True


def test_needs_detail_for_played_game_missing_score(tmp_path):
    scoreless = {**BASE, "id": "s", "game_date": "2026-07-12", "status": "played",
                 "venue": "Freibad Dellwig", "venue_address": "Scheppmannskamp 6"}
    conn = _conn(tmp_path, scoreless)
    assert db.needs_detail(conn, scoreless, date(2026, 8, 24)) is True


def test_needs_detail_for_unknown_game(tmp_path):
    conn = _conn(tmp_path)
    assert db.needs_detail(conn, {**BASE, "id": "new", "game_date": "2026-09-21"},
                           date(2026, 8, 24)) is True
