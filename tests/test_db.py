"""Schema and upsert behaviour — the scraper reruns twice a day, so upserts must be idempotent."""

import db

GAME = {
    "id": "g1", "season": "2025", "competition": "Ruhrgebietsliga männlich",
    "game_date": "2026-01-19", "game_time": "20:20",
    "home_team": "SG Wasserball Essen II", "away_team": "Wfr. 1912 Mülheim Masters",
    "status": "scheduled",
}


def test_upsert_is_idempotent(tmp_path):
    conn = db.init_db(str(tmp_path / "t.db"))
    for _ in range(3):
        db.upsert_game(conn, GAME)
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == 1


def test_upsert_updates_result_without_duplicating(tmp_path):
    conn = db.init_db(str(tmp_path / "t.db"))
    db.upsert_game(conn, GAME)
    db.upsert_game(conn, {**GAME, "status": "played", "home_score": 21, "away_score": 10})
    conn.commit()
    rows = conn.execute("SELECT status, home_score, away_score FROM games").fetchall()
    assert rows == [("played", 21, 10)]


def test_slug_separates_teams_and_age_groups():
    assert db.slug_for_team("SG Wasserball Essen", "NRW Verbandsliga - Gruppe C") == "sgw_essen_herren_1"
    assert db.slug_for_team("SG Wasserball Essen II", "Ruhrgebietsliga männlich") == "sgw_essen_herren_2"
    for comp, expected in [
        ("U12 Ruhrgebietsliga - Gruppe A", "sgw_essen_u12"),
        ("U14 Ruhrgebietsliga", "sgw_essen_u14"),
        ("U16 Ruhrgebietsliga", "sgw_essen_u16"),
        ("Ruhrgebietsliga weiblich", "sgw_essen_damen"),
    ]:
        assert db.slug_for_team("SG Wasserball Essen", comp) == expected
