"""Parser tests — run entirely offline against captured DSV HTML."""

import pytest
from conftest import FIXTURE_GAME_ID

import scraper


def test_club_page_yields_games(games):
    assert len(games) > 50, "captured season should contain the full club schedule"


def test_every_game_has_id_and_competition(games):
    assert all(g["id"] for g in games)
    assert all(g["competition"] for g in games)


def test_game_ids_are_unique(games):
    ids = [g["id"] for g in games]
    assert len(ids) == len(set(ids)), "DSV game ids must be unique — they are the upsert key"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("21.09.26 20:30", ("2026-09-21", "20:30")),
        ("01.02.2026 14:30", ("2026-02-01", "14:30")),
        ("7.3.26", ("2026-03-07", "")),
        ("", ("", "")),
        ("kein Datum", ("", "")),
    ],
)
def test_parse_date(raw, expected):
    assert scraper._parse_date(raw) == expected


def test_dates_are_iso_or_empty(games):
    for g in games:
        d = g.get("game_date") or ""
        assert d == "" or (len(d) == 10 and d[4] == "-" and d[7] == "-"), f"bad date {d!r}"


def test_every_game_has_an_essen_side(games):
    for g in games:
        sides = f"{g.get('home_team','')} {g.get('away_team','')}".lower()
        assert "essen" in sides, f"non-Essen game leaked into results: {g['id']}"


def test_played_games_carry_scores(games):
    played = [g for g in games if g.get("status") == "played"]
    assert played, "fixture season must contain played games"


def test_google_maps_url_encoding():
    url = scraper.build_google_maps_url("Scheppmannskamp 6, 45357 Essen")
    assert url.startswith("https://www.google.com/maps/search/?")
    assert "Scheppmannskamp+6%2C+45357+Essen" in url
    assert scraper.build_google_maps_url("") == ""


def test_parse_game_detail_extracts_venue_and_referees(game_detail, games):
    game = next(g for g in games if g["id"] == FIXTURE_GAME_ID)
    detail = scraper.parse_game_detail(
        game_detail, game["detail_url"], "SG Wasserball Essen II",
        home_team=game["home_team"], away_team=game["away_team"],
    )
    assert detail.get("venue"), "venue must be extracted from the detail page"
    assert detail.get("venue_address"), "address feeds the calendar LOCATION field"
    assert detail.get("home_score") is not None
    assert detail.get("away_score") is not None


def test_parse_game_detail_yields_player_stats(game_detail, games):
    game = next(g for g in games if g["id"] == FIXTURE_GAME_ID)
    detail = scraper.parse_game_detail(
        game_detail, game["detail_url"], "SG Wasserball Essen II",
        home_team=game["home_team"], away_team=game["away_team"],
    )
    players = detail.get("essen_players") or []
    assert players, "Essen player table must be parsed"
    assert all(p["player_name"] for p in players), "every roster row needs a name"
    assert all(isinstance(p["goals"], int) for p in players)
    assert sum(p["goals"] for p in players) > 0, "a played game must have scorers"
