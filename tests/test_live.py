"""Live smoke tests against the real DSV portal.

Excluded from the default run (`-m "not live"`) so the normal suite stays
offline and fast. The nightly workflow runs these to notice when the DSV
changes its HTML — the failure mode that would otherwise silently empty
every calendar.
"""

import pytest

import config
import scraper

pytestmark = pytest.mark.live


def test_club_page_is_reachable_and_parses():
    key = config.season_keys()[0]
    html = scraper.fetch_club_page(key)
    assert len(html) > 10_000, "club page suspiciously small — layout may have changed"
    games = scraper.parse_games(html, key)
    assert games, "no games parsed from a live club page — the parser is broken"


def test_parsed_games_still_have_the_expected_shape():
    key = config.season_keys()[0]
    games = scraper.parse_games(scraper.fetch_club_page(key), key)
    for g in games[:10]:
        assert g["id"], "every game needs the DSV id used as upsert key"
        assert g["competition"]
        assert g["home_team"] and g["away_team"]


def test_at_least_one_essen_team_is_present():
    key = config.season_keys()[0]
    games = scraper.parse_games(scraper.fetch_club_page(key), key)
    sides = " ".join(f"{g['home_team']} {g['away_team']}" for g in games).lower()
    assert "wasserball essen" in sides
