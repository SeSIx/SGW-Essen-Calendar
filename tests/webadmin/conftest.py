"""Fixtures for the admin app tests."""

import pytest

import custom_events
from webadmin.testdata import EVENT_MULTI, EVENT_TIMED, TEAM_GAMES, team_ics


@pytest.fixture
def fake_dir(tmp_path):
    d = tmp_path / "fake-github"
    d.mkdir()
    (d / "custom_events.json").write_text(
        custom_events.serialize([EVENT_TIMED, EVENT_MULTI]), encoding="utf-8")
    for team, games in TEAM_GAMES.items():
        (d / f"sgw_essen_{team}.ics").write_text(team_ics(games), encoding="utf-8")
    return d
