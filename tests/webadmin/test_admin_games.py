"""Fixtures from the six team calendars: parsing, time zone, cache, filter."""

from datetime import date, datetime

from admin import games
from admin.fake_store import FakeGitHubStore
from admin.github_store import Unavailable
from webadmin.testdata import TEAM_GAMES, team_ics


def test_parse_ics_converts_to_berlin_and_strips_uid_domain():
    [past, upcoming] = games.parse_ics(team_ics(TEAM_GAMES["herren_1"]), "herren_1")
    assert upcoming.uid == "2025_1_A_1"
    assert upcoming.start == datetime(2026, 11, 16, 20, 30, tzinfo=games.BERLIN)
    assert upcoming.start.utcoffset().total_seconds() == 3600
    assert past.start.hour == 14, "September is summer time: 12:00Z is 14:00 in Essen"
    assert upcoming.team_label == "Herren I" and not upcoming.all_day
    assert upcoming.location == "Sportbad Thurmfeld, Essen"


def test_all_day_game():
    [turnier] = games.parse_ics(team_ics(TEAM_GAMES["u16"]), "u16")
    assert turnier.all_day and turnier.start_date == date(2026, 12, 5)


def test_parse_teams():
    assert games.parse_teams(None) is None
    assert games.parse_teams("damen,herren_1,quatsch") == ("herren_1", "damen")
    assert games.parse_teams("quatsch") == ()
    assert games.parse_teams("") == ()


class Tick:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_cache_reuses_for_five_minutes(fake_dir):
    store, tick = FakeGitHubStore(fake_dir), Tick()
    cache = games.GameCache(store, clock=tick)
    cache.games(["damen"])
    (fake_dir / "sgw_essen_damen.ics").write_text(team_ics([]), encoding="utf-8")
    found, stale = cache.games(["damen"])
    assert len(found) == 1 and not stale
    tick.t = games.TTL
    found, _ = cache.games(["damen"])
    assert found == []


def test_failure_falls_back_to_last_good_copy(fake_dir):
    store, tick = FakeGitHubStore(fake_dir), Tick()
    cache = games.GameCache(store, clock=tick)
    cache.games(["damen"])
    tick.t = games.TTL
    store.fail["read_text"].append(Unavailable("down"))
    found, stale = cache.games(["damen"])
    assert len(found) == 1 and stale


def test_failure_without_copy_is_empty_and_stale(fake_dir):
    store = FakeGitHubStore(fake_dir)
    store.fail["read_text"].append(Unavailable("down"))
    assert games.GameCache(store).games(["damen"]) == ([], True)


def test_find_searches_all_teams(fake_dir):
    cache = games.GameCache(FakeGitHubStore(fake_dir))
    assert cache.find("2025_3_A_1").team == "damen"
    assert cache.find("gibt-es-nicht") is None
