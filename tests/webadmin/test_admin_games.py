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
    assert games.parse_teams("herren_1, damen") == ("herren_1", "damen")


GOOD = ["UID:good@sgw-essen.local", "SUMMARY:Gut", "DTSTART:20261110T200000Z", "DTEND:20261110T213000Z"]


def _calendar(*vevents):
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0"]
    for props in vevents:
        lines += ["BEGIN:VEVENT", *props, "END:VEVENT"]
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


def test_vevent_without_dtstart_is_skipped():
    text = _calendar(["UID:ohne@sgw-essen.local", "SUMMARY:Ohne Start"], GOOD)
    assert [g.uid for g in games.parse_ics(text, "damen")] == ["good"]


def test_unparseable_dtstart_is_skipped():
    text = _calendar(["UID:kaputt@sgw-essen.local", "DTSTART:20261340T200000Z"], GOOD)
    assert [g.uid for g in games.parse_ics(text, "damen")] == ["good"]


def test_mismatched_dtend_type_is_skipped():
    text = _calendar(["UID:gemischt@sgw-essen.local", "DTSTART:20261110T200000Z",
                      "DTEND;VALUE=DATE:20261111"], GOOD)
    assert [g.uid for g in games.parse_ics(text, "damen")] == ["good"]


def test_missing_uid_is_skipped():
    text = _calendar(["SUMMARY:Ohne UID", "DTSTART:20261110T200000Z"], GOOD)
    assert [g.uid for g in games.parse_ics(text, "damen")] == ["good"]


def test_floating_time_is_berlin_wall_clock():
    text = _calendar(["UID:floating@sgw-essen.local", "DTSTART:20261110T200000", "DTEND:20261110T213000"])
    [game] = games.parse_ics(text, "damen")
    assert game.start == datetime(2026, 11, 10, 20, 0, tzinfo=games.BERLIN)
    assert game.start.hour == 20


def test_dst_transitions():
    text = team_ics([
        ("2026_1_A_1", "Winter", "20261024T220000Z", "20261024T230000Z"),
        ("2027_1_A_1", "Sommer", "20270327T233000Z", "20270328T003000Z"),
    ])
    winter, summer = games.parse_ics(text, "damen")
    assert (winter.start.date(), winter.start.hour) == (date(2026, 10, 25), 0)
    assert (summer.start.date(), summer.start.hour, summer.start.minute) == (date(2027, 3, 28), 0, 30)


class Counting(FakeGitHubStore):
    reads = 0

    def read_text(self, path):
        self.reads += 1
        return super().read_text(path)


def test_failed_refresh_is_retried_at_most_once_a_minute(fake_dir):
    store, tick = Counting(fake_dir), Tick()
    cache = games.GameCache(store, clock=tick)
    cache.games(["damen"])
    tick.t = games.TTL
    store.fail["read_text"].append(Unavailable("down"))
    assert cache.games(["damen"])[1] is True
    tick.t = games.TTL + 30
    found, stale = cache.games(["damen"])
    assert len(found) == 1 and stale and store.reads == 2
    tick.t = games.TTL + games.RETRY_AFTER
    cache.games(["damen"])
    assert store.reads == 3


def test_failure_without_copy_is_also_retried_only_once_a_minute(fake_dir):
    store, tick = Counting(fake_dir), Tick()
    store.fail["read_text"].append(Unavailable("down"))
    cache = games.GameCache(store, clock=tick)
    assert cache.games(["damen"]) == ([], True)
    tick.t = games.RETRY_AFTER - 1
    assert cache.games(["damen"]) == ([], True)
    assert store.reads == 1


def test_garbage_file_falls_back_to_last_good_copy(fake_dir):
    store, tick = FakeGitHubStore(fake_dir), Tick()
    cache = games.GameCache(store, clock=tick)
    cache.games(["damen"])
    (fake_dir / "sgw_essen_damen.ics").write_text("das ist kein kalender\x00", encoding="utf-8")
    tick.t = games.TTL
    found, stale = cache.games(["damen"])
    assert len(found) == 1 and stale
    assert cache.find("gibt-es-nicht") is None
    assert games.GameCache(store).games(["damen"]) == ([], True)


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


def test_cache_survives_concurrent_requests(fake_dir):
    import threading

    cache = games.GameCache(FakeGitHubStore(fake_dir), ttl=0)  # every call refetches and writes
    errors = []

    def hammer():
        try:
            for _ in range(50):
                found, _stale = cache.games(slug for slug, _ in games.TEAMS)
                assert found
        except Exception as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
