"""Month groups and labels exactly as the start screen shows them."""

from datetime import date

import custom_events
from admin import agenda, games
from webadmin.testdata import EVENT_MULTI, EVENT_TIMED, TEAM_GAMES, team_ics

TODAY = date(2026, 10, 9)


def _games(team):
    return games.parse_ics(team_ics(TEAM_GAMES[team]), team)


def test_labels_for_timed_multi_day_and_game():
    timed = agenda.event_item(EVENT_TIMED)
    assert (timed.day_label, timed.date_label) == ("SA", "21")
    assert timed.detail == "09:00 – 15:00 · Sportbad Thurmfeld"
    multi = agenda.event_item(EVENT_MULTI)
    assert (multi.day_label, multi.date_label) == ("FR–SO", "27–29")
    assert multi.detail == "3 Tage · ganztägig · Sportpark Wedau"
    game = agenda.game_item(_games("herren_1")[1])
    assert (game.day_label, game.date_label, game.kind) == ("MO", "16", "game")
    assert game.detail == "20:30 · Herren I · 🔒 DSV"


def test_multi_day_across_months():
    item = agenda.event_item({**EVENT_MULTI, "start_date": "2026-11-30", "end_date": "2026-12-02"})
    assert (item.day_label, item.date_label) == ("MO–MI", "30.11.–2.12.")


def test_upcoming_is_grouped_by_month_and_sorted():
    months = agenda.build([EVENT_MULTI, EVENT_TIMED], _games("herren_1") + _games("herren_2"), TODAY)
    assert [m.label for m in months] == ["November 2026"]
    assert [i.title for i in months[0].items] == [
        "SG Wasserball Essen : Iserlohn Schleddenhofer SV (Oberliga)",
        "TPSK 1925 : SG Wasserball Essen II (Verbandsliga)",
        "Kampfrichter-Lehrgang",
        "Trainingslager Duisburg",
    ]


def test_ongoing_multi_day_counts_as_upcoming():
    running = {**EVENT_MULTI, "start_date": "2026-10-08", "end_date": "2026-10-10"}
    assert agenda.build([running], [], TODAY)[0].items[0].key == EVENT_MULTI["id"]


def test_past_view_is_newest_first():
    months = agenda.build([{**EVENT_TIMED, "start_date": "2026-09-03"}], _games("herren_1"), TODAY, past=True)
    assert [m.label for m in months] == ["September 2026"]
    assert [i.kind for i in months[0].items] == ["game", "event"]


def test_same_day_event_before_game_at_same_time():
    game = _games("herren_1")[1]
    event = {**EVENT_TIMED, "start_date": "2026-11-16", "start_time": "20:30", "end_time": None}
    assert [i.kind for i in agenda.build([event], [game], TODAY)[0].items] == ["event", "game"]


def test_broken_items():
    text = custom_events.serialize([{"id": "kaputt-1", "title": "Feier", "start_date": "2026-13-01"},
                                    {"title": ""}, "Unsinn"])
    broken = agenda.broken_items(custom_events.parse(text).invalid)
    assert sorted((b.key or "", b.title) for b in broken) == [
        ("", "(ohne Titel)"), ("", "(ohne Titel)"), ("kaputt-1", "Feier")]
    assert all(b.problem for b in broken)


def test_all_day_game_in_agenda():
    [turnier] = _games("u16")
    [item] = agenda.build([], [turnier], TODAY)[0].items
    assert (item.kind, item.key, item.day_label, item.date_label) == ("game", "2025_4_A_1", "SA", "5")
    assert item.detail == "ganztägig · U16 · 🔒 DSV"
    assert item.sort_time == ""


def test_event_ending_today_is_not_in_past_view():
    ends_today = {**EVENT_TIMED, "start_date": "2026-10-08", "end_date": "2026-10-09"}
    assert agenda.build([ends_today], [], TODAY, past=True) == []
    upcoming = agenda.build([ends_today], [], TODAY)
    assert [i.key for m in upcoming for i in m.items] == [EVENT_TIMED["id"]]
