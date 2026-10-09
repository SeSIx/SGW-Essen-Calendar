"""The start screen: agenda, team filter, banners and the game page."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote

import pytest

import custom_events
from admin.github_store import CorruptFile, RateLimited, Unauthorized, Unavailable
from webadmin.testdata import EVENT_TIMED, HOST, NOW

TEMPLATES = Path(__file__).resolve().parents[2] / "admin" / "templates"


def text(resp):
    return resp.get_data(as_text=True)


def test_default_shows_mens_games_and_events(user_client):
    body = text(user_client.get("/"))
    assert "November 2026" in body and "Heute: 9. Okt" in body
    assert "Kampfrichter-Lehrgang" in body and "Trainingslager Duisburg" in body
    assert "27–29" in body and "FR–SO" in body
    assert "Iserlohn Schleddenhofer SV" in body and "TPSK 1925" in body
    assert "Duisburg 98" not in body
    assert "20:30 · Herren I · 🔒 DSV" in body
    assert f'href="/termin/{EVENT_TIMED["id"]}"' in body and 'href="/spiel/2025_1_A_1"' in body
    assert 'href="/termin/neu"' in body


def test_team_filter_is_remembered_per_device(user_client):
    r = user_client.get("/?teams=damen")
    assert "Duisburg 98" in text(r) and "Iserlohn" not in text(r)
    cookie = next(h for h in r.headers.getlist("Set-Cookie") if h.startswith("teams="))
    assert cookie.startswith("teams=damen")
    for flag in ("Secure", "HttpOnly", "SameSite=Lax"):
        assert flag in cookie
    assert "Duisburg 98" in text(user_client.get("/"))


def test_unknown_team_values_fall_back_to_default(user_client):
    body = text(user_client.get("/?teams=quatsch"))
    assert "Iserlohn" in body and "Duisburg 98" not in body


def test_chips_toggle_one_team_each(user_client):
    body = unquote(text(user_client.get("/")))
    assert 'href="/?teams=herren_2"' in body
    assert 'href="/?teams=herren_1,herren_2,damen"' in body


def test_last_team_chip_toggles_off_to_empty_selection(user_client):
    one = unquote(text(user_client.get("/?teams=damen")))
    assert 'href="/?teams="' in one


def test_chip_links_keep_past_flag(user_client):
    body = unquote(text(user_client.get("/?frueher=1")))
    assert 'href="/?teams=herren_2&amp;frueher=1"' in body or 'href="/?teams=herren_2&frueher=1"' in body


def test_empty_selection_shows_only_club_events_and_hint(user_client):
    r = user_client.get("/?teams=")
    body = text(r)
    assert "Keine Mannschaft gewählt – nur Vereinstermine" in body
    assert "Kampfrichter-Lehrgang" in body and "Trainingslager Duisburg" in body
    assert "Iserlohn" not in body and "Duisburg 98" not in body and "/spiel/" not in body
    assert 'class="chip on"' not in body
    cookie = next(h for h in r.headers.getlist("Set-Cookie") if h.startswith("teams="))
    assert cookie.startswith("teams=keine;")
    for flag in ("Secure", "HttpOnly", "SameSite=Lax", "Path=/", "Max-Age=31536000"):
        assert flag in cookie


def test_hint_absent_with_selection(user_client):
    assert "Keine Mannschaft gewählt" not in text(user_client.get("/"))


def test_empty_selection_cookie_round_trip(user_client):
    user_client.get("/?teams=")
    body = text(user_client.get("/"))
    assert "Keine Mannschaft gewählt" in body and "Iserlohn" not in body
    user_client.get("/?teams=damen")
    assert "Duisburg 98" in text(user_client.get("/"))


def test_default_without_param_and_cookie(user_client):
    body = text(user_client.get("/"))
    assert "Iserlohn" in body and "Keine Mannschaft gewählt" not in body


def test_keine_cookie_means_no_team(user_client):
    user_client.set_cookie("teams", "keine", domain=HOST)
    body = text(user_client.get("/"))
    assert "Keine Mannschaft gewählt" in body and "Iserlohn" not in body


@pytest.mark.parametrize("value", ["quatsch", "keinezz"])
def test_garbage_cookie_falls_back_to_default(user_client, value):
    user_client.set_cookie("teams", value, domain=HOST)
    body = text(user_client.get("/"))
    assert "Iserlohn" in body and "Keine Mannschaft gewählt" not in body


def test_past_view(user_client):
    body = text(user_client.get("/?frueher=1"))
    assert "September 2026" in body and "ASC Duisburg" in body
    assert "Kampfrichter-Lehrgang" not in body and "Kommende Termine anzeigen" in body
    assert "Frühere Termine anzeigen" in text(user_client.get("/"))


def test_notices(user_client):
    assert "Gespeichert ✓ – Kalender in ca. 1 Min. aktuell" in text(user_client.get("/?ok=gespeichert"))
    assert "Gelöscht ✓ – Kalender in ca. 1 Min. aktuell" in text(user_client.get("/?ok=geloescht"))
    assert "✓" not in text(user_client.get("/?ok=quatsch"))


def test_broken_entries_are_listed(user_client, fake_dir):
    (fake_dir / "custom_events.json").write_text(custom_events.serialize([
        EVENT_TIMED, {"id": "kaputt-1", "title": "Feier", "start_date": "2026-13-01"},
        {"title": "Ohne Kennung"}]), encoding="utf-8")
    body = text(user_client.get("/"))
    assert "Fehlerhafte Einträge" in body and 'href="/termin/kaputt-1"' in body
    assert "bitte korrigieren" in body and "bitte Julius Bescheid geben" in body
    assert "Kampfrichter-Lehrgang" in body


def test_markup_in_titles_is_escaped(user_client, fake_dir):
    (fake_dir / "custom_events.json").write_text(
        custom_events.serialize([{**EVENT_TIMED, "title": "<b>fett</b>"}]), encoding="utf-8")
    body = text(user_client.get("/"))
    assert "<b>fett</b>" not in body and "&lt;b&gt;fett&lt;/b&gt;" in body


@pytest.mark.parametrize("exc, message", [
    (Unavailable("down"), "GitHub gerade nicht erreichbar"),
    (CorruptFile("x"), "custom_events.json ist beschädigt"),
    (RateLimited("x"), "Zu viele Anfragen"),
])
def test_store_problems_still_render(user_client, store, exc, message):
    store.fail["load"].append(exc)
    r = user_client.get("/")
    assert r.status_code == 200 and message in text(r) and "Erneut versuchen" in text(r)
    assert "Iserlohn" in text(r), "games come from the team calendars and still show"


def test_unauthorized_shows_red_banner(user_client, store):
    # A dead token fails every call, the team calendars included.
    store.fail["load"].append(Unauthorized("401"))
    store.fail["read_text"] += [Unauthorized("401"), Unauthorized("401")]
    body = text(user_client.get("/"))
    assert "banner-red" in body and "Speichern ist gesperrt" in body


@pytest.mark.parametrize("days, shown", [(10, True), (14, True), (20, False)])
def test_expiry_banner(user_client, store, days, shown):
    store.token_expiry = datetime.fromtimestamp(NOW, UTC) + timedelta(days=days)
    body = text(user_client.get("/"))
    assert ("banner-yellow" in body) is shown
    if shown:
        assert "Zugang zu GitHub läuft am" in body


def test_games_unavailable_hint(user_client, store):
    store.fail["read_text"].append(Unavailable("down"))
    assert "Spiele gerade nicht ganz aktuell" in text(user_client.get("/"))


def test_game_page(user_client):
    body = text(user_client.get("/spiel/2025_1_A_1"))
    assert "Iserlohn Schleddenhofer SV" in body and "Herren I" in body
    assert "Mo 16.11.2026, 20:30 Uhr" in body and "Spiele kommen automatisch vom DSV" in body
    assert user_client.get("/spiel/gibt-es-nicht").status_code == 404
    assert user_client.get("/spiel/a.b").status_code == 404


def test_temporary_start_page_is_gone():
    assert not (TEMPLATES / "start.html").exists()
