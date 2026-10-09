"""Instant team filter: every team's games are rendered, and /filter stores the choice."""

import re

import pytest

from webadmin.testdata import BASE, csrf_of

HDR = {"Origin": BASE, "Sec-Fetch-Site": "same-origin"}


def text(resp):
    return resp.get_data(as_text=True)


def game_items(body):
    """(team, hidden, uid) for every game <li>."""
    out = []
    for attrs, inner in re.findall(r"<li([^>]*data-team[^>]*)>(.*?)</li>", body, re.S):
        out.append((re.search(r'data-team="([^"]+)"', attrs).group(1),
                    bool(re.search(r"\shidden(\s|$|=)", attrs)),
                    re.search(r'href="/spiel/([^"]+)"', inner).group(1)))
    return out


def test_index_renders_all_teams_with_unselected_hidden(user_client):
    items = game_items(text(user_client.get("/")))
    by_team = {team: hid for team, hid, _ in items}
    assert by_team == {"herren_1": False, "herren_2": False, "damen": True, "u16": True}
    assert len(items) == 4  # past game of herren_1 is not in the upcoming view


def test_past_view_renders_past_games_of_all_teams(user_client):
    items = game_items(text(user_client.get("/?frueher=1")))
    assert [(t, h) for t, h, _ in items] == [("herren_1", False)]


def test_empty_selection_hides_every_game_and_shows_hint(user_client):
    body = text(user_client.get("/?teams="))
    assert all(hid for _, hid, _ in game_items(body))
    assert re.search(r'<p class="hint"[^>]*data-no-teams(?![^>]*hidden)[^>]*>Keine Mannschaft', body)


def test_hint_is_hidden_with_selection(user_client):
    assert re.search(r'<p class="hint"[^>]*data-no-teams[^>]*\shidden[^>]*>', text(user_client.get("/")))


def test_month_without_visible_game_is_hidden(user_client):
    body = text(user_client.get("/?teams="))
    assert 'data-month' in body
    for section in re.findall(r"<section[^>]*data-month[^>]*>", body):
        assert "Dezember" not in section
    # December 2026 only holds a u16 game (plus nothing else): hidden by default
    default = text(user_client.get("/"))
    dec = re.search(r"<section([^>]*)>\s*<h2 class=\"month\"><span>Dezember 2026", default)
    assert dec and "hidden" in dec.group(1)


def test_chips_carry_filter_endpoint_and_token(user_client):
    body = text(user_client.get("/"))
    assert 'data-filter-url="/filter"' in body
    assert f'data-csrf="{csrf_of(user_client.get("/"))}"' in body
    assert 'data-team="damen"' in body.split('class="chips"')[1].split("</nav>")[0]


def post(client, teams, **kw):
    page = client.get("/")
    data = {"csrf_token": csrf_of(page)}
    if teams is not None:
        data["teams"] = teams
    data.update(kw.pop("data", {}))
    return client.post("/filter", data=data, headers=kw.pop("headers", HDR))


def cookie(resp):
    return next((h for h in resp.headers.getlist("Set-Cookie") if h.startswith("teams=")), None)


def test_filter_sets_cookie(user_client):
    r = post(user_client, "damen,u16")
    assert r.status_code == 204 and r.get_data() == b""
    c = cookie(r)
    assert c.startswith('teams="damen\\054u16";')  # werkzeug quotes the comma, as for the index
    for flag in ("Secure", "HttpOnly", "SameSite=Lax", "Path=/", "Max-Age=31536000"):
        assert flag in c
    assert "Duisburg 98" in text(user_client.get("/")) and ("damen", False) in [
        (t, h) for t, h, _ in game_items(text(user_client.get("/")))]


def test_filter_empty_stores_keine(user_client):
    r = post(user_client, "")
    assert r.status_code == 204 and cookie(r).startswith("teams=keine;")


@pytest.mark.parametrize("teams", [None, "quatsch", ",,", "damen;u16x"])
def test_filter_rejects_invalid_teams(user_client, teams):
    r = post(user_client, teams)
    assert r.status_code == 400 and cookie(r) is None


def test_filter_rejects_bad_csrf(user_client):
    r = post(user_client, "damen", data={"csrf_token": "x" * 64})
    assert r.status_code == 400 and cookie(r) is None


def test_filter_rejects_foreign_origin(user_client):
    r = post(user_client, "damen", headers={"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"})
    assert r.status_code == 403 and cookie(r) is None


def test_filter_requires_login(client):
    r = client.post("/filter", data={"teams": "damen", "csrf_token": "x"}, headers=HDR)
    assert r.status_code in (302, 303, 400, 401) and cookie(r) is None
    assert r.status_code != 204


def test_filter_get_not_allowed(user_client):
    assert user_client.get("/filter").status_code == 405
