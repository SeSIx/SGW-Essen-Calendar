"""Pool quick-picks in the event form and the Maps link."""

import html
import re

import pytest

from admin import places
from webadmin.testdata import EVENT_TIMED

ADDRESSES = {
    "Hesse": 'Freibad Dellwig "Hesse", Scheppmannskamp 6, 45357 Essen',
    "Thurmfeld": "Sportbad Thurmfeld, Reckhammerweg 84, 45141 Essen",
    "LZ Rüttenscheid": "Schwimmleistungszentrum Essen, von-Einem-Straße 77, 45130 Essen",
}


def text(resp):
    return resp.get_data(as_text=True)


def test_place_constants():
    assert dict(places.POOLS) == ADDRESSES


def test_maps_url_escapes_everything():
    url = places.maps_url('Halle "A" & Co, Müllerstraße 1')
    assert url == ("https://www.google.com/maps/search/?api=1&query="
                   "Halle%20%22A%22%20%26%20Co%2C%20M%C3%BCllerstra%C3%9Fe%201")


@pytest.mark.parametrize("path", ["/termin/neu"])
def test_form_has_quick_pick_buttons_and_datalist(user_client, path):
    body = text(user_client.get(path))
    buttons = re.findall(r'<button type="button" class="[^"]*place"[^>]*data-place="([^"]*)"[^>]*>([^<]*)</button>', body)
    assert {label: html.unescape(addr) for addr, label in buttons} == ADDRESSES
    assert re.search(r'<input name="location"[^>]*list="places"', body)
    options = re.findall(r'<option value="([^"]*)"', body.split('<datalist id="places">')[1].split("</datalist>")[0])
    assert [html.unescape(o) for o in options] == list(ADDRESSES.values())


def test_no_maps_link_on_new_form(user_client):
    assert "Karte öffnen" not in text(user_client.get("/termin/neu"))


def test_edit_form_maps_link(user_client, fake_dir):
    import custom_events
    ev = {**EVENT_TIMED, "location": 'Freibad Dellwig "Hesse", A&B, Müll'}
    (fake_dir / "custom_events.json").write_text(custom_events.serialize([ev]), encoding="utf-8")
    body = text(user_client.get(f"/termin/{ev['id']}"))
    link = re.search(r'<a [^>]*href="([^"]*)"[^>]*>Karte öffnen</a>', body)
    assert link
    assert html.unescape(link.group(1)) == places.maps_url(ev["location"])
    tag = re.search(r'<a [^>]*>Karte öffnen</a>', body).group(0)
    assert 'target="_blank"' in tag and 'rel="noopener noreferrer"' in tag


def test_edit_form_without_location_has_no_link(user_client, fake_dir):
    import custom_events
    ev = {**EVENT_TIMED, "location": None}
    (fake_dir / "custom_events.json").write_text(custom_events.serialize([ev]), encoding="utf-8")
    assert "Karte öffnen" not in text(user_client.get(f"/termin/{ev['id']}"))


def test_game_page_maps_link(user_client):
    body = text(user_client.get("/spiel/2025_1_A_1"))
    link = re.search(r'<a [^>]*href="([^"]*)"[^>]*>Karte öffnen</a>', body)
    assert html.unescape(link.group(1)) == places.maps_url("Sportbad Thurmfeld, Essen")
    assert 'rel="noopener noreferrer"' in body
