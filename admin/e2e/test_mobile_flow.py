"""Phone-sized walk through the real container (fake GitHub): invite, login, create, edit, delete, filter."""

import json
import os
from pathlib import Path

import pytest
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import expect, sync_playwright

BASE = "http://localhost:8099"
SHOTS = Path(os.environ.get("SGW_E2E_SHOTS", "admin/e2e/screenshots"))
FAKE = Path(os.environ.get("SGW_E2E_FAKE_DIR", "/nonexistent"))
TITLE = "E2E Trainingslager"
PASSWORD = "E2E-Wasserball-Essen-2026"


@pytest.fixture(scope="module")
def page():
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2,
                                      is_mobile=True, has_touch=True, locale="de-DE",
                                      timezone_id="Europe/Berlin")
        yield context.new_page()
        browser.close()


def stored():
    return json.loads((FAKE / "custom_events.json").read_text(encoding="utf-8"))


def ours():
    return [e for e in stored() if isinstance(e, dict) and e.get("title") == TITLE]


def shot(page, name):
    SHOTS.mkdir(parents=True, exist_ok=True)
    path = str(SHOTS / f"{name}.png")
    try:
        page.screenshot(path=path, full_page=True)
    except PlaywrightError:
        try:
            page.screenshot(path=path, full_page=True)
        except PlaywrightError:
            page.screenshot(path=path, full_page=False)


def test_full_flow(page):
    invite = os.environ["SGW_E2E_INVITE"]
    assert invite.startswith(BASE + "/einladung/")

    page.goto(invite)
    expect(page.get_by_text("Hallo Julius, wähle dein Passwort")).to_be_visible()
    shot(page, "01-einladung")
    page.fill("input[name=password]", PASSWORD)
    page.fill("input[name=password2]", PASSWORD)
    page.get_by_role("button", name="Passwort speichern").click()
    expect(page.get_by_text("Hallo Julius 👋")).to_be_visible()
    shot(page, "02-agenda")

    page.locator("summary").click()
    page.get_by_role("button", name="Abmelden", exact=True).click()
    expect(page.locator("input[name=login]")).to_be_visible()
    shot(page, "03-login")
    page.fill("input[name=login]", "julius")
    page.fill("input[name=password]", PASSWORD)
    page.get_by_role("button", name="Anmelden").click()
    expect(page.locator("a.fab")).to_be_visible()

    page.locator("a.fab").click()
    page.fill("input[name=title]", TITLE)
    page.check("input[name=all_day]")
    page.check("input[name=multi_day]")
    expect(page.locator("input[name=start_time]")).to_be_hidden()
    page.fill("input[name=start_date]", "2026-12-04")
    page.fill("input[name=end_date]", "2026-12-06")
    page.fill("input[name=location]", "Sportpark Wedau")
    shot(page, "04-formular")
    page.get_by_role("button", name="Sichern").click()
    expect(page.get_by_text("Gespeichert ✓ – Kalender in ca. 1 Min. aktuell")).to_be_visible()
    shot(page, "05-gespeichert")
    [created] = ours()
    assert (created["start_time"], created["end_date"]) == (None, "2026-12-06")

    page.locator("a.item", has_text=TITLE).click()
    page.fill("input[name=location]", "Sportpark Duisburg-Wedau")
    page.get_by_role("button", name="Sichern").click()
    expect(page.get_by_text("Gespeichert ✓")).to_be_visible()
    assert [e["location"] for e in ours()] == ["Sportpark Duisburg-Wedau"]

    page.locator("a.item", has_text=TITLE).click()
    page.get_by_role("link", name="Termin löschen").click()
    expect(page.get_by_text("Termin wirklich löschen?")).to_be_visible()
    shot(page, "06-loeschen")
    page.get_by_role("button", name="Löschen", exact=True).click()
    expect(page.get_by_text("Gelöscht ✓ – Kalender in ca. 1 Min. aktuell")).to_be_visible()
    assert ours() == []

    page.get_by_role("link", name="Damen", exact=True).click()
    assert "teams=" in page.url
    expect(page.locator("a.item", has_text="Damen").first).to_be_visible()
    shot(page, "07-filter-damen")

    while page.locator("a.chip.on").count():
        page.locator("a.chip.on").first.click()
        page.wait_for_load_state()
    expect(page.get_by_text("Keine Mannschaft gewählt – nur Vereinstermine")).to_be_visible()
    expect(page.locator("li[data-team]:visible")).to_have_count(0)
    shot(page, "08-filter-keine")


def test_instant_filter_survives_reload(page):
    page.goto(BASE + "/?teams=herren_1")
    page.evaluate("window.__sameDocument = true")  # lost if the browser loads a new document
    games = page.locator("li[data-team]:visible")
    before = games.count()
    with page.expect_response(lambda r: r.url.endswith("/filter") and r.request.method == "POST") as saved:
        page.get_by_role("link", name="U16", exact=True).click()
    assert saved.value.status == 204
    assert page.evaluate("window.__sameDocument") is True, "toggling a chip must not reload the page"
    assert "teams=herren_1,u16" in page.url.replace("%2C", ",")
    assert page.locator("a.chip.on", has_text="U16").count() == 1
    assert games.count() >= before

    while page.locator("a.chip.on").count():
        with page.expect_response(lambda r: r.url.endswith("/filter")):
            page.locator("a.chip.on").first.click()
    assert page.evaluate("window.__sameDocument") is True
    expect(page.get_by_text("Keine Mannschaft gewählt – nur Vereinstermine")).to_be_visible()
    expect(page.locator("li[data-team]:visible")).to_have_count(0)
    expect(page.locator("a.item.event").first).to_be_visible()  # club dates stay

    with page.expect_response(lambda r: r.url.endswith("/filter")):
        page.get_by_role("link", name="Damen", exact=True).click()
    page.goto(BASE + "/")  # no query: the choice comes from the cookie
    expect(page.locator("a.chip.on")).to_have_count(1)
    expect(page.locator("a.chip.on")).to_have_text("Damen")
    expect(page.get_by_text("Keine Mannschaft gewählt")).to_be_hidden()
    expect(page.locator("li[data-team]:visible").first).to_contain_text("Damen")
    shot(page, "09-filter-sofort")


def test_pool_quick_picks_and_map_link(page):
    page.goto(BASE + "/termin/neu")
    expect(page.get_by_role("link", name="Karte öffnen")).to_have_count(0)
    page.get_by_role("button", name="Thurmfeld").click()
    expect(page.locator("input[name=location]")).to_have_value(
        "Sportbad Thurmfeld, Reckhammerweg 84, 45141 Essen")
    page.get_by_role("button", name="Hesse").click()
    expect(page.locator("input[name=location]")).to_have_value(
        'Freibad Dellwig "Hesse", Scheppmannskamp 6, 45357 Essen')
    box = page.get_by_role("button", name="LZ Rüttenscheid").bounding_box()
    assert box["height"] >= 44
    shot(page, "10-orte")

    page.goto(BASE + "/?teams=herren_1,herren_2,damen,u16,u14,u12")
    page.locator("li[data-team]:visible a.item.game").first.click()
    link = page.get_by_role("link", name="Karte öffnen")
    expect(link).to_have_attribute("target", "_blank")
    expect(link).to_have_attribute("rel", "noopener noreferrer")
    assert "google.com/maps/search/?api=1&query=" in link.get_attribute("href")
