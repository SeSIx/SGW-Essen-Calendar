"""Creating, editing and deleting events through the form, including the awkward cases."""

import html
import json
import re

import pytest

import custom_events
from admin.github_store import Author, Conflict, CorruptFile, RateLimited, Unauthorized, Unavailable
from webadmin.testdata import BASE, EVENT_MULTI, EVENT_TIMED, csrf_of

BROKEN = {"id": "kaputt-1", "title": "Feier", "start_date": "2026-13-01"}
FORM = {"title": "Weihnachtsfeier", "start_date": "2026-12-19", "start_time": "18:00",
        "end_date": "", "end_time": "23:00", "location": "Vereinsheim", "description": ""}
TIMED_FORM = {"title": "Mein Titel", "start_date": "2026-11-21", "start_time": "09:00", "end_time": "15:00"}
KAPITAEN = Author("Kapitän", "admin+kapitaen@sgw-essen.local")


def hidden(page) -> dict:
    found = re.findall(r'<input type="hidden" name="([^"]+)" value="([^"]*)">', page.get_data(as_text=True))
    return {name: html.unescape(value) for name, value in found}


def submit(client, path, page, *, with_form=True, **overrides):
    data = {**hidden(page), **(FORM if with_form else {}), **overrides}
    data["csrf_token"] = csrf_of(page)
    return client.post(path, data={k: v for k, v in data.items() if v is not None},
                       headers={"Origin": BASE, "Sec-Fetch-Site": "same-origin"})


def stored(fake_dir):
    return json.loads((fake_dir / "custom_events.json").read_text(encoding="utf-8"))


def titles(fake_dir):
    return [e.get("title") for e in stored(fake_dir) if isinstance(e, dict)]


def edit_path(event):
    return f"/termin/{event['id']}"


def test_new_form_has_a_fresh_id(user_client):
    page = user_client.get("/termin/neu")
    assert page.status_code == 200
    assert re.fullmatch(r"[0-9a-f-]{36}", hidden(page)["id"])


def test_create_timed_event(user_client, store, fake_dir):
    r = submit(user_client, "/termin/neu", user_client.get("/termin/neu"))
    assert r.status_code == 302 and r.headers["Location"].endswith("/?ok=gespeichert")
    [new] = [e for e in stored(fake_dir) if e["title"] == "Weihnachtsfeier"]
    assert (new["start_time"], new["end_time"], new["end_date"]) == ("18:00", "23:00", None)
    assert store.last_commit() == ("Julius", "admin+julius@sgw-essen.local", "Julius: „Weihnachtsfeier“ angelegt")


def test_create_multi_day_all_day_event(user_client, fake_dir):
    submit(user_client, "/termin/neu", user_client.get("/termin/neu"),
           all_day="1", multi_day="1", end_date="2026-12-21", start_time="", end_time="")
    [new] = [e for e in stored(fake_dir) if e["title"] == "Weihnachtsfeier"]
    assert (new["start_time"], new["end_time"], new["end_date"]) == (None, None, "2026-12-21")


def test_resubmitting_the_same_new_form_creates_one_event(user_client, store, fake_dir):
    page = user_client.get("/termin/neu")
    first = submit(user_client, "/termin/neu", page)
    second = submit(user_client, "/termin/neu", page)
    assert first.status_code == second.status_code == 302
    assert titles(fake_dir).count("Weihnachtsfeier") == 1
    assert store.saves == 1


@pytest.mark.parametrize("overrides, message", [
    ({"start_time": ""}, "Uhrzeit fehlt"),
    ({"multi_day": "1", "end_date": ""}, "Enddatum fehlt"),
])
def test_missing_time_or_end_date_is_a_field_error(user_client, fake_dir, overrides, message):
    before = stored(fake_dir)
    r = submit(user_client, "/termin/neu", user_client.get("/termin/neu"), **overrides)
    body = r.get_data(as_text=True)
    assert r.status_code == 400 and message in body and 'class="field-error"' in body
    assert 'value="Weihnachtsfeier"' in body, "input survives"
    assert stored(fake_dir) == before


def test_rule_violation_from_the_model_is_reported(user_client, fake_dir):
    before = stored(fake_dir)
    r = submit(user_client, "/termin/neu", user_client.get("/termin/neu"), start_time="20:00", end_time="18:00")
    body = r.get_data(as_text=True)
    assert r.status_code == 400 and ('class="field-error"' in body or 'class="error"' in body)
    assert stored(fake_dir) == before


def test_title_with_markup_is_escaped(user_client):
    r = submit(user_client, "/termin/neu", user_client.get("/termin/neu"),
               title="<script>alert(1)</script>", start_time="")
    body = r.get_data(as_text=True)
    assert "<script>alert(1)</script>" not in body and "&lt;script&gt;" in body


def test_edit_prefills_and_saves(user_client, store, fake_dir):
    page = user_client.get(edit_path(EVENT_TIMED))
    body = page.get_data(as_text=True)
    assert 'value="Kampfrichter-Lehrgang"' in body and 'value="09:00"' in body
    r = submit(user_client, edit_path(EVENT_TIMED), page, **{**TIMED_FORM, "title": "Kampfrichter-Lehrgang II"})
    assert r.status_code == 302
    assert "Kampfrichter-Lehrgang II" in titles(fake_dir) and "Kampfrichter-Lehrgang" not in titles(fake_dir)
    assert store.last_commit()[2] == "Julius: „Kampfrichter-Lehrgang II“ geändert"


def test_conflict_when_someone_else_changed_the_same_event(user_client, store, fake_dir):
    page = user_client.get(edit_path(EVENT_TIMED))
    store.save([{**EVENT_TIMED, "location": "Hauptbad"}, EVENT_MULTI], store.load().sha, "x", KAPITAEN)
    r = submit(user_client, edit_path(EVENT_TIMED), page, **TIMED_FORM)
    body = r.get_data(as_text=True)
    assert r.status_code == 409
    assert "inzwischen von Kapitän geändert" in body and "Hauptbad" in body and "Mein Titel" in body
    keep = submit(user_client, edit_path(EVENT_TIMED), r, **TIMED_FORM)
    assert keep.status_code == 302 and "Mein Titel" in titles(fake_dir)


def test_change_to_another_event_does_not_block(user_client, store, fake_dir):
    page = user_client.get(edit_path(EVENT_TIMED))
    store.save([EVENT_TIMED, {**EVENT_MULTI, "title": "Trainingslager Wedau"}], store.load().sha, "x", KAPITAEN)
    r = submit(user_client, edit_path(EVENT_TIMED), page, **TIMED_FORM)
    assert r.status_code == 302
    assert {"Mein Titel", "Trainingslager Wedau"} <= set(titles(fake_dir))


def test_edit_of_deleted_event_offers_to_recreate(user_client, store, fake_dir):
    page = user_client.get(edit_path(EVENT_TIMED))
    store.save([EVENT_MULTI], store.load().sha, "x", KAPITAEN)
    r = submit(user_client, edit_path(EVENT_TIMED), page, **TIMED_FORM)
    assert r.status_code == 409 and "gelöscht" in r.get_data(as_text=True)
    again = submit(user_client, edit_path(EVENT_TIMED), r, **TIMED_FORM)
    assert again.status_code == 302 and "Mein Titel" in titles(fake_dir)


def test_delete_needs_confirmation_and_get_changes_nothing(user_client, store, fake_dir):
    path = f"/termin/{EVENT_TIMED['id']}/loeschen"
    confirm = user_client.get(path)
    assert confirm.status_code == 200 and "Termin wirklich löschen?" in confirm.get_data(as_text=True)
    assert "Kampfrichter-Lehrgang" in titles(fake_dir) and store.saves == 0
    r = submit(user_client, path, confirm, with_form=False)
    assert r.status_code == 302 and r.headers["Location"].endswith("/?ok=geloescht")
    assert "Kampfrichter-Lehrgang" not in titles(fake_dir)
    assert store.last_commit()[2] == "Julius: „Kampfrichter-Lehrgang“ gelöscht"
    assert submit(user_client, path, confirm, with_form=False).status_code == 302, "second tap is harmless"


def test_delete_after_someone_changed_it_asks_again(user_client, store, fake_dir):
    path = f"/termin/{EVENT_TIMED['id']}/loeschen"
    confirm = user_client.get(path)
    store.save([{**EVENT_TIMED, "location": "Hauptbad"}, EVENT_MULTI], store.load().sha, "x", KAPITAEN)
    r = submit(user_client, path, confirm, with_form=False)
    assert r.status_code == 409 and "Trotzdem löschen" in r.get_data(as_text=True)
    assert submit(user_client, path, r, with_form=False).status_code == 302
    assert "Kampfrichter-Lehrgang" not in titles(fake_dir)


def test_saving_keeps_broken_entries_untouched(user_client, fake_dir):
    (fake_dir / "custom_events.json").write_text(custom_events.serialize([EVENT_TIMED, BROKEN]), encoding="utf-8")
    assert submit(user_client, "/termin/neu", user_client.get("/termin/neu")).status_code == 302
    assert BROKEN in stored(fake_dir)
    assert "Weihnachtsfeier" in titles(fake_dir)


def test_broken_entry_with_id_can_be_repaired(user_client, fake_dir):
    (fake_dir / "custom_events.json").write_text(custom_events.serialize([BROKEN]), encoding="utf-8")
    page = user_client.get("/termin/kaputt-1")
    assert page.status_code == 200 and 'value="Feier"' in page.get_data(as_text=True)
    r = submit(user_client, "/termin/kaputt-1", page, title="Feier", start_date="2026-12-01")
    assert r.status_code == 302
    [fixed] = stored(fake_dir)
    assert (fixed["id"], fixed["start_date"]) == ("kaputt-1", "2026-12-01")


def test_unauthorized_on_save_keeps_input_and_blocks_saving(user_client, store):
    page = user_client.get("/termin/neu")
    store.fail["save"].append(Unauthorized("401"))
    r = submit(user_client, "/termin/neu", page)
    body = r.get_data(as_text=True)
    assert r.status_code == 503 and "Zugang zu GitHub abgelaufen" in body and 'value="Weihnachtsfeier"' in body
    again = user_client.get("/termin/neu").get_data(as_text=True)
    assert "Speichern ist gesperrt" in again and "disabled" in again


def test_unavailable_on_save_keeps_input(user_client, store):
    page = user_client.get("/termin/neu")
    store.fail["save"].append(Unavailable("down"))
    r = submit(user_client, "/termin/neu", page)
    body = r.get_data(as_text=True)
    assert r.status_code == 503 and "Deine Eingaben sind noch da" in body and 'value="Weihnachtsfeier"' in body


def test_two_conflicts_in_a_row_ask_to_save_again(user_client, store):
    page = user_client.get("/termin/neu")
    store.fail["save"] += [Conflict("409"), Conflict("409")]
    r = submit(user_client, "/termin/neu", page)
    assert r.status_code == 409 and "bitte nochmal speichern" in r.get_data(as_text=True)


def test_unknown_or_malformed_ids_are_404(user_client):
    for path in ("/termin/gibt-es-nicht", "/termin/" + "x" * 65, "/termin/a_b", "/termin/gibt-es-nicht/loeschen"):
        assert user_client.get(path).status_code == 404, path


def test_anonymous_is_sent_to_login(client):
    assert client.get("/termin/neu").headers["Location"].endswith("/login")


# --- fix round 1 ---

NASTY = {"title": 'A & B <i> "q" \'s\'', "description": "Zeile 1\nZeile 2", "start_date": "2026-11-21",
         "start_time": "", "all_day": "1", "multi_day": "1", "end_date": "2026-11-23", "end_time": ""}


def test_end_date_without_multi_day_is_an_error(user_client, fake_dir):
    before = stored(fake_dir)
    r = submit(user_client, "/termin/neu", user_client.get("/termin/neu"), end_date="2026-12-21")
    body = r.get_data(as_text=True)
    assert r.status_code == 400 and "Mehrtägig einschalten oder Ende leeren" in body
    assert stored(fake_dir) == before


def test_times_with_all_day_are_an_error(user_client, fake_dir):
    before = stored(fake_dir)
    r = submit(user_client, "/termin/neu", user_client.get("/termin/neu"), all_day="1")
    body = r.get_data(as_text=True)
    assert r.status_code == 400 and "Ganztägig ausschalten oder Uhrzeiten leeren" in body
    assert stored(fake_dir) == before


def test_js_like_submission_with_hidden_fields_omitted_saves(user_client, fake_dir):
    r = submit(user_client, "/termin/neu", user_client.get("/termin/neu"),
               all_day="1", start_time=None, end_time=None, end_date=None)
    assert r.status_code == 302
    [new] = [e for e in stored(fake_dir) if e["title"] == "Weihnachtsfeier"]
    assert new["start_time"] is None


def test_duplicate_id_with_other_content_redirects_to_edit_page(user_client, fake_dir):
    page = user_client.get("/termin/neu")
    event_id = hidden(page)["id"]
    submit(user_client, "/termin/neu", page)
    r = submit(user_client, "/termin/neu", page, title="Anderer Titel")
    assert r.status_code == 303
    assert r.headers["Location"].endswith(f"/termin/{event_id}?hinweis=schon-gespeichert")
    assert titles(fake_dir).count("Weihnachtsfeier") == 1 and "Anderer Titel" not in titles(fake_dir)
    assert "bereits gespeichert" in user_client.get(r.headers["Location"]).get_data(as_text=True)


def _assert_nasty_saved(fake_dir, event_id):
    [saved] = [e for e in stored(fake_dir) if e["id"] == event_id]
    assert saved["title"] == NASTY["title"] and saved["description"] == NASTY["description"]
    assert (saved["start_time"], saved["end_date"]) == (None, "2026-11-23")


def test_keep_my_version_after_conflict_stores_exact_input(user_client, store, fake_dir):
    page = user_client.get(edit_path(EVENT_TIMED))
    store.save([{**EVENT_TIMED, "location": "Hauptbad"}, EVENT_MULTI], store.load().sha, "x", KAPITAEN)
    r = submit(user_client, edit_path(EVENT_TIMED), page, **NASTY)
    assert r.status_code == 409
    assert submit(user_client, edit_path(EVENT_TIMED), r, with_form=False).status_code == 302
    _assert_nasty_saved(fake_dir, EVENT_TIMED["id"])


def test_recreate_after_deletion_stores_exact_input(user_client, store, fake_dir):
    page = user_client.get(edit_path(EVENT_TIMED))
    store.save([EVENT_MULTI], store.load().sha, "x", KAPITAEN)
    r = submit(user_client, edit_path(EVENT_TIMED), page, **NASTY)
    assert r.status_code == 409
    assert submit(user_client, edit_path(EVENT_TIMED), r, with_form=False).status_code == 302
    _assert_nasty_saved(fake_dir, EVENT_TIMED["id"])


def test_delete_commit_message_uses_stored_title(user_client, store):
    path = f"/termin/{EVENT_TIMED['id']}/loeschen"
    confirm = user_client.get(path)
    submit(user_client, path, confirm, with_form=False, title="Gefälscht")
    assert store.last_commit()[2] == "Julius: „Kampfrichter-Lehrgang“ gelöscht"


def test_delete_blocked_while_unauthorized(user_client, store, fake_dir):
    path = f"/termin/{EVENT_TIMED['id']}/loeschen"
    confirm = user_client.get(path)
    store.unauthorized = True
    assert "disabled" in user_client.get(path).get_data(as_text=True)
    store.unauthorized = True
    r = submit(user_client, path, confirm, with_form=False)
    assert r.status_code == 503 and "Kampfrichter-Lehrgang" in titles(fake_dir)


@pytest.mark.parametrize("exc, text", [(RateLimited("429"), "Zu viele Anfragen"),
                                       (CorruptFile("bad"), "beschädigt")])
def test_other_store_problems_on_save(user_client, store, exc, text):
    page = user_client.get("/termin/neu")
    store.fail["save"].append(exc)
    r = submit(user_client, "/termin/neu", page)
    assert r.status_code == 503 and text in r.get_data(as_text=True)


def test_delete_under_store_error_and_save_conflict(user_client, store, fake_dir):
    path = f"/termin/{EVENT_TIMED['id']}/loeschen"
    confirm = user_client.get(path)
    store.fail["save"].append(Unavailable("down"))
    r = submit(user_client, path, confirm, with_form=False)
    assert r.status_code == 503 and "nicht erreichbar" in r.get_data(as_text=True)
    store.fail["save"] += [Conflict("409"), Conflict("409")]
    r = submit(user_client, path, confirm, with_form=False)
    assert r.status_code == 409 and "bitte nochmal speichern" in r.get_data(as_text=True)
    assert "Kampfrichter-Lehrgang" in titles(fake_dir)


def test_head_on_delete_page_changes_nothing(user_client, store, fake_dir):
    assert user_client.head(f"/termin/{EVENT_TIMED['id']}/loeschen").status_code == 200
    assert store.saves == 0 and "Kampfrichter-Lehrgang" in titles(fake_dir)


def test_resubmitting_the_same_edit_saves_once_and_never_conflicts(user_client, store, fake_dir):
    page = user_client.get(edit_path(EVENT_TIMED))
    first = submit(user_client, edit_path(EVENT_TIMED), page, **TIMED_FORM)
    second = submit(user_client, edit_path(EVENT_TIMED), page, **TIMED_FORM)
    assert (first.status_code, second.status_code) == (302, 302)
    assert second.headers["Location"].endswith("/?ok=gespeichert")
    assert store.saves == 1 and "Mein Titel" in titles(fake_dir)


def test_delete_loads_the_file_once_per_attempt(user_client, store, monkeypatch):
    path = f"/termin/{EVENT_TIMED['id']}/loeschen"
    confirm = user_client.get(path)
    loads = []
    real = store.load
    monkeypatch.setattr(store, "load", lambda: loads.append(1) or real())
    assert submit(user_client, path, confirm, with_form=False).status_code == 302
    assert len(loads) == 1
    assert store.last_commit()[2] == "Julius: „Kampfrichter-Lehrgang“ gelöscht"


def test_delete_pages_carry_no_title_field(user_client, store):
    path = f"/termin/{EVENT_TIMED['id']}/loeschen"
    confirm = user_client.get(path)
    assert "title" not in hidden(confirm)
    store.save([{**EVENT_TIMED, "location": "Hauptbad"}, EVENT_MULTI], store.load().sha, "x", KAPITAEN)
    conflict = submit(user_client, path, confirm, with_form=False)
    assert conflict.status_code == 409 and "title" not in hidden(conflict)


def test_conflict_page_blocks_saving_while_token_is_invalid(user_client, store, monkeypatch):
    page = user_client.get(edit_path(EVENT_TIMED))
    store.save([{**EVENT_TIMED, "location": "Hauptbad"}, EVENT_MULTI], store.load().sha, "x", KAPITAEN)
    # Looking up who changed it hits GitHub, which now rejects the token.
    monkeypatch.setattr(store, "last_author", lambda: setattr(store, "unauthorized", True))
    r = submit(user_client, edit_path(EVENT_TIMED), page, **TIMED_FORM)
    assert r.status_code == 409
    assert re.search(r'<button type="submit" class="primary" disabled>', r.get_data(as_text=True))


def test_post_with_malformed_id_is_rejected(user_client, store):
    page = user_client.get("/termin/neu")
    assert submit(user_client, "/termin/a_b", page).status_code in (400, 404)
    assert submit(user_client, "/termin/a_b/loeschen", page, with_form=False).status_code in (400, 404)
    assert submit(user_client, "/termin/neu", page, id="a_b").status_code in (400, 404)
    assert store.saves == 0
