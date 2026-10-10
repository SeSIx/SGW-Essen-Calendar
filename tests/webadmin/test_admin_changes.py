"""Per-event conflict detection, idempotent create and the single retry."""

import pytest

import custom_events
from admin import changes
from admin.fake_store import FakeGitHubStore
from admin.github_store import Author, Conflict, Snapshot
from webadmin.testdata import EVENT_MULTI, EVENT_TIMED

BROKEN = {"id": "kaputt-1", "title": "", "start_date": "2026-13-01"}


def snap(*entries):
    return Snapshot(custom_events.parse(custom_events.serialize(list(entries))), "sha-1")


def edited(event, **changes_):
    return {**event, **changes_}


def test_create_appends():
    new = edited(EVENT_TIMED, id="11111111-2222-4333-8444-555555555555", title="Neu")
    assert changes.create(snap(EVENT_TIMED), new) == [EVENT_TIMED, new]


def test_create_with_same_content_again_is_a_no_op():
    assert changes.create(snap(EVENT_TIMED), dict(EVENT_TIMED)) is None


def test_create_with_reused_id_but_other_content_is_refused():
    with pytest.raises(changes.DuplicateId):
        changes.create(snap(EVENT_TIMED), edited(EVENT_TIMED, title="Anders"))


def test_update_replaces_only_the_target():
    rev = custom_events.event_rev(EVENT_TIMED)
    new = edited(EVENT_TIMED, title="Lehrgang (verschoben)")
    assert changes.update(snap(EVENT_TIMED, EVENT_MULTI), EVENT_TIMED["id"], rev, new) == [new, EVENT_MULTI]


def test_update_to_what_is_already_stored_is_a_no_op():
    # A re-submitted edit: the stored event already equals the new one, whatever the rev says.
    stale_rev = custom_events.event_rev(EVENT_MULTI)
    new = edited(EVENT_TIMED, title="Lehrgang (verschoben)")
    assert changes.update(snap(new, EVENT_MULTI), EVENT_TIMED["id"], stale_rev, dict(new)) is None


def test_update_after_someone_else_changed_it_is_a_conflict():
    rev = custom_events.event_rev(EVENT_TIMED)
    theirs = edited(EVENT_TIMED, location="Hauptbad")
    with pytest.raises(changes.EventConflict) as info:
        changes.update(snap(theirs), EVENT_TIMED["id"], rev, edited(EVENT_TIMED, title="Mein"))
    assert info.value.current == theirs


def test_update_after_someone_deleted_it_is_a_conflict():
    with pytest.raises(changes.EventConflict) as info:
        changes.update(snap(EVENT_MULTI), EVENT_TIMED["id"], custom_events.event_rev(EVENT_TIMED), EVENT_TIMED)
    assert info.value.current is None


def test_keeping_my_version_of_a_deleted_event_recreates_it():
    assert changes.update(snap(EVENT_MULTI), EVENT_TIMED["id"], "", EVENT_TIMED) == [EVENT_MULTI, EVENT_TIMED]


def test_change_to_another_event_is_no_conflict():
    rev = custom_events.event_rev(EVENT_TIMED)
    others_changed = edited(EVENT_MULTI, title="Trainingslager Wedau")
    result = changes.update(snap(EVENT_TIMED, others_changed), EVENT_TIMED["id"], rev,
                            edited(EVENT_TIMED, title="Mein"))
    assert others_changed in result


def test_delete_and_delete_twice():
    rev = custom_events.event_rev(EVENT_TIMED)
    assert changes.delete(snap(EVENT_TIMED, EVENT_MULTI), EVENT_TIMED["id"], rev) == [EVENT_MULTI]
    assert changes.delete(snap(EVENT_MULTI), EVENT_TIMED["id"], rev) is None


def test_delete_of_changed_event_is_a_conflict():
    with pytest.raises(changes.EventConflict):
        changes.delete(snap(edited(EVENT_TIMED, title="X")), EVENT_TIMED["id"],
                       custom_events.event_rev(EVENT_TIMED))


def test_broken_entries_are_carried_along_and_editable():
    s = snap(EVENT_TIMED, BROKEN)
    result = changes.update(s, EVENT_TIMED["id"], custom_events.event_rev(EVENT_TIMED),
                            edited(EVENT_TIMED, title="Mein"))
    assert BROKEN in result
    fixed = {**EVENT_MULTI, "id": "kaputt-1"}
    repaired = changes.update(s, "kaputt-1", custom_events.event_rev(BROKEN), fixed)
    assert fixed in repaired and BROKEN not in repaired


def test_message_and_author():
    assert changes.message("Trainer", "Weihnachtsfeier", "geändert") == "Trainer: „Weihnachtsfeier“ geändert"
    assert changes.message("Trainer", "Zeile\nzwei", "angelegt") == "Trainer: „Zeile zwei“ angelegt"


def test_author_for_unmapped_login_cannot_match_a_github_account():
    author = changes.author_for("trainer", "Trainer")
    assert author == Author("Trainer (SGW-Admin)", "admin+trainer@sgw-essen.local")
    assert changes.author_for("trainer", "Trainer", {"julius": ("J", "j@x.de")}) == author


def test_author_for_mapped_login_uses_the_exact_identity():
    mapping = {"julius": ("Julius Gerecke", "76214201+SeSIx@users.noreply.github.com")}
    assert changes.author_for("julius", "Julius", mapping) == Author(
        "Julius Gerecke", "76214201+SeSIx@users.noreply.github.com")


def test_commit_retries_once_on_conflict(fake_dir):
    store = FakeGitHubStore(fake_dir)
    store.fail["save"].append(Conflict("409"))
    new = edited(EVENT_TIMED, id="11111111-2222-4333-8444-555555555555", title="Neu")
    changes.commit(store, lambda s: changes.create(s, new), "m", Author("A", "a@b"))
    assert store.saves == 1
    assert new in store.load().result.valid


def test_commit_gives_up_after_second_conflict(fake_dir):
    store = FakeGitHubStore(fake_dir)
    store.fail["save"] += [Conflict("409"), Conflict("409")]
    with pytest.raises(changes.SaveConflict):
        changes.commit(store, lambda s: changes.create(s, edited(EVENT_TIMED, id="x-1")), "m", Author("A", "a@b"))


def test_commit_without_change_does_not_write(fake_dir):
    store = FakeGitHubStore(fake_dir)
    changes.commit(store, lambda s: changes.create(s, dict(EVENT_TIMED)), "m", Author("A", "a@b"))
    assert store.saves == 0


def test_shown_author_is_the_app_display_name():
    mapping = {"julius": ("Julius Gerecke", "j@x.de")}
    names = {"julius": "Julius"}.get
    assert changes.shown_author("MaxK (SGW-Admin)", mapping, names) == "MaxK"
    assert changes.shown_author("Julius Gerecke", mapping, names) == "Julius"
    assert changes.shown_author("Fremder", mapping, names) == "Fremder"
    assert changes.shown_author("Julius Gerecke", mapping, {}.get) == "Julius Gerecke"
