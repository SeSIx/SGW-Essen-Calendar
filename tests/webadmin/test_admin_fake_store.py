"""The fake must behave like GitHub where the app depends on it."""

import pytest

from admin import github_store as gs
from admin.fake_store import FakeGitHubStore
from webadmin.testdata import EVENT_MULTI, EVENT_TIMED

AUTHOR = gs.Author("Trainer", "admin+trainer@sgw-essen.local")


def test_load_and_save_round_trip(fake_dir):
    store = FakeGitHubStore(fake_dir)
    snap = store.load()
    store.save([EVENT_TIMED], snap.sha, "msg", AUTHOR)
    assert [e["id"] for e in store.load().result.valid] == [EVENT_TIMED["id"]]
    assert store.saves == 1
    assert store.last_commit() == ("Trainer", "admin+trainer@sgw-essen.local", "msg")
    assert store.last_author() == "Trainer"


def test_stale_sha_is_a_conflict(fake_dir):
    store = FakeGitHubStore(fake_dir)
    old = store.load().sha
    store.save([EVENT_MULTI], old, "first", AUTHOR)
    with pytest.raises(gs.Conflict):
        store.save([EVENT_TIMED], old, "second", AUTHOR)


def test_two_instances_share_the_files(fake_dir):
    a, b = FakeGitHubStore(fake_dir), FakeGitHubStore(fake_dir)
    a.save([EVENT_TIMED], a.load().sha, "m", AUTHOR)
    assert len(b.load().result.valid) == 1


def test_injected_failures(fake_dir):
    store = FakeGitHubStore(fake_dir)
    store.fail["load"].append(gs.Unauthorized("x"))
    with pytest.raises(gs.Unauthorized):
        store.load()
    assert store.unauthorized is True
    store.load()
    assert store.unauthorized is False


def test_read_text_and_missing_file(fake_dir):
    store = FakeGitHubStore(fake_dir)
    assert "BEGIN:VCALENDAR" in store.read_text("sgw_essen_damen.ics")
    with pytest.raises(gs.StoreError):
        store.read_text("gibt_es_nicht.ics")


def test_corrupt_file(fake_dir):
    (fake_dir / "custom_events.json").write_text("{kaputt", encoding="utf-8")
    with pytest.raises(gs.CorruptFile):
        FakeGitHubStore(fake_dir).load()
