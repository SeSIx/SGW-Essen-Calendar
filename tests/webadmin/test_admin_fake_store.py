"""The fake must behave like GitHub where the app depends on it."""

import pytest

import custom_events
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


def test_missing_events_file_is_a_store_error(fake_dir):
    (fake_dir / "custom_events.json").unlink()
    store = FakeGitHubStore(fake_dir)
    with pytest.raises(gs.StoreError) as info:
        store.load()
    assert not isinstance(info.value, FileNotFoundError)
    with pytest.raises(gs.StoreError):
        store.save([EVENT_TIMED], "irgendwas", "m", AUTHOR)


def test_save_leaves_no_temp_files_and_complete_content(fake_dir):
    store = FakeGitHubStore(fake_dir)
    store.save([EVENT_TIMED, EVENT_MULTI], store.load().sha, "m", AUTHOR)
    names = sorted(p.name for p in fake_dir.iterdir())
    teams = ["herren_1", "herren_2", "damen", "u16", "u14", "u12"]
    assert names == sorted(["custom_events.json", ".lock", ".last-commit"]
                           + [f"sgw_essen_{t}.ics" for t in teams])
    content = (fake_dir / "custom_events.json").read_text(encoding="utf-8")
    assert content == custom_events.serialize([EVENT_TIMED, EVENT_MULTI])


def test_save_replaces_the_file_atomically(fake_dir, monkeypatch):
    import os
    from pathlib import Path

    from admin import fake_store

    seen = []
    real_replace = os.replace

    def spy(src, dst):
        # At the moment of the rename the temp file must already be complete,
        # and the target must still hold the old, complete content.
        if Path(dst).name == "custom_events.json":
            seen.append((Path(src).read_text(encoding="utf-8"),
                         Path(dst).read_text(encoding="utf-8")))
        return real_replace(src, dst)

    monkeypatch.setattr(fake_store.os, "replace", spy)
    store = FakeGitHubStore(fake_dir)
    old_text = (fake_dir / "custom_events.json").read_text(encoding="utf-8")
    store.save([EVENT_TIMED], store.load().sha, "m", AUTHOR)
    assert len(seen) == 1
    tmp_text, target_text = seen[0]
    assert tmp_text == custom_events.serialize([EVENT_TIMED])
    assert target_text == old_text
    assert store.load().result.valid[0]["id"] == EVENT_TIMED["id"]


def test_non_ascii_round_trip(fake_dir):
    event = {**EVENT_TIMED, "title": "„Weihnachtsfeier“ in Düsseldorf",
             "location": "Löwenbad Düsseldorf"}
    store = FakeGitHubStore(fake_dir)
    store.save([event], store.load().sha, "Trainer: „Weihnachtsfeier“ angelegt", AUTHOR)
    loaded = store.load().result.valid
    assert loaded[0]["title"] == "„Weihnachtsfeier“ in Düsseldorf"
    assert loaded[0]["location"] == "Löwenbad Düsseldorf"
    assert store.last_commit()[2] == "Trainer: „Weihnachtsfeier“ angelegt"
