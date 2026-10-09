"""The scheduled workflow is what publishes the calendars; these checks pin the
parts of it that the admin app relies on and that must not change by accident."""

from pathlib import Path

import combine

WORKFLOW = (Path(__file__).resolve().parent.parent
            / ".github" / "workflows" / "update-calendar.yml").read_text(encoding="utf-8")


def _step(name: str) -> str:
    for block in WORKFLOW.split("\n      - name: ")[1:]:
        if block.startswith(name):
            return block
    raise AssertionError(f"step {name!r} not found")


def test_editing_club_dates_on_main_triggers_a_run():
    assert "  push:\n    branches: [main]\n    paths:\n      - custom_events.json\n" in WORKFLOW
    assert "  schedule:\n" in WORKFLOW and "  workflow_dispatch:\n" in WORKFLOW


def test_scrape_step_is_unchanged_but_skipped_on_push():
    step = _step("Scrape DSV and rebuild calendars")
    assert "if: github.event_name != 'push'" in step
    assert "python main.py 2>&1 | tee run.log" in step
    assert 'echo "exit_code=${PIPESTATUS[0]}" >> "$GITHUB_OUTPUT"' in step


def test_push_run_only_recombines():
    step = _step("Rebuild calendars from cached data")
    assert "if: github.event_name == 'push'" in step
    assert "python combine.py" in step
    assert "python main.py" in step, "full scrape when the cache is gone"


def test_rebuild_requires_every_source_db():
    step = _step("Rebuild calendars from cached data")
    for slug in combine.SOURCE_SLUGS:
        assert f"[ -f output/{slug}.db ]" in step, slug


def test_alarm_never_fires_for_push_runs():
    """The scrape step is skipped on push, so its exit_code output is empty —
    and '' != '0' would otherwise open a false scraper alarm."""
    step = _step("Raise alarm")
    assert "if: github.event_name != 'push' && steps.scrape.outputs.exit_code != '0'" in step


def test_commit_and_legacy_branch_steps_still_run_on_every_event():
    for name in ("Commit changed calendars", "Keep the legacy branch on the default branch"):
        assert "\n        if:" not in _step(name).split("\n        run:")[0], name
