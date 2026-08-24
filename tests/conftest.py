"""Shared fixtures: real DSV HTML captured 2026-08-24, replayed offline."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

FIXTURES = Path(__file__).parent / "fixtures"

# A played Herren II fixture present in club_page_2025.html and game_detail.html
FIXTURE_GAME_ID = "2025_212__56"


@pytest.fixture(scope="session")
def club_page() -> str:
    return FIXTURES.joinpath("club_page_2025.html").read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def game_detail() -> str:
    return FIXTURES.joinpath("game_detail.html").read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def games(club_page) -> list[dict]:
    import scraper
    return scraper.parse_games(club_page, "2025")
