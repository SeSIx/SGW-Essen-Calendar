"""Season selection — the reason the calendar silently ran dry in July 2026.

DSV season key "2025" means the 2025/2026 season, which starts in autumn and
runs into the following summer. Cup fixtures can spill over into September of
the *next* calendar year, so during the changeover both keys must be scraped.
"""

from datetime import date

import pytest

from config import season_keys


@pytest.mark.parametrize(
    "today,expected",
    [
        # Changeover window: the 2025/26 cup overhang (21.09.2026) must survive
        # while 2026/27 fixtures are being published.
        (date(2026, 8, 21), ["2025", "2026"]),
        (date(2026, 9, 15), ["2025", "2026"]),
        (date(2026, 10, 31), ["2025", "2026"]),
        # Mid-season: only the running season is worth fetching.
        (date(2026, 11, 1), ["2026"]),
        (date(2027, 3, 12), ["2026"]),
        (date(2026, 5, 1), ["2025"]),
        (date(2026, 7, 31), ["2025"]),
        # A new season starts in August.
        (date(2027, 8, 1), ["2026", "2027"]),
    ],
)
def test_season_keys(today, expected):
    assert season_keys(today) == expected


def test_keys_are_strings_because_the_dsv_url_takes_strings():
    assert all(isinstance(k, str) for k in season_keys(date(2026, 8, 21)))


def test_default_uses_today():
    assert season_keys() == season_keys(date.today())
