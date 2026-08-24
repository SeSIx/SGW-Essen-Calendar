"""Configuration and season arithmetic for the DSV water-polo scraper."""

from datetime import date

BASE_URL = "https://dsvdaten.dsv.de"
CLUBS_URL = f"{BASE_URL}/Modules/WB/Clubs.aspx"

CLUB_NAME = "SG Wasserball Essen"
CLUB_ID = "6638"

# --- Season handling -------------------------------------------------------
# A DSV season key is the starting year: "2025" means the 2025/2026 season.
# Seasons begin in August; cup fixtures of the outgoing season can still be
# scheduled into September/October of the following calendar year, which is why
# both keys are scraped during the changeover window.
SEASON_START_MONTH = 8
CHANGEOVER_MONTHS = (8, 9, 10)


def current_season_key(today: date | None = None) -> str:
    """Return the DSV key of the season that is running on `today`."""
    today = today or date.today()
    year = today.year if today.month >= SEASON_START_MONTH else today.year - 1
    return str(year)


def season_keys(today: date | None = None) -> list[str]:
    """Return every DSV season key worth scraping on `today`, oldest first.

    During the changeover window the previous season is included so that
    fixtures spilling over from it (typically cup rounds) stay in the calendar.
    """
    today = today or date.today()
    current = int(current_season_key(today))
    if today.month in CHANGEOVER_MONTHS:
        return [str(current - 1), str(current)]
    return [str(current)]


def season_label(key: str) -> str:
    return f"{key}/{int(key) + 1}"


# Kept for callers that want a single key (default argument of fetch_club_page).
SEASON_YEAR = current_season_key()
SEASON_LABEL = season_label(SEASON_YEAR)

OUTPUT_DIR = "output"

REQUEST_DELAY = 4.0           # seconds between Game.aspx fetches; DSV throttles aggressively
RATE_LIMIT_RETRIES = 5

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
# Game.aspx returns a 403-equivalent without this header
REFERER_HEADER = f"{BASE_URL}/Modules/WB/Clubs.aspx"
