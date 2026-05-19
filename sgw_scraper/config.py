BASE_URL = "https://dsvdaten.dsv.de"
CLUBS_URL = f"{BASE_URL}/Modules/WB/Clubs.aspx"

CLUB_NAME = "SG Wasserball Essen"
CLUB_ID = "6638"

SEASON_YEAR = "2025"          # DSV season key; means 2025/2026
SEASON_LABEL = "2025/2026"

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
