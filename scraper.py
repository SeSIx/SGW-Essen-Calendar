"""
DSV water-polo scraper for SG Wasserball Essen.
Uses plain requests + BeautifulSoup/lxml — no browser automation.
"""

import re
import time
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

from config import (
    CLUBS_URL,
    CLUB_ID,
    SEASON_YEAR,
    RATE_LIMIT_RETRIES,
    REQUEST_DELAY,
    USER_AGENT,
    REFERER_HEADER,
)

# ---------------------------------------------------------------------------
# Session singleton
# ---------------------------------------------------------------------------

_SESSION: requests.Session | None = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        _SESSION = s
    return _SESSION


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(text: str) -> str:
    return " ".join(text.split())


def _hidden_fields(soup: BeautifulSoup) -> dict:
    """Extract ASP.NET ViewState hidden inputs."""
    fields = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        tag = soup.find("input", {"type": "hidden", "name": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields


def _parse_date(raw: str) -> tuple[str, str]:
    """
    Parse 'dd.mm.yy HH:MM' -> ('yyyy-mm-dd', 'HH:MM').
    Returns ('', '') if no match.
    """
    date_str = time_str = ""
    dm = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", raw)
    if dm:
        d, m, y = dm.groups()
        full_year = y if len(y) == 4 else f"20{y}"
        date_str = f"{full_year}-{int(m):02d}-{int(d):02d}"
    tm = re.search(r"(\d{2}:\d{2})", raw)
    if tm:
        time_str = tm.group(1)
    return date_str, time_str


def build_google_maps_url(address: str) -> str:
    if not address:
        return ""
    query = urlencode({"api": "1", "query": address})
    return f"https://www.google.com/maps/search/?{query}"


# ---------------------------------------------------------------------------
# Club page: GET + POST
# ---------------------------------------------------------------------------

def fetch_club_page(season_year: str = SEASON_YEAR) -> str:
    """
    GET Clubs.aspx to harvest hidden fields, then POST with
    season + club + button to get the full schedule table HTML.
    """
    print(f"[Scraper] Fetching club page for season {season_year}, club {CLUB_ID}")
    sess = _session()

    resp = sess.get(CLUBS_URL, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    soup = BeautifulSoup(resp.text, "lxml")

    hidden = _hidden_fields(soup)
    if not hidden.get("__VIEWSTATE"):
        print("[Scraper] WARNING: __VIEWSTATE not found — page structure may have changed")

    payload = {
        **hidden,
        "ClubsSeasonSelect": season_year,
        "ClubsClubsSelect": CLUB_ID,
        "ClubsSubmitButton": "Daten laden",
    }

    post_resp = sess.post(CLUBS_URL, data=payload, timeout=30)
    post_resp.raise_for_status()
    post_resp.encoding = post_resp.apparent_encoding or "utf-8"
    print(f"[Scraper] Club page received ({len(post_resp.text)} bytes)")
    return post_resp.text


# ---------------------------------------------------------------------------
# Parse game list
# ---------------------------------------------------------------------------

def parse_games(html: str, season_year: str = SEASON_YEAR) -> list[dict]:
    """
    Parse the single <table> returned after the club POST.
    Returns one dict per game row.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        print("[Scraper] No table found in club page HTML")
        return []

    games = []
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 5:
            continue

        # Skip header rows
        if cells[0].name == "th" or _clean(cells[0].get_text()).lower() in ("liga", ""):
            continue

        texts = [_clean(c.get_text()) for c in cells]

        # Column 0: competition name lives in span[title]
        liga_span = cells[0].find("span", title=True)
        competition = _clean(liga_span["title"]) if liga_span else texts[0]

        # Column 1: date + time
        game_date, game_time = _parse_date(texts[1])

        # Columns 2 & 3: teams
        home_team = texts[2]
        away_team = texts[3]

        # Column 4: result link or "mehr..."
        info_cell = cells[4]
        link = info_cell.find("a", href=True)
        if not link:
            continue  # no link → no usable detail URL, skip row

        href = link["href"]
        link_text = _clean(link.get_text())

        # Build absolute URL (hrefs on this page are relative to the WB/ dir)
        if href.startswith("http"):
            detail_url = href
        elif href.startswith("/"):
            detail_url = f"https://dsvdaten.dsv.de{href}"
        else:
            detail_url = f"https://dsvdaten.dsv.de/Modules/WB/{href}"

        # Parse URL params to build composite stable ID
        sm = re.search(r"Season=(\d+)", href)
        lm = re.search(r"LeagueID=(\d+)", href)
        gpm = re.search(r"Group=([^&]*)", href)
        gm = re.search(r"GameID=(\d+)", href)

        if not gm:
            continue  # no GameID → cannot build ID, skip

        s = sm.group(1) if sm else season_year
        l = lm.group(1) if lm else "?"
        grp = gpm.group(1) if gpm else ""
        game_id = f"{s}_{l}_{grp}_{gm.group(1)}"

        # Determine status and score from link text
        score_m = re.search(r"(\d+)\s*:\s*(\d+)", link_text)
        if score_m:
            home_score = int(score_m.group(1))
            away_score = int(score_m.group(2))
            status = "played"
        else:
            home_score = away_score = None
            status = "scheduled"

        games.append({
            "id": game_id,
            "competition": competition,
            "game_date": game_date,
            "game_time": game_time,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "status": status,
            "detail_url": detail_url,
            "season": f"{s}/{int(s) + 1}",
        })

    print(f"[Scraper] Parsed {len(games)} games")
    return games


# ---------------------------------------------------------------------------
# Fetch game detail page
# ---------------------------------------------------------------------------

def fetch_game_detail(url: str) -> str | None:
    """
    GET a Game.aspx URL with the required Referer header.
    Retries up to RATE_LIMIT_RETRIES times on 'Rekordtempo' (anti-bot) responses.
    Returns HTML string or None on persistent failure.
    """
    sess = _session()
    headers = {"Referer": REFERER_HEADER}

    for attempt in range(RATE_LIMIT_RETRIES):
        try:
            resp = sess.get(url, headers=headers, timeout=30)
        except requests.RequestException as exc:
            print(f"[Scraper] Network error fetching {url}: {exc}")
            return None

        # Server signals throttling either via HTTP 429 or via a body containing
        # "Rekordtempo". Both need the same exponential backoff treatment.
        if resp.status_code == 429 or (
            resp.status_code == 200 and "Rekordtempo" in resp.text
        ):
            wait = 15 * (2 ** attempt)
            print(f"[Scraper] Rate limited ({resp.status_code}) — sleeping {wait}s (attempt {attempt + 1}/{RATE_LIMIT_RETRIES})")
            time.sleep(wait)
            continue

        # Any other non-success (5xx, 404, …) — skip this game rather than
        # crashing the whole run.
        if resp.status_code >= 400:
            print(f"[Scraper] HTTP {resp.status_code} for {url} — skipping")
            return None

        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

    print(f"[Scraper] Failed to fetch after {RATE_LIMIT_RETRIES} attempts: {url}")
    return None


# ---------------------------------------------------------------------------
# Parse game detail
# ---------------------------------------------------------------------------

def _parse_grunddaten(table) -> dict:
    result: dict = {}
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        label = _clean(cells[0].get_text()).rstrip(":").lower()
        value = _clean(cells[1].get_text())
        if not value or value in ("* nicht vorhanden *", "* keine Einträge *"):
            continue

        if "liga" in label:
            result["league_name"] = value
        elif "spiel-nr" in label:
            result["game_number"] = value
        elif "schwimmbad" in label:
            result["venue"] = value
        elif "bad-adresse" in label:
            result["venue_address"] = value
        elif "schiedsrichter 1" in label:
            result["referee_1"] = value
        elif "schiedsrichter 2" in label:
            result["referee_2"] = value

    return result


def _parse_score_summary(table) -> dict:
    """Extract home_score / away_score from the 'Tore Total' row."""
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        label = _clean(cells[0].get_text()).lower()
        if "tore total" in label:
            try:
                return {
                    "home_score": int(_clean(cells[1].get_text())),
                    "away_score": int(_clean(cells[2].get_text())),
                }
            except ValueError:
                pass
    return {}


def _parse_player_table(table) -> list[dict]:
    """
    Parse roster table. Columns: Nr | Name | Jg | Tore | Q1 | Q2 | Q3 | Q4
    Q-cells contain foul codes: A=Ausschluss, S=Strafwurf.
    """
    players = []
    rows = table.find_all("tr")

    # Find the header row that contains "Nr." or "Nr"
    header_idx = None
    for i, row in enumerate(rows):
        cells_text = [_clean(c.get_text()).lower() for c in row.find_all(["td", "th"])]
        if any(t in ("nr.", "nr") for t in cells_text):
            header_idx = i
            break

    if header_idx is None:
        return []

    for row in rows[header_idx + 1:]:
        cells = [_clean(c.get_text()) for c in row.find_all(["td", "th"])]
        if not cells or not any(cells):
            continue

        # Footer rows: "Kapitän: …", "Trainer: …"
        if any(kw in cells[0].lower() for kw in ("kapit", "trainer", "betreuer")):
            continue

        if len(cells) < 3:
            continue

        cap_number = int(cells[0]) if cells[0].isdigit() else None
        name = cells[1] if len(cells) > 1 else ""
        # Skip rows without a plausible name (must start with an uppercase letter)
        if not name or not re.match(r"[A-ZÜÄÖ]", name):
            continue

        birth_year = None
        if len(cells) > 2 and cells[2].isdigit():
            birth_year = int(cells[2])

        goals = 0
        if len(cells) > 3 and cells[3].isdigit():
            goals = int(cells[3])

        # Q1-Q4 foul cells (indices 4-7)
        fouls = [cells[i] if i < len(cells) else "" for i in range(4, 8)]
        exclusions = fouls.count("A")
        penalty_throws = fouls.count("S")

        players.append({
            "player_name": name,
            "cap_number": cap_number,
            "birth_year": birth_year,
            "goals": goals,
            "exclusions": exclusions,
            "penalty_throws": penalty_throws,
        })

    return players


def _parse_mannschaft_stats(table) -> dict:
    """
    Parse Mannschaftsstatistik table.
    Returns the four keys the DB schema needs.
    Label matching is lenient (lowercased, encoding-tolerant).
    """
    stats: dict = {}
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        # Normalise: lowercase, strip accents best-effort
        label = _clean(cells[0].get_text()).lower()
        raw_val = _clean(cells[1].get_text()).replace("%", "").strip()
        try:
            n = int(raw_val) if raw_val.isdigit() else None
        except ValueError:
            n = None

        # "berzahl" covers both "Überzahl" and its encoding variants
        if "berzahl" in label and "tore" not in label and "quote" not in label:
            stats["powerplay_situations"] = n
        elif "tore in" in label and "berzahl" in label:
            stats["powerplay_goals"] = n
        elif "strafwürfe" in label and "tore" not in label and "quote" not in label:
            stats["penalty_throws"] = n
        elif "tore aus strafwürf" in label:
            stats["penalty_throw_goals"] = n

    return stats


def parse_game_detail(html: str, url: str, essen_team_name: str, home_team: str = "", away_team: str = "") -> dict:
    """
    Parse a Game.aspx HTML page.

    Always returns venue / referee / score fields.
    For player_stats and team_stats: only parses the side whose team name
    matches essen_team_name — opponent data is skipped entirely.

    Returns empty essen_players / essen_team_stats for scheduled games (<7 tables).
    """
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")

    data: dict = {
        "venue": "",
        "venue_address": "",
        "google_maps_url": "",
        "referee_1": "",
        "referee_2": "",
        "home_score": None,
        "away_score": None,
        "game_number": "",
        "protocol_url": "",
        "essen_players": [],
        "essen_team_stats": {},
    }

    # Table 0: Grunddaten
    if len(tables) >= 1:
        gd = _parse_grunddaten(tables[0])
        data.update(gd)
        addr = data.get("venue_address", "")
        if addr:
            data["google_maps_url"] = build_google_maps_url(addr)

    # Table 1: Score summary
    if len(tables) >= 2:
        data.update(_parse_score_summary(tables[1]))

    # Protocol PDF link (only present for played games).
    # DSV uses an ASP.NET __doPostBack JS handler that we can't follow as a
    # plain URL, so we only keep the link if it's an actual http(s) href.
    pdf_link = soup.find("a", string=re.compile(r"Spielprotokoll als PDF", re.I))
    if pdf_link and pdf_link.get("href"):
        href = pdf_link["href"]
        if href and not href.startswith("javascript:"):
            data["protocol_url"] = urljoin(url, href)

    # Tables 2-5 only exist for played games (7 tables total)
    if len(tables) < 7:
        return data

    # Determine which side is the Essen team so we only parse that side.
    # Use caller-supplied names when available; fall back to heading scrape.
    home_team_name = home_team
    away_team_name = away_team
    if not home_team_name and not away_team_name:
        for tag in soup.find_all(["h1", "h2", "h3"]):
            text = _clean(tag.get_text())
            # Page heading format is typically "Heimteam : Auswärtsteam"
            if " : " in text:
                parts = text.split(" : ", 1)
                home_team_name, away_team_name = parts[0].strip(), parts[1].strip()
                break

    essen_is_home = (
        essen_team_name.lower() in home_team_name.lower()
        if home_team_name else False
    )
    essen_is_away = (
        essen_team_name.lower() in away_team_name.lower()
        if away_team_name else False
    )

    # If heading-based detection failed, fall back: check which roster table
    # looks more like an Essen side by looking for "Essen" in the table header
    if not essen_is_home and not essen_is_away:
        for side_idx, table_idx in enumerate((2, 3)):
            header_text = _clean(tables[table_idx].get_text(separator=" ")).lower()
            if "essen" in header_text:
                essen_is_home = (side_idx == 0)
                essen_is_away = (side_idx == 1)
                break

    if essen_is_home:
        data["essen_players"] = _parse_player_table(tables[2])
        data["essen_team_stats"] = _parse_mannschaft_stats(tables[4])
    elif essen_is_away:
        data["essen_players"] = _parse_player_table(tables[3])
        data["essen_team_stats"] = _parse_mannschaft_stats(tables[5])
    else:
        print(f"[Scraper] WARNING: could not determine Essen side for {url}")

    return data
