"""ics.py — write a RFC 5545 VCALENDAR from a per-team SGW Essen SQLite DB."""

import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# RFC 5545 §3.1: lines MUST be ≤75 octets; fold with CRLF + single SPACE.
# We fold on octets (UTF-8 encoded), not characters.
# ---------------------------------------------------------------------------
def _fold(s: str) -> str:
    """Fold a content line so no output line exceeds 75 octets.

    The limit counts the leading SPACE that marks a continuation, so folded
    chunks after the first may only carry 74 octets. Splitting is done on the
    UTF-8 encoding and never inside a multi-byte character.
    """
    encoded = s.encode("utf-8")
    if len(encoded) <= 75:
        return s

    def take(buf: bytes, limit: int) -> tuple[bytes, bytes]:
        """Split off at most `limit` octets without cutting a character in half."""
        if len(buf) <= limit:
            return buf, b""
        cut = limit
        while cut > 0 and (buf[cut] & 0xC0) == 0x80:   # landed on a continuation byte
            cut -= 1
        return buf[:cut], buf[cut:]

    head, rest = take(encoded, 75)
    lines = [head.decode("utf-8")]
    while rest:
        chunk, rest = take(rest, 74)   # 74 + the leading SPACE = 75 octets
        lines.append(chunk.decode("utf-8"))
    return "\r\n ".join(lines)


def _esc(value: str) -> str:
    """Escape text for DESCRIPTION/SUMMARY per RFC 5545 §3.3.11."""
    return (value
            .replace("\\", "\\\\")
            .replace(";", "\\;")
            .replace(",", "\\,")
            .replace("\n", "\\n"))





def write_if_changed(path: str, content: str) -> bool:
    """Write only when something other than DTSTAMP differs. Returns True if written.

    DTSTAMP is derived from the database's last_updated, and a laptop and a CI
    runner keep separate databases — so the same fixtures yield different stamps
    on each machine. Rewriting on that alone makes the two churn against each
    other in git, which is the noise this whole mechanism exists to avoid.
    """
    target = Path(path)
    new = content.encode("utf-8")
    if target.exists():
        def strip_stamps(raw: bytes) -> bytes:
            return b"\r\n".join(
                line for line in raw.split(b"\r\n") if not line.startswith(b"DTSTAMP:")
            )
        if strip_stamps(target.read_bytes()) == strip_stamps(new):
            return False
    target.write_bytes(new)
    return True


def data_dtstamp(conn: sqlite3.Connection) -> str:
    """DTSTAMP derived from the data, not from the clock.

    RFC 5545 defines DTSTAMP as the moment the calendar information was last
    revised. Using wall-clock time would rewrite every VEVENT on every run, so
    the scheduled job would commit an identical calendar twice a day forever.
    Deriving it from the newest row makes output byte-stable until data changes.
    """
    stamps = []
    for table, column in (("games", "last_updated"), ("custom_events", "added_at")):
        try:
            row = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()
        except sqlite3.OperationalError:
            continue  # table absent in this database
        if row and row[0]:
            stamps.append(str(row[0]))
    if not stamps:
        return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    newest = max(stamps)
    try:
        dt = datetime.strptime(newest[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return dt.strftime("%Y%m%dT%H%M%SZ")

BERLIN = ZoneInfo("Europe/Berlin")


def to_utc(date_str: str, time_str: str) -> datetime:
    """Berlin wall-clock time as stored by the DSV, expressed in UTC.

    Every feed Google accepts uses UTC, VALUE=DATE or floating times; a
    VTIMEZONE block with TZID parameters is the one structural trait shared
    only by feeds it rejects. Converting here keeps the output unambiguous
    without shipping a timezone definition.
    """
    naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    return naive.replace(tzinfo=BERLIN).astimezone(UTC)


# The DSV's Game.aspx links answer 302 -> Index.aspx often enough to be useless
# as a bookmark. Probing each one per run is not an option -- seventy requests
# against a host that rate-limits would get the scraper blocked -- so the choice
# is made from the date alone.
LIVE_URL = "https://lizenz.dsv.de/Live.aspx"


def event_url(row: dict, today: date) -> str:
    """The link the event points at: the live scoreboard on match day, the DSV
    page otherwise. The DSV link stays in the description either way."""
    if row.get("game_date") == today.isoformat():
        return LIVE_URL
    return row.get("protocol_url") or row.get("detail_url") or ""


def _build_vevent(row: dict, dtstamp: str, today: date) -> str:
    uid       = f"{row['id']}@sgw-essen.local"
    home      = row["home_team"] or ""
    away      = row["away_team"] or ""
    comp      = row["competition"] or ""
    short_comp = comp if len(comp) <= 34 else comp[:34].rsplit(" ", 1)[0]
    played    = row["status"] == "played"

    # SUMMARY
    if played and row["home_score"] is not None:
        summary = f"{home} {row['home_score']}:{row['away_score']} {away}"
    else:
        summary = f"{home} : {away}"
    summary += f" ({short_comp})"

    # DTSTART / DTEND — all-day if game_time is NULL.
    # If date is also missing (e.g., legacy/imported rows), skip the event entirely.
    date_str = row["game_date"]
    time_str = row["game_time"]
    if not date_str:
        return ""
    all_day  = not time_str

    lines = [
        "BEGIN:VEVENT",
        _fold(f"UID:{uid}"),
        _fold(f"DTSTAMP:{dtstamp}"),
        _fold(f"SUMMARY:{_esc(summary)}"),
        "STATUS:CONFIRMED",
        "TRANSP:OPAQUE",
    ]

    if all_day:
        # Outlook renders all-day events badly without DTEND — use date+1
        start_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        end_date = start_date + timedelta(days=1)
        lines.append(_fold(f"DTSTART;VALUE=DATE:{start_date.strftime('%Y%m%d')}"))
        lines.append(_fold(f"DTEND;VALUE=DATE:{end_date.strftime('%Y%m%d')}"))
    else:
        dt_start = to_utc(date_str, time_str)
        dt_end   = dt_start + timedelta(minutes=90)
        lines.append(_fold(f"DTSTART:{dt_start:%Y%m%dT%H%M%S}Z"))
        lines.append(_fold(f"DTEND:{dt_end:%Y%m%dT%H%M%S}Z"))

    # LOCATION
    venue   = row.get("venue") or ""
    addr    = row.get("venue_address") or ""
    if venue and addr:
        location = f"{venue}, {addr}"
    elif venue:
        location = venue
    elif addr:
        location = addr
    else:
        location = ""
    if location:
        lines.append(_fold(f"LOCATION:{_esc(location)}"))

    # DESCRIPTION (multi-line, folded)
    desc_parts = [f"Wettbewerb: {comp}"]
    if played and row["home_score"] is not None:
        desc_parts.append(f"Ergebnis: {row['home_score']}:{row['away_score']}")
    else:
        desc_parts.append("Ergebnis: geplant")
    refs = " / ".join(r for r in [row.get("referee_1"), row.get("referee_2")] if r)
    if refs:
        desc_parts.append(f"Schiedsrichter: {refs}")
    if row.get("protocol_url"):
        desc_parts.append(f"Spielprotokoll: {row['protocol_url']}")
    if row.get("detail_url"):
        desc_parts.append(f"Details: {row['detail_url']}")
    description = "\\n".join(_esc(p) for p in desc_parts)
    lines.append(_fold(f"DESCRIPTION:{description}"))

    url = event_url(row, today)
    if url:
        lines.append(_fold(f"URL:{url}"))

    lines.append("END:VEVENT")
    return "\r\n".join(lines)


def write_ics(db_path: str, ics_path: str, calendar_name: str = "SGW Essen",
              today: date | None = None) -> int:
    """Return number of events written."""
    today = today or datetime.now(tz=BERLIN).date()
    Path(ics_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    dtstamp = data_dtstamp(conn)
    rows = conn.execute(
        "SELECT * FROM games ORDER BY game_date, game_time"
    ).fetchall()
    conn.close()

    vevents = [v for v in (_build_vevent(dict(r), dtstamp, today) for r in rows) if v]

    cal = "\r\n".join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//SGW Essen//sgw_scraper//DE",
        _fold(f"X-WR-CALNAME:{_esc(calendar_name)}"),
        _fold(f"X-WR-CALDESC:{_esc('Spiele der SG Wasserball Essen, Daten vom DSV')}"),
        "X-WR-TIMEZONE:Europe/Berlin",
        "REFRESH-INTERVAL;VALUE=DURATION:PT12H",
        "X-PUBLISHED-TTL:PT12H",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        *vevents,
        "END:VCALENDAR",
    ]) + "\r\n"

    write_if_changed(ics_path, cal)
    return len(vevents)
