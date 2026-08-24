"""ics.py — write a RFC 5545 VCALENDAR from a per-team SGW Essen SQLite DB."""

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path


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


# Minimal but valid VTIMEZONE for Europe/Berlin (CET/CEST).
# Offsets and RRULE per IANA tzdata; enough for any date in the modern era.
_VTIMEZONE = """\
BEGIN:VTIMEZONE\r
TZID:Europe/Berlin\r
BEGIN:STANDARD\r
TZOFFSETFROM:+0200\r
TZOFFSETTO:+0100\r
TZNAME:CET\r
DTSTART:19701025T030000\r
RRULE:FREQ=YEARLY;BYDAY=-1SU;BYMONTH=10\r
END:STANDARD\r
BEGIN:DAYLIGHT\r
TZOFFSETFROM:+0100\r
TZOFFSETTO:+0200\r
TZNAME:CEST\r
DTSTART:19700329T020000\r
RRULE:FREQ=YEARLY;BYDAY=-1SU;BYMONTH=3\r
END:DAYLIGHT\r
END:VTIMEZONE"""




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

def _build_vevent(row: dict, dtstamp: str) -> str:
    uid       = f"{row['id']}@sgw-essen.local"
    home      = row["home_team"] or ""
    away      = row["away_team"] or ""
    comp      = row["competition"] or ""
    short_comp = comp[:30]
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
        _fold(f"STATUS:{'CONFIRMED' if played else 'TENTATIVE'}"),
    ]

    if all_day:
        # Outlook renders all-day events badly without DTEND — use date+1
        start_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        end_date = start_date + timedelta(days=1)
        lines.append(_fold(f"DTSTART;VALUE=DATE:{start_date.strftime('%Y%m%d')}"))
        lines.append(_fold(f"DTEND;VALUE=DATE:{end_date.strftime('%Y%m%d')}"))
    else:
        dt_start = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        dt_end   = dt_start + timedelta(minutes=90)
        lines.append(_fold(f"DTSTART;TZID=Europe/Berlin:{dt_start:%Y%m%dT%H%M%S}"))
        lines.append(_fold(f"DTEND;TZID=Europe/Berlin:{dt_end:%Y%m%dT%H%M%S}"))

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

    # URL — prefer protocol PDF, then detail page
    url = row.get("protocol_url") or row.get("detail_url") or ""
    if url:
        lines.append(_fold(f"URL:{url}"))

    lines.append("END:VEVENT")
    return "\r\n".join(lines)


def write_ics(db_path: str, ics_path: str, calendar_name: str = "SGW Essen") -> int:
    """Return number of events written."""
    Path(ics_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    dtstamp = data_dtstamp(conn)
    rows = conn.execute(
        "SELECT * FROM games ORDER BY game_date, game_time"
    ).fetchall()
    conn.close()

    vevents = [v for v in (_build_vevent(dict(r), dtstamp) for r in rows) if v]

    cal = "\r\n".join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//SGW Essen//sgw_scraper//DE",
        _fold(f"X-WR-CALNAME:{_esc(calendar_name)}"),
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        _VTIMEZONE,
        *vevents,
        "END:VCALENDAR",
    ]) + "\r\n"

    write_if_changed(ics_path, cal)
    return len(vevents)
