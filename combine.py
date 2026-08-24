"""combine.py — build sgw_termine.db + sgw_termine.ics from Herren I + II + custom events.

Usage:
  python combine.py [--output-dir OUTPUT_DIR] [--add-event] [--list-events]

Custom events are stored in custom_events.db next to the output directory.
They are merged into sgw_termine.db and exported to sgw_termine.ics together
with Herren I and Herren II fixtures.
"""

import argparse
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import config
from ics import _VTIMEZONE, _esc, _fold

OUTPUT_DIR = Path(__file__).parent / config.OUTPUT_DIR
CUSTOM_EVENTS_DB = OUTPUT_DIR / "custom_events.db"

# Source DBs that feed into sgw_termine
SOURCE_SLUGS = ("sgw_essen_herren_1", "sgw_essen_herren_2")

_TERMINE_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS games (
    id              TEXT PRIMARY KEY,
    season          TEXT,
    competition     TEXT NOT NULL,
    game_number     TEXT,
    game_date       TEXT,
    game_time       TEXT,
    home_team       TEXT,
    away_team       TEXT,
    home_score      INTEGER,
    away_score      INTEGER,
    venue           TEXT,
    venue_address   TEXT,
    google_maps_url TEXT,
    referee_1       TEXT,
    referee_2       TEXT,
    status          TEXT,
    detail_url      TEXT,
    protocol_url    TEXT,
    first_seen      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS custom_events (
    id          TEXT PRIMARY KEY,      -- stable UUID
    title       TEXT NOT NULL,
    start_date  TEXT NOT NULL,         -- yyyy-mm-dd
    start_time  TEXT,                  -- HH:MM or NULL (all-day)
    end_date    TEXT,                  -- yyyy-mm-dd or NULL (same as start)
    end_time    TEXT,                  -- HH:MM or NULL
    location    TEXT,
    description TEXT,
    added_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_CUSTOM_EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    start_date  TEXT NOT NULL,
    start_time  TEXT,
    end_date    TEXT,
    end_time    TEXT,
    location    TEXT,
    description TEXT,
    added_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_GAME_COLS = (
    "id", "season", "competition", "game_number", "game_date", "game_time",
    "home_team", "away_team", "home_score", "away_score", "venue",
    "venue_address", "google_maps_url", "referee_1", "referee_2",
    "status", "detail_url", "protocol_url",
)


def _init_custom_events_db() -> sqlite3.Connection:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(CUSTOM_EVENTS_DB))
    conn.executescript(_CUSTOM_EVENTS_SCHEMA)
    conn.commit()
    return conn


def _init_termine_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_TERMINE_SCHEMA)
    conn.commit()
    return conn


def _upsert_game(conn: sqlite3.Connection, row: dict) -> None:
    values = [row.get(c) for c in _GAME_COLS]
    placeholders = ", ".join("?" * len(_GAME_COLS))
    col_list = ", ".join(_GAME_COLS)
    update_cols = [c for c in _GAME_COLS if c not in ("id", "first_seen")]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    update_clause += ", last_updated = CURRENT_TIMESTAMP"
    conn.execute(
        f"INSERT INTO games ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {update_clause}",
        values,
    )


def _sync_custom_events(termine_conn: sqlite3.Connection) -> None:
    if not CUSTOM_EVENTS_DB.exists():
        return
    src = sqlite3.connect(str(CUSTOM_EVENTS_DB))
    src.row_factory = sqlite3.Row
    rows = src.execute("SELECT * FROM events").fetchall()
    src.close()
    for r in rows:
        r = dict(r)
        termine_conn.execute(
            "INSERT INTO custom_events (id, title, start_date, start_time, "
            "end_date, end_time, location, description, added_at) "
            "VALUES (:id, :title, :start_date, :start_time, "
            ":end_date, :end_time, :location, :description, :added_at) "
            "ON CONFLICT(id) DO UPDATE SET "
            "title=excluded.title, start_date=excluded.start_date, "
            "start_time=excluded.start_time, end_date=excluded.end_date, "
            "end_time=excluded.end_time, location=excluded.location, "
            "description=excluded.description",
            r,
        )


def build_termine_db(output_dir: Path) -> tuple[int, int]:
    """Return (game_count, custom_event_count)."""
    termine_path = output_dir / "sgw_termine.db"
    termine_conn = _init_termine_db(str(termine_path))

    game_count = 0
    for slug in SOURCE_SLUGS:
        src_path = output_dir / f"{slug}.db"
        if not src_path.exists():
            print(f"[Combine] WARNING: {src_path} not found — skipping")
            continue
        src = sqlite3.connect(str(src_path))
        src.row_factory = sqlite3.Row
        rows = src.execute("SELECT * FROM games").fetchall()
        src.close()
        for row in rows:
            _upsert_game(termine_conn, dict(row))
            game_count += 1
        print(f"[Combine] {slug}: {len(rows)} game(s) merged")

    _sync_custom_events(termine_conn)
    custom_count = termine_conn.execute("SELECT COUNT(*) FROM custom_events").fetchone()[0]

    termine_conn.commit()
    termine_conn.close()
    print(f"[Combine] sgw_termine.db: {game_count} games, {custom_count} custom event(s)")
    return game_count, custom_count


# ---------------------------------------------------------------------------
# ICS generation for sgw_termine (games + custom events)
# ---------------------------------------------------------------------------

def _build_game_vevent(row: dict, dtstamp: str) -> str:
    uid = f"{row['id']}@sgw-essen.local"
    home = row["home_team"] or ""
    away = row["away_team"] or ""
    comp = row["competition"] or ""
    played = row["status"] == "played"

    if played and row["home_score"] is not None:
        summary = f"{home} {row['home_score']}:{row['away_score']} {away}"
    else:
        summary = f"{home} : {away}"
    summary += f" ({comp[:30]})"

    date_str = row["game_date"]
    time_str = row["game_time"]
    if not date_str:
        return ""

    lines = [
        "BEGIN:VEVENT",
        _fold(f"UID:{uid}"),
        _fold(f"DTSTAMP:{dtstamp}"),
        _fold(f"SUMMARY:{_esc(summary)}"),
        _fold(f"STATUS:{'CONFIRMED' if played else 'TENTATIVE'}"),
    ]

    if not time_str:
        start_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        end_date = start_date + timedelta(days=1)
        lines.append(_fold(f"DTSTART;VALUE=DATE:{start_date.strftime('%Y%m%d')}"))
        lines.append(_fold(f"DTEND;VALUE=DATE:{end_date.strftime('%Y%m%d')}"))
    else:
        dt_start = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        dt_end = dt_start + timedelta(minutes=90)
        lines.append(_fold(f"DTSTART;TZID=Europe/Berlin:{dt_start:%Y%m%dT%H%M%S}"))
        lines.append(_fold(f"DTEND;TZID=Europe/Berlin:{dt_end:%Y%m%dT%H%M%S}"))

    venue = row.get("venue") or ""
    addr = row.get("venue_address") or ""
    location = f"{venue}, {addr}" if venue and addr else (venue or addr)
    if location:
        lines.append(_fold(f"LOCATION:{_esc(location)}"))

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
    lines.append(_fold(f"DESCRIPTION:{'\\n'.join(_esc(p) for p in desc_parts)}"))

    url = row.get("protocol_url") or row.get("detail_url") or ""
    if url:
        lines.append(_fold(f"URL:{url}"))

    lines.append("END:VEVENT")
    return "\r\n".join(lines)


def _build_custom_vevent(row: dict, dtstamp: str) -> str:
    uid = f"custom-{row['id']}@sgw-essen.local"
    lines = [
        "BEGIN:VEVENT",
        _fold(f"UID:{uid}"),
        _fold(f"DTSTAMP:{dtstamp}"),
        _fold(f"SUMMARY:{_esc(row['title'])}"),
        "STATUS:CONFIRMED",
    ]

    date_str = row["start_date"]
    time_str = row.get("start_time")
    end_date_str = row.get("end_date") or date_str
    end_time_str = row.get("end_time")

    if not time_str:
        start_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date() + timedelta(days=1)
        lines.append(_fold(f"DTSTART;VALUE=DATE:{start_date.strftime('%Y%m%d')}"))
        lines.append(_fold(f"DTEND;VALUE=DATE:{end_date.strftime('%Y%m%d')}"))
    else:
        dt_start = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        if end_time_str:
            dt_end = datetime.strptime(f"{end_date_str} {end_time_str}", "%Y-%m-%d %H:%M")
        else:
            dt_end = dt_start + timedelta(hours=2)
        lines.append(_fold(f"DTSTART;TZID=Europe/Berlin:{dt_start:%Y%m%dT%H%M%S}"))
        lines.append(_fold(f"DTEND;TZID=Europe/Berlin:{dt_end:%Y%m%dT%H%M%S}"))

    if row.get("location"):
        lines.append(_fold(f"LOCATION:{_esc(row['location'])}"))
    if row.get("description"):
        lines.append(_fold(f"DESCRIPTION:{_esc(row['description'])}"))

    lines.append("END:VEVENT")
    return "\r\n".join(lines)


def write_termine_ics(output_dir: Path) -> int:
    termine_path = output_dir / "sgw_termine.db"
    ics_path = output_dir.parent / "sgw_termine.ics"

    dtstamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    conn = sqlite3.connect(str(termine_path))
    conn.row_factory = sqlite3.Row

    game_rows = conn.execute("SELECT * FROM games ORDER BY game_date, game_time").fetchall()
    custom_rows = conn.execute("SELECT * FROM custom_events ORDER BY start_date, start_time").fetchall()
    conn.close()

    vevents = []
    for r in game_rows:
        v = _build_game_vevent(dict(r), dtstamp)
        if v:
            vevents.append(v)
    for r in custom_rows:
        vevents.append(_build_custom_vevent(dict(r), dtstamp))

    cal = "\r\n".join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//SGW Essen//sgw_scraper//DE",
        _fold(f"X-WR-CALNAME:{_esc('SGW Essen — Termine')}"),
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        _VTIMEZONE,
        *vevents,
        "END:VCALENDAR",
    ]) + "\r\n"

    ics_path.write_bytes(cal.encode("utf-8"))
    print(f"[Combine] sgw_termine.ics: {len(vevents)} event(s) written to {ics_path}")
    return len(vevents)


# ---------------------------------------------------------------------------
# CLI: add-event / list-events
# ---------------------------------------------------------------------------

def cmd_add_event() -> None:
    conn = _init_custom_events_db()
    print("Add custom event (Ctrl+C to cancel)\n")
    title = input("Title: ").strip()
    if not title:
        print("Title required.")
        return
    start_date = input("Start date (yyyy-mm-dd): ").strip()
    start_time = input("Start time (HH:MM, or blank for all-day): ").strip() or None
    end_date = input(f"End date (yyyy-mm-dd, or blank = {start_date}): ").strip() or None
    end_time = input("End time (HH:MM, or blank): ").strip() or None
    location = input("Location (or blank): ").strip() or None
    description = input("Description (or blank): ").strip() or None

    event_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO events (id, title, start_date, start_time, end_date, end_time, location, description) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (event_id, title, start_date, start_time, end_date, end_time, location, description),
    )
    conn.commit()
    conn.close()
    print(f"[Combine] Added custom event '{title}' ({event_id})")


def cmd_list_events() -> None:
    if not CUSTOM_EVENTS_DB.exists():
        print("[Combine] No custom events yet.")
        return
    conn = sqlite3.connect(str(CUSTOM_EVENTS_DB))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM events ORDER BY start_date, start_time").fetchall()
    conn.close()
    if not rows:
        print("[Combine] No custom events.")
        return
    print(f"\n{'ID':<36}  {'Title':<30}  {'Date':<12}  Location")
    print("-" * 90)
    for r in rows:
        print(f"{r['id']:<36}  {r['title']:<30}  {r['start_date']:<12}  {r['location'] or ''}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build sgw_termine.db + sgw_termine.ics")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR),
                        help=f"Directory with per-team DBs (default: {OUTPUT_DIR})")
    parser.add_argument("--add-event", action="store_true",
                        help="Interactively add a custom event to custom_events.db")
    parser.add_argument("--list-events", action="store_true",
                        help="List all custom events")
    args = parser.parse_args()

    if args.add_event:
        cmd_add_event()
        return
    if args.list_events:
        cmd_list_events()
        return

    out = Path(args.output_dir)
    build_termine_db(out)
    write_termine_ics(out)


if __name__ == "__main__":
    main()
