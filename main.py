"""main.py — orchestrator for the SGW Essen DSV water-polo scraper."""

import argparse
import sqlite3
import time
from pathlib import Path

import combine
import config
import db
import ics
import scraper

OUTPUT_DIR = Path(__file__).parent / config.OUTPUT_DIR

ESSEN_SUBSTR = "wasserball essen"

# Columns main.py is responsible for populating from the detail page
_DETAIL_COLS = {
    "venue", "venue_address", "google_maps_url",
    "referee_1", "referee_2",
    "home_score", "away_score",
    "game_number", "protocol_url",
}
_GAME_COLS_SET = set(db._GAME_COLS)


def _essen_side(game: dict) -> str | None:
    """Return the Essen team name from a game dict, or None if not found."""
    for side in ("home_team", "away_team"):
        name = game.get(side) or ""
        if ESSEN_SUBSTR in name.lower():
            return name
    return None


def _clean_game_dict(game: dict) -> dict:
    """Strip helper keys not in db._GAME_COLS before upserting."""
    return {k: v for k, v in game.items() if k in _GAME_COLS_SET}


def cmd_summary() -> None:
    if not OUTPUT_DIR.exists():
        print("[Main] output/ directory not found — nothing to summarise")
        return
    dbs = sorted(OUTPUT_DIR.glob("*.db"))
    if not dbs:
        print("[Main] No .db files found in output/")
        return
    print(f"\n{'Team':<30} {'Total':>6} {'Played':>8} {'Scheduled':>10}")
    print("-" * 58)
    for db_path in dbs:
        slug = db_path.stem
        conn = sqlite3.connect(db_path)
        total    = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
        played   = conn.execute("SELECT COUNT(*) FROM games WHERE status='played'").fetchone()[0]
        sched    = conn.execute("SELECT COUNT(*) FROM games WHERE status='scheduled'").fetchone()[0]
        conn.close()
        print(f"{slug:<30} {total:>6} {played:>8} {sched:>10}")
    print()


def cmd_scrape(season_year: str, fetch_details: bool, write_ics_files: bool) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Fetch + parse ---
    html = scraper.fetch_club_page(season_year)
    games = scraper.parse_games(html, season_year)

    # --- Bucket by slug, tag Essen team name on each game ---
    buckets: dict[str, list[dict]] = {}
    for game in games:
        essen_name = _essen_side(game)
        if essen_name is None:
            print(f"[Main] WARNING: no Essen side in game {game.get('id')} — skipping")
            continue
        slug = db.slug_for_team(essen_name, game.get("competition", ""))
        game["_essen_team_name"] = essen_name
        buckets.setdefault(slug, []).append(game)

    print(f"[Main] Bucketed into {len(buckets)} team(s): {', '.join(sorted(buckets))}")

    # --- Init DBs and upsert game stubs ---
    conns: dict[str, sqlite3.Connection] = {}
    try:
        for slug, bucket in buckets.items():
            db_path = OUTPUT_DIR / f"{slug}.db"
            conn = db.init_db(str(db_path))
            conns[slug] = conn
            for game in bucket:
                db.upsert_game(conn, _clean_game_dict(game))
            print(f"[Main] {slug}: upserted {len(bucket)} game(s)")

        # --- Fetch details for played games ---
        if fetch_details:
            detail_queue = [
                (slug, game)
                for slug, bucket in buckets.items()
                for game in bucket
                if game.get("status") == "played" and game.get("detail_url")
            ]
            print(f"[Main] Fetching details for {len(detail_queue)} played game(s)...")
            for i, (slug, game) in enumerate(detail_queue):
                if i > 0:
                    time.sleep(config.REQUEST_DELAY)
                url = game["detail_url"]
                game_id = game["id"]
                essen_name = game["_essen_team_name"]

                detail_html = scraper.fetch_game_detail(url)
                if detail_html is None:
                    continue

                detail = scraper.parse_game_detail(
                    detail_html, url, essen_name,
                    home_team=game.get("home_team", ""),
                    away_team=game.get("away_team", ""),
                )

                # Merge detail fields into game dict (only known DB cols) and upsert
                merged = {**_clean_game_dict(game)}
                for k, v in detail.items():
                    if k in _GAME_COLS_SET and v not in (None, "", []):
                        merged[k] = v
                db.upsert_game(conns[slug], merged)

                if detail.get("essen_players"):
                    db.replace_player_stats(conns[slug], game_id, detail["essen_players"])

                if detail.get("essen_team_stats"):
                    db.replace_team_stats(conns[slug], game_id, detail["essen_team_stats"])
    finally:
        # Guarantee SQLite connections close even on mid-run exceptions
        for conn in conns.values():
            conn.close()

    # --- Write ICS files ---
    ics_counts: dict[str, int] = {}
    if write_ics_files:
        for slug in buckets:
            db_path = OUTPUT_DIR / f"{slug}.db"
            ics_path = OUTPUT_DIR / f"{slug}.ics"
            count = ics.write_ics(
                str(db_path), str(ics_path),
                calendar_name=f"SGW Essen — {slug}",
            )
            ics_counts[slug] = count
            print(f"[Main] {slug}.ics: {count} event(s)")

    # --- Build combined sgw_termine.db + sgw_termine.ics ---
    if write_ics_files:
        print("[Main] Building sgw_termine.db + sgw_termine.ics...")
        combine.build_termine_db(OUTPUT_DIR)
        combine.write_termine_ics(OUTPUT_DIR)

    # --- Final summary ---
    print(f"\n{'Team':<30} {'Games':>6} {'ICS events':>12}")
    print("-" * 52)
    for slug, bucket in sorted(buckets.items()):
        ics_n = ics_counts.get(slug, "-")
        print(f"{slug:<30} {len(bucket):>6} {str(ics_n):>12}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="SGW Essen DSV water-polo scraper")
    parser.add_argument("--season",     default=config.SEASON_YEAR,
                        help=f"DSV season key (default: {config.SEASON_YEAR})")
    parser.add_argument("--summary",    action="store_true",
                        help="Print row counts from existing DBs; skip scraping")
    parser.add_argument("--no-ics",     action="store_true",
                        help="Skip ICS file generation")
    parser.add_argument("--no-details", action="store_true",
                        help="Skip Game.aspx detail fetches (fast mode)")
    args = parser.parse_args()

    if args.summary:
        cmd_summary()
        return

    cmd_scrape(
        season_year=args.season,
        fetch_details=not args.no_details,
        write_ics_files=not args.no_ics,
    )


if __name__ == "__main__":
    main()
