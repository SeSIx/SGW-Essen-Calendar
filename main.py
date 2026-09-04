"""main.py — orchestrator for the SGW Essen DSV water-polo scraper.

Scrapes every relevant DSV season, stores fixtures per team in SQLite, and
writes iCalendar files. Exits non-zero when a run produces no upcoming
fixtures at all, which is how a silently dead calendar gets noticed.
"""

import argparse
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

import combine
import config
import db
import ics
import scraper

OUTPUT_DIR = Path(__file__).parent / config.OUTPUT_DIR

ESSEN_SUBSTR = "wasserball essen"

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
    print(f"\n{'Team':<30} {'Total':>6} {'Played':>8} {'Scheduled':>10} {'Upcoming':>9}")
    print("-" * 68)
    for db_path in dbs:
        if db_path.name in ("custom_events.db", "sgw_termine.db"):
            continue
        conn = sqlite3.connect(db_path)
        total  = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
        played = conn.execute("SELECT COUNT(*) FROM games WHERE status='played'").fetchone()[0]
        sched  = conn.execute("SELECT COUNT(*) FROM games WHERE status='scheduled'").fetchone()[0]
        upcoming = db.count_upcoming(conn)
        conn.close()
        print(f"{db_path.stem:<30} {total:>6} {played:>8} {sched:>10} {upcoming:>9}")
    print()


def _collect_games(season_keys: list[str]) -> dict[str, list[dict]]:
    """Fetch every season's club page and bucket Essen fixtures by team slug."""
    by_id: dict[str, dict] = {}
    for key in season_keys:
        html = scraper.fetch_club_page(key)
        games = scraper.parse_games(html, key)
        print(f"[Main] Season {config.season_label(key)}: {len(games)} game(s)")
        for game in games:
            by_id[game["id"]] = game  # DSV ids are unique; later seasons win

    buckets: dict[str, list[dict]] = {}
    for game in by_id.values():
        essen_name = _essen_side(game)
        if essen_name is None:
            continue  # DSV returns full group schedules; non-Essen fixtures are expected
        slug = db.slug_for_team(essen_name, game.get("competition", ""))
        game["_essen_team_name"] = essen_name
        buckets.setdefault(slug, []).append(game)
    return buckets


def cmd_scrape(season_keys: list[str], fetch_details: bool, write_ics_files: bool,
               force_details: bool = False) -> int:
    """Run the full pipeline. Returns a process exit code."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today()

    buckets = _collect_games(season_keys)
    print(f"[Main] Bucketed into {len(buckets)} team(s): {', '.join(sorted(buckets))}")

    fetched = skipped = failed = 0
    conns: dict[str, sqlite3.Connection] = {}
    try:
        for slug, bucket in buckets.items():
            conn = db.init_db(str(OUTPUT_DIR / f"{slug}.db"))
            conns[slug] = conn
            for game in bucket:
                db.upsert_game(conn, _clean_game_dict(game))
            conn.commit()
            print(f"[Main] {slug}: upserted {len(bucket)} game(s)")

        if fetch_details:
            queue = [
                (slug, game)
                for slug, bucket in buckets.items()
                for game in bucket
                if game.get("detail_url")
                and (force_details or db.needs_detail(conns[slug], game, today))
            ]
            total_with_url = sum(
                1 for b in buckets.values() for g in b if g.get("detail_url")
            )
            skipped = total_with_url - len(queue)
            print(f"[Main] Detail pages: {len(queue)} to fetch, {skipped} already complete")

            for i, (slug, game) in enumerate(queue):
                if i > 0:
                    time.sleep(config.REQUEST_DELAY)
                detail_html = scraper.fetch_game_detail(game["detail_url"])
                if detail_html is None:
                    failed += 1
                    continue

                detail = scraper.parse_game_detail(
                    detail_html, game["detail_url"], game["_essen_team_name"],
                    home_team=game.get("home_team", ""),
                    away_team=game.get("away_team", ""),
                )
                merged = {**_clean_game_dict(game)}
                for k, v in detail.items():
                    if k in _GAME_COLS_SET and v not in (None, "", []):
                        merged[k] = v
                db.upsert_game(conns[slug], merged)

                if detail.get("essen_players"):
                    db.replace_player_stats(conns[slug], game["id"], detail["essen_players"])
                if detail.get("essen_team_stats"):
                    db.replace_team_stats(conns[slug], game["id"], detail["essen_team_stats"])
                fetched += 1
            for conn in conns.values():
                conn.commit()
    finally:
        for conn in conns.values():
            conn.close()

    ics_counts: dict[str, int] = {}
    if write_ics_files:
        for slug in buckets:
            ics_path = Path(__file__).parent / f"{slug}.ics"  # repo root — tracked by git
            ics_counts[slug] = ics.write_ics(
                str(OUTPUT_DIR / f"{slug}.db"), str(ics_path),
                calendar_name=f"SGW Essen — {slug}",
            )
        print("[Main] Building sgw_termine.db + sgw_termine.ics...")
        combine.build_termine_db(OUTPUT_DIR)
        combine.write_termine_ics(OUTPUT_DIR)
        combine.write_vereinstermine_ics(OUTPUT_DIR)

    return _report(buckets, ics_counts, fetched, skipped, failed, today)


def _report(buckets, ics_counts, fetched, skipped, failed, today) -> int:
    """Print a run summary and decide the exit code."""
    print(f"\n{'Team':<26} {'Games':>6} {'ICS':>5} {'Upcoming':>9} {'No date':>8}")
    print("-" * 60)
    total_upcoming = 0
    total_undated: list[str] = []
    for slug in sorted(buckets):
        conn = sqlite3.connect(OUTPUT_DIR / f"{slug}.db")
        upcoming = db.count_upcoming(conn, today)
        undated = db.undated_game_ids(conn)
        conn.close()
        total_upcoming += upcoming
        total_undated += undated
        print(f"{slug:<26} {len(buckets[slug]):>6} {str(ics_counts.get(slug, '-')):>5} "
              f"{upcoming:>9} {len(undated):>8}")

    print(f"\n[Main] Detail pages: {fetched} fetched, {skipped} skipped, {failed} failed")
    if total_undated:
        print(f"[Main] WARNING: {len(total_undated)} game(s) have no date and are absent "
              f"from every calendar: {', '.join(sorted(total_undated))}")

    if total_upcoming == 0:
        print("\n[Main] ERROR: no upcoming fixtures in any calendar. Either the season "
              "has not been published yet or the parser broke — the calendar is dead "
              "until this is resolved.")
        return 2

    print(f"[Main] OK: {total_upcoming} upcoming fixture(s) across all teams\n")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="SGW Essen DSV water-polo scraper")
    parser.add_argument("--season", default=None,
                        help="Comma-separated DSV season keys (default: auto — "
                             f"currently {','.join(config.season_keys())})")
    parser.add_argument("--summary", action="store_true",
                        help="Print row counts from existing DBs; skip scraping")
    parser.add_argument("--no-ics", action="store_true", help="Skip ICS file generation")
    parser.add_argument("--no-details", action="store_true",
                        help="Skip Game.aspx detail fetches (fast mode)")
    parser.add_argument("--force-details", action="store_true",
                        help="Refetch every detail page, even ones already stored")
    args = parser.parse_args()

    if args.summary:
        cmd_summary()
        return

    keys = [k.strip() for k in args.season.split(",")] if args.season else config.season_keys()
    print(f"[Main] Seasons: {', '.join(config.season_label(k) for k in keys)}")

    sys.exit(cmd_scrape(
        season_keys=keys,
        fetch_details=not args.no_details,
        write_ics_files=not args.no_ics,
        force_details=args.force_details,
    ))


if __name__ == "__main__":
    main()
