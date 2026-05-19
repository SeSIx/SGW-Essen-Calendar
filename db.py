import sqlite3
from pathlib import Path

_SCHEMA = """
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

CREATE TABLE IF NOT EXISTS player_stats (
    game_id        TEXT NOT NULL REFERENCES games(id) ON DELETE CASCADE,
    player_name    TEXT NOT NULL,
    cap_number     INTEGER,
    birth_year     INTEGER,
    goals          INTEGER DEFAULT 0,
    exclusions     INTEGER DEFAULT 0,
    penalty_throws INTEGER DEFAULT 0,
    PRIMARY KEY (game_id, player_name)
);

CREATE TABLE IF NOT EXISTS team_stats (
    game_id                TEXT PRIMARY KEY REFERENCES games(id) ON DELETE CASCADE,
    powerplay_goals        INTEGER,
    powerplay_situations   INTEGER,
    penalty_throw_goals    INTEGER,
    penalty_throws         INTEGER
);

CREATE VIEW IF NOT EXISTS v_games AS
SELECT game_date, game_time, competition, home_team, away_team,
       CASE WHEN home_score IS NULL THEN '—'
            ELSE home_score || ':' || away_score END AS result,
       venue, referee_1 || ' / ' || referee_2 AS referees,
       google_maps_url, status
FROM games ORDER BY game_date, game_time;

-- Aggregated player stats across all competitions (one row per player)
CREATE VIEW IF NOT EXISTS v_player_totals AS
SELECT
    player_name,
    SUM(goals)          AS total_goals,
    SUM(exclusions)     AS total_exclusions,
    SUM(penalty_throws) AS total_penalty_throws,
    COUNT(DISTINCT game_id) AS games_played
FROM player_stats
GROUP BY player_name
ORDER BY total_goals DESC, player_name;

-- Aggregated player stats per competition (one row per player per competition)
-- Filter with: SELECT * FROM v_player_by_competition WHERE competition = 'NRW Verbandsliga ...'
CREATE VIEW IF NOT EXISTS v_player_by_competition AS
SELECT
    g.competition,
    ps.player_name,
    SUM(ps.goals)          AS goals,
    SUM(ps.exclusions)     AS exclusions,
    SUM(ps.penalty_throws) AS penalty_throws,
    COUNT(DISTINCT ps.game_id) AS games_played
FROM player_stats ps
JOIN games g ON g.id = ps.game_id
GROUP BY g.competition, ps.player_name
ORDER BY g.competition, goals DESC, ps.player_name;

-- Aggregated team stats per competition (Essen-only)
CREATE VIEW IF NOT EXISTS v_team_by_competition AS
SELECT
    g.competition,
    COUNT(DISTINCT ts.game_id) AS games_with_stats,
    SUM(ts.powerplay_goals)     AS pp_goals,
    SUM(ts.powerplay_situations) AS pp_situations,
    ROUND(100.0 * SUM(ts.powerplay_goals) / NULLIF(SUM(ts.powerplay_situations), 0), 1) AS pp_pct,
    SUM(ts.penalty_throw_goals) AS pt_goals,
    SUM(ts.penalty_throws)      AS pt_situations,
    ROUND(100.0 * SUM(ts.penalty_throw_goals) / NULLIF(SUM(ts.penalty_throws), 0), 1) AS pt_pct
FROM team_stats ts
JOIN games g ON g.id = ts.game_id
GROUP BY g.competition;
"""

# Columns the caller supplies; first_seen and last_updated are DB-managed
_GAME_COLS = (
    "id", "season", "competition", "game_number", "game_date", "game_time",
    "home_team", "away_team", "home_score", "away_score", "venue",
    "venue_address", "google_maps_url", "referee_1", "referee_2",
    "status", "detail_url", "protocol_url",
)


def init_db(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def upsert_game(conn: sqlite3.Connection, game: dict) -> None:
    values = [game.get(c) for c in _GAME_COLS]
    placeholders = ", ".join("?" * len(_GAME_COLS))
    col_list = ", ".join(_GAME_COLS)
    # Skip first_seen and id in the UPDATE clause — first_seen is append-only
    update_cols = [c for c in _GAME_COLS if c not in ("id", "first_seen")]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    update_clause += ", last_updated = CURRENT_TIMESTAMP"
    sql = (
        f"INSERT INTO games ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {update_clause}"
    )
    conn.execute(sql, values)
    conn.commit()


def replace_player_stats(
    conn: sqlite3.Connection, game_id: str, rows: list
) -> None:
    conn.execute("DELETE FROM player_stats WHERE game_id = ?", (game_id,))
    cols = ("game_id", "player_name", "cap_number", "birth_year",
            "goals", "exclusions", "penalty_throws")
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(cols)
    sql = f"INSERT INTO player_stats ({col_list}) VALUES ({placeholders})"
    for row in rows:
        conn.execute(sql, [
            game_id,
            row.get("player_name"),
            row.get("cap_number"),
            row.get("birth_year"),
            row.get("goals", 0),
            row.get("exclusions", 0),
            row.get("penalty_throws", 0),
        ])
    conn.commit()


def replace_team_stats(
    conn: sqlite3.Connection, game_id: str, stats: dict
) -> None:
    conn.execute("DELETE FROM team_stats WHERE game_id = ?", (game_id,))
    conn.execute(
        "INSERT INTO team_stats "
        "(game_id, powerplay_goals, powerplay_situations, "
        "penalty_throw_goals, penalty_throws) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            game_id,
            stats.get("powerplay_goals"),
            stats.get("powerplay_situations"),
            stats.get("penalty_throw_goals"),
            stats.get("penalty_throws"),
        ),
    )
    conn.commit()


def slug_for_team(team_name: str, competition: str) -> str:
    team_name = (team_name or "").strip()
    comp_upper = (competition or "").upper()
    # Youth age class takes priority — youth women still bucket by age.
    if comp_upper.startswith("U12"):
        suffix = "u12"
    elif comp_upper.startswith("U14"):
        suffix = "u14"
    elif comp_upper.startswith("U16"):
        suffix = "u16"
    elif comp_upper.startswith("U18"):
        suffix = "u18"
    elif team_name.endswith(" III"):
        suffix = "herren_3"
    elif team_name.endswith(" II"):
        suffix = "herren_2"
    elif "Damen" in team_name or "weiblich" in comp_upper.lower():
        suffix = "damen"
    else:
        suffix = "herren_1"
    return f"sgw_essen_{suffix}"
