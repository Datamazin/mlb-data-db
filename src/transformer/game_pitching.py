"""
Populate silver.game_pitching from bronze game-feed Parquet files.

Called by the nightly scheduler (per-date) and the backfill CLI (all dates).

Innings pitched are stored as integer `outs` (3 outs = 1 IP) so that
ERA, WHIP, and rate stats use clean integer arithmetic with no rounding
from the MLB "5.2 = 5⅔" display notation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_outs(innings_pitched: str | None) -> int:
    """Convert MLB innings-pitched string ('5.2') → integer outs (17)."""
    if not innings_pitched:
        return 0
    try:
        val = float(innings_pitched)
        full = int(val)
        partial = round((val - full) * 10)   # "5.2" → partial = 2
        return full * 3 + partial
    except (ValueError, TypeError):
        return 0


def extract_records(game_pk: int, raw_json: str) -> list[dict]:
    """Parse one game feed JSON → list of per-pitcher stat dicts."""
    records: list[dict] = []
    try:
        feed = json.loads(raw_json)
        game_teams = feed.get("gameData", {}).get("teams", {})
        home_id = game_teams.get("home", {}).get("id")
        away_id = game_teams.get("away", {}).get("id")

        bs_teams = (
            feed.get("liveData", {})
                .get("boxscore", {})
                .get("teams", {})
        )

        for side, is_home, team_id in [
            ("home", True,  home_id),
            ("away", False, away_id),
        ]:
            if team_id is None:
                continue

            section = bs_teams.get(side, {})
            players  = section.get("players", {})
            pitchers = section.get("pitchers", [])

            for pid in pitchers:
                p = players.get(f"ID{pid}", {})
                s = p.get("stats", {}).get("pitching", {})

                records.append({
                    "game_pk":           game_pk,
                    "player_id":         pid,
                    "team_id":           team_id,
                    "is_home":           is_home,
                    "wins":              s.get("wins",           0) or 0,
                    "losses":            s.get("losses",         0) or 0,
                    "saves":             s.get("saves",          0) or 0,
                    "holds":             s.get("holds",          0) or 0,
                    "blown_saves":       s.get("blownSaves",     0) or 0,
                    "games_started":     s.get("gamesStarted",   0) or 0,
                    "games_finished":    s.get("gamesFinished",  0) or 0,
                    "complete_games":    s.get("completeGames",  0) or 0,
                    "shutouts":          s.get("shutouts",       0) or 0,
                    "outs":              _parse_outs(s.get("inningsPitched")),
                    "hits_allowed":      s.get("hits",           0) or 0,
                    "runs_allowed":      s.get("runs",           0) or 0,
                    "earned_runs":       s.get("earnedRuns",     0) or 0,
                    "home_runs_allowed": s.get("homeRuns",       0) or 0,
                    "walks":             s.get("baseOnBalls",    0) or 0,
                    "strikeouts":        s.get("strikeOuts",     0) or 0,
                    "hit_by_pitch":      s.get("hitByPitch",     0) or 0,
                    "pitches_thrown":    s.get("pitchesThrown",  0) or 0,
                    "strikes":           s.get("strikes",        0) or 0,
                    "loaded_at":         _utc_now(),
                })
    except Exception as exc:
        print(f"    parse error game {game_pk}: {exc}")

    return records


def populate_from_files(
    conn: duckdb.DuckDBPyConnection,
    parquet_files: list[Path],
) -> int:
    """
    Insert/replace pitching rows for the given Parquet files.
    Returns total row count written.
    """
    total = 0
    for f in parquet_files:
        try:
            rows = conn.execute(f"""
                SELECT game_pk, raw_json
                FROM read_parquet('{f}', union_by_name=true)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY extracted_at DESC) = 1
            """).fetchall()
        except Exception as exc:
            print(f"  SKIP {f.name}: {exc}")
            continue

        records: list[dict] = []
        for game_pk, raw_json in rows:
            records.extend(extract_records(game_pk, raw_json))

        if not records:
            continue

        conn.execute("BEGIN")
        try:
            conn.executemany(
                """
                INSERT OR REPLACE INTO silver.game_pitching
                    (game_pk, player_id, team_id, is_home,
                     wins, losses, saves, holds, blown_saves,
                     games_started, games_finished, complete_games, shutouts,
                     outs, hits_allowed, runs_allowed, earned_runs,
                     home_runs_allowed, walks, strikeouts,
                     hit_by_pitch, pitches_thrown, strikes, loaded_at)
                VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?,?)
                """,
                [
                    (
                        r["game_pk"], r["player_id"], r["team_id"], r["is_home"],
                        r["wins"], r["losses"], r["saves"], r["holds"], r["blown_saves"],
                        r["games_started"], r["games_finished"], r["complete_games"], r["shutouts"],
                        r["outs"], r["hits_allowed"], r["runs_allowed"], r["earned_runs"],
                        r["home_runs_allowed"], r["walks"], r["strikeouts"],
                        r["hit_by_pitch"], r["pitches_thrown"], r["strikes"], r["loaded_at"],
                    )
                    for r in records
                ],
            )
            conn.execute("COMMIT")
            total += len(records)
        except Exception as exc:
            conn.execute("ROLLBACK")
            print(f"  INSERT error {f.name}: {exc}")

    return total
