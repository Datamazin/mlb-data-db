"""
Populate silver.game_batting from bronze game-feed Parquet files.

Called by the nightly scheduler (per-date) and the backfill CLI (all dates).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_records(game_pk: int, raw_json: str) -> list[dict]:
    """Parse one game feed JSON → list of batting-stat dicts."""
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
            players = section.get("players", {})
            order   = section.get("battingOrder", [])

            for pid_str in order:
                pid = int(pid_str)
                p   = players.get(f"ID{pid}", {})

                bat_ord_str = p.get("battingOrder", "")
                bat_ord = int(bat_ord_str) if bat_ord_str else None
                pos = p.get("position", {}).get("abbreviation") or None

                b = p.get("stats", {}).get("batting", {})
                records.append({
                    "game_pk":         game_pk,
                    "player_id":       pid,
                    "team_id":         team_id,
                    "is_home":         is_home,
                    "batting_order":   bat_ord,
                    "position_abbrev": pos,
                    "at_bats":         b.get("atBats",      0) or 0,
                    "runs":            b.get("runs",         0) or 0,
                    "hits":            b.get("hits",         0) or 0,
                    "doubles":         b.get("doubles",      0) or 0,
                    "triples":         b.get("triples",      0) or 0,
                    "home_runs":       b.get("homeRuns",     0) or 0,
                    "rbi":             b.get("rbi",          0) or 0,
                    "walks":           b.get("baseOnBalls",  0) or 0,
                    "strikeouts":      b.get("strikeOuts",   0) or 0,
                    "left_on_base":    b.get("leftOnBase",   0) or 0,
                    "loaded_at":       _utc_now(),
                })
    except Exception as exc:
        print(f"    parse error game {game_pk}: {exc}")

    return records


def populate_from_files(
    conn: duckdb.DuckDBPyConnection,
    parquet_files: list[Path],
) -> int:
    """
    Insert/replace batting rows for the given Parquet files.
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
                INSERT OR REPLACE INTO silver.game_batting
                    (game_pk, player_id, team_id, is_home, batting_order,
                     position_abbrev, at_bats, runs, hits, doubles, triples,
                     home_runs, rbi, walks, strikeouts, left_on_base, loaded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        r["game_pk"], r["player_id"], r["team_id"],
                        r["is_home"], r["batting_order"], r["position_abbrev"],
                        r["at_bats"], r["runs"], r["hits"], r["doubles"],
                        r["triples"], r["home_runs"], r["rbi"], r["walks"],
                        r["strikeouts"], r["left_on_base"], r["loaded_at"],
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
