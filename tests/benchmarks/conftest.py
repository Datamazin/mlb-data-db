"""
Benchmark fixtures — large-scale synthetic MLB data.

Generates a realistic 5-season dataset seeded directly into silver tables:
  - 30 teams across 6 divisions / 2 leagues
  - 5 seasons (2022–2026), 2,430 regular-season games per season (12,150 total)
  - 750 players with fact_batting rows (one per player × season)

The fixture is module-scoped so it is built once per benchmark module,
keeping benchmark runs fast despite the large data volume.
"""

from __future__ import annotations

import random
from pathlib import Path

import duckdb
import pytest

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "sql" / "schema"
GOLD_SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "gold"

SEASONS = [2022, 2023, 2024, 2025, 2026]

# 2 leagues × 3 divisions × 5 teams = 30 teams
LEAGUES = [
    (103, "American League", "AL"),
    (104, "National League", "NL"),
]
DIVISIONS = [
    (200, "AL West",    103),
    (201, "AL East",    103),
    (202, "AL Central", 103),
    (203, "NL West",    104),
    (204, "NL East",    104),
    (205, "NL Central", 104),
]
# 5 teams per division → 30 total
_TEAM_NAMES = [
    # AL West
    (108, 200, "Los Angeles Angels",     "LAA"),
    (117, 200, "Houston Astros",         "HOU"),
    (133, 200, "Oakland Athletics",      "OAK"),
    (136, 200, "Seattle Mariners",       "SEA"),
    (140, 200, "Texas Rangers",          "TEX"),
    # AL East
    (110, 201, "Baltimore Orioles",      "BAL"),
    (111, 201, "Boston Red Sox",         "BOS"),
    (147, 201, "New York Yankees",       "NYY"),
    (139, 201, "Tampa Bay Rays",         "TBR"),
    (141, 201, "Toronto Blue Jays",      "TOR"),
    # AL Central
    (145, 202, "Chicago White Sox",      "CWS"),
    (114, 202, "Cleveland Guardians",    "CLE"),
    (116, 202, "Detroit Tigers",         "DET"),
    (118, 202, "Kansas City Royals",     "KAN"),
    (142, 202, "Minnesota Twins",        "MIN"),
    # NL West
    (109, 203, "Arizona Diamondbacks",   "ARI"),
    (115, 203, "Colorado Rockies",       "COL"),
    (119, 203, "Los Angeles Dodgers",    "LAD"),
    (135, 203, "San Diego Padres",       "SDP"),
    (137, 203, "San Francisco Giants",   "SFG"),
    # NL East
    (144, 204, "Atlanta Braves",         "ATL"),
    (146, 204, "Miami Marlins",          "MIA"),
    (121, 204, "New York Mets",          "NYM"),
    (143, 204, "Philadelphia Phillies",  "PHI"),
    (120, 204, "Washington Nationals",   "WSH"),
    # NL Central
    (112, 205, "Chicago Cubs",           "CHC"),
    (113, 205, "Cincinnati Reds",        "CIN"),
    (158, 205, "Milwaukee Brewers",      "MIL"),
    (134, 205, "Pittsburgh Pirates",     "PIT"),
    (138, 205, "St. Louis Cardinals",    "STL"),
]
TEAM_IDS = [t[0] for t in _TEAM_NAMES]
NUM_PLAYERS = 750
GAMES_PER_SEASON = 2_430   # 30 teams × 162 / 2 (each game involves 2 teams)


def _seed_reference_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Seed seasons, leagues, divisions, venues, teams."""
    # Seasons
    conn.executemany(
        """INSERT OR REPLACE INTO silver.seasons
               (season_year, sport_id, regular_season_start, regular_season_end,
                games_per_team, loaded_at)
           VALUES (?, 1, ?, ?, 162, current_timestamp)""",
        [(y, f"{y}-04-01", f"{y}-10-01") for y in SEASONS],
    )

    # Leagues
    conn.executemany(
        """INSERT OR REPLACE INTO silver.leagues
               (league_id, league_name, short_name, abbreviation, loaded_at)
           VALUES (?, ?, ?, ?, current_timestamp)""",
        [(lid, name, name, abbrev) for lid, name, abbrev in LEAGUES],
    )

    # Divisions
    conn.executemany(
        """INSERT OR REPLACE INTO silver.divisions
               (division_id, division_name, short_name, league_id, loaded_at)
           VALUES (?, ?, ?, ?, current_timestamp)""",
        [(did, name, name, lid) for did, name, lid in DIVISIONS],
    )

    # Venues (one per team, simple)
    conn.executemany(
        """INSERT OR REPLACE INTO silver.venues
               (venue_id, venue_name, loaded_at)
           VALUES (?, ?, current_timestamp)""",
        [(team_id, f"Venue_{team_id}") for team_id, *_ in _TEAM_NAMES],
    )

    # Teams × seasons
    conn.executemany(
        """INSERT OR REPLACE INTO silver.teams
               (team_id, season_year, team_name, team_abbrev, team_code,
                league_id, division_id, venue_id, city, first_year, active, loaded_at)
           SELECT ?, ?, ?, ?, 'xxx',
                  d.league_id, ?, ?, 'City', 1900, TRUE, current_timestamp
           FROM silver.divisions d WHERE d.division_id = ?""",
        [(tid, sy, name, abbrev, div_id, tid, div_id) for tid, div_id, name, abbrev in _TEAM_NAMES for sy in SEASONS],
    )


def _seed_games(conn: duckdb.DuckDBPyConnection, rng: random.Random) -> None:
    """Generate ~2,430 regular-season games per season across 5 seasons."""
    games = []
    game_pk = 700_000

    # All unique team pairs: 30C2 = 435. Repeat cyclically to reach 2,430/season.
    base_pairs = [
        (TEAM_IDS[i], TEAM_IDS[j])
        for i in range(len(TEAM_IDS))
        for j in range(i + 1, len(TEAM_IDS))
    ]
    # Each pair plays ~5-6 series per season; extend and trim to exact target.
    season_pairs = (base_pairs * ((GAMES_PER_SEASON // len(base_pairs)) + 1))[:GAMES_PER_SEASON]

    for season_year in SEASONS:
        for home_id, away_id in season_pairs:
            month = rng.randint(4, 9)
            day = rng.randint(1, 28)
            game_date = f"{season_year}-{month:02d}-{day:02d}"
            home_score = rng.randint(0, 12)
            away_score = rng.randint(0, 12)
            # Avoid ties — baseball has no draws
            while home_score == away_score:
                away_score = rng.randint(0, 12)

            games.append((
                game_pk, season_year, game_date, "R", "Final",
                home_id, away_id, home_score, away_score,
                9, home_id,  # venue_id = home team id
            ))
            game_pk += 1

    conn.executemany(
        """INSERT OR REPLACE INTO silver.games
               (game_pk, season_year, game_date, game_type, status,
                home_team_id, away_team_id, home_score, away_score,
                innings, venue_id, loaded_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
        games,
    )


def _seed_players_and_batting(conn: duckdb.DuckDBPyConnection, rng: random.Random) -> None:
    """Seed 750 players and one fact_batting row per player × season."""
    players = [
        (
            100_000 + i,
            f"Player_{i:04d}",
            f"First_{i}",
            f"Last_{i}",
            rng.choice(["L", "R", "S"]),
            rng.choice(["L", "R"]),
            rng.choice(["SP", "RP", "C", "1B", "2B", "3B", "SS", "OF", "DH"]),
            True,
        )
        for i in range(NUM_PLAYERS)
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO silver.players
               (player_id, full_name, first_name, last_name,
                bats, throws, primary_position, active, loaded_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
        players,
    )

    player_ids = [100_000 + i for i in range(NUM_PLAYERS)]
    batting_rows = []
    for player_id in player_ids:
        team_id = rng.choice(TEAM_IDS)
        for season_year in SEASONS:
            ab = rng.randint(50, 550)
            hits = rng.randint(0, min(ab, 200))
            doubles = rng.randint(0, hits // 4)
            triples = rng.randint(0, hits // 20)
            hr = rng.randint(0, hits // 5)
            bb = rng.randint(0, 120)
            so = rng.randint(0, 200)
            pa = ab + bb
            avg = round(hits / ab, 3) if ab > 0 else 0.0
            obp = round((hits + bb) / pa, 3) if pa > 0 else 0.0
            slg = round(
                (hits - doubles - triples - hr + 2 * doubles + 3 * triples + 4 * hr) / ab, 3
            ) if ab > 0 else 0.0
            ops = round(obp + slg, 3)
            batting_rows.append((
                player_id, team_id, season_year, "R",
                rng.randint(20, 162), pa, ab, hits, doubles, triples, hr,
                rng.randint(0, 120), rng.randint(0, 100), bb, rng.randint(0, 20),
                so, rng.randint(0, 40), rng.randint(0, 15),
                avg, obp, slg, ops,
                round(hits / (ab - hr - so + bb) if (ab - hr - so + bb) > 0 else 0, 3),
            ))

    conn.executemany(
        """INSERT OR REPLACE INTO silver.fact_batting
               (player_id, team_id, season_year, game_type,
                games, pa, ab, hits, doubles, triples, home_runs,
                rbi, runs, walks, ibb, strikeouts, stolen_bases, caught_stealing,
                avg, obp, slg, ops, babip, loaded_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, current_timestamp)""",
        batting_rows,
    )


@pytest.fixture(scope="module")
def large_db(tmp_path_factory: pytest.TempPathFactory) -> duckdb.DuckDBPyConnection:
    """
    Module-scoped file-backed DuckDB with full schema + 5-season realistic data.

    Built once per benchmark module. Tests share the connection read-only style
    (no mutations). Returns an open connection; cleanup is automatic at module teardown.
    """
    tmp_path = tmp_path_factory.mktemp("bench")
    db_path = tmp_path / "bench_mlb.duckdb"

    conn = duckdb.connect(str(db_path))
    # Apply schema
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        conn.execute(migration.read_text(encoding="utf-8"))

    rng = random.Random(42)  # deterministic for reproducibility

    _seed_reference_tables(conn)
    _seed_games(conn, rng)
    _seed_players_and_batting(conn, rng)

    # Apply all gold view definitions
    from aggregator.aggregate import Aggregator
    Aggregator(conn).run(
        scripts=[
            "001_dim_player.sql",
            "002_dim_team.sql",
            "003_dim_venue.sql",
            "004_fact_game.sql",
            "005_head_to_head.sql",
            "006_leaderboards.sql",
            "007_player_season_summary.sql",
        ],
        force=True,
    )

    yield conn
    conn.close()
