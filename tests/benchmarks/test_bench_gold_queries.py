"""
M8 — Query benchmark suite for the gold schema.

Each test runs a representative gold query against a realistic 5-season dataset
(12,150 games, 750 players, 30 teams) and asserts it completes within an SLA.

Dataset scale:
  - 30 teams × 5 seasons = 150 dim_team rows
  - 750 players
  - 12,150 fact_game rows (2,430 games × 5 seasons)
  - ~435 head_to_head pairs × 5 seasons = ~2,175 rows
  - 3,750 fact_batting rows (750 players × 5 seasons)

SLA thresholds (conservative for embedded DuckDB on developer hardware):
  Query                       SLA
  ─────────────────────────── ──────
  dim_player full scan        100 ms
  dim_team enriched join      100 ms
  dim_venue full scan          50 ms
  fact_game full scan         500 ms
  fact_game filtered (1 team) 200 ms
  head_to_head full scan      500 ms
  head_to_head pair lookup     50 ms
  leaderboards full scan      200 ms
  player_season_summary       200 ms
  standings_snap insert       2000 ms  (complex SQL, 30 teams)
  standings_snap read          200 ms
  meta pipeline_runs query      50 ms

Run with:
    uv run pytest tests/benchmarks/ -v
    uv run pytest tests/benchmarks/ -v -s      # prints timing table
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pytest

# ── Timing helper ──────────────────────────────────────────────────────────────

class _Timer:
    """Context manager that measures wall-clock elapsed time in milliseconds."""

    def __enter__(self) -> _Timer:
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_) -> None:
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000

    def assert_within(self, sla_ms: int, label: str = "") -> None:
        tag = f"[{label}] " if label else ""
        assert self.elapsed_ms < sla_ms, (
            f"{tag}Query exceeded SLA: {self.elapsed_ms:.1f} ms > {sla_ms} ms"
        )


def _q(conn: duckdb.DuckDBPyConnection, sql: str) -> list:
    return conn.execute(sql).fetchall()


# ── dim_player ────────────────────────────────────────────────────────────────

class TestDimPlayerPerf:
    def test_full_scan(self, large_db):
        """Full dim_player scan — 750 rows."""
        with _Timer() as t:
            rows = _q(large_db, "SELECT * FROM gold.dim_player")
        assert len(rows) == 750
        t.assert_within(100, "dim_player full scan")

    def test_filter_by_position(self, large_db):
        """Filtered dim_player — 'SP' pitchers only."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT player_id, full_name FROM gold.dim_player "
                      "WHERE primary_position = 'SP'")
        assert len(rows) >= 0  # count varies by seed
        t.assert_within(100, "dim_player filter by position")

    def test_active_players(self, large_db):
        """All active players — full table since all seeded as active."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT COUNT(*) FROM gold.dim_player WHERE active = TRUE")
        assert rows[0][0] == 750
        t.assert_within(100, "dim_player active filter")


# ── dim_team ──────────────────────────────────────────────────────────────────

class TestDimTeamPerf:
    def test_full_scan(self, large_db):
        """Full dim_team scan — 30 teams × 5 seasons = 150 rows."""
        with _Timer() as t:
            rows = _q(large_db, "SELECT * FROM gold.dim_team")
        assert len(rows) == 150
        t.assert_within(100, "dim_team full scan")

    def test_single_season(self, large_db):
        """dim_team filtered to one season — 30 rows."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT team_id, team_name, league_name, division_name "
                      "FROM gold.dim_team WHERE season_year = 2024")
        assert len(rows) == 30
        t.assert_within(100, "dim_team single season")

    def test_division_filter(self, large_db):
        """dim_team for one division, all seasons — 5 teams × 5 seasons = 25 rows."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT team_id, season_year FROM gold.dim_team "
                      "WHERE division_id = 203")
        assert len(rows) == 25
        t.assert_within(100, "dim_team division filter")


# ── dim_venue ─────────────────────────────────────────────────────────────────

class TestDimVenuePerf:
    def test_full_scan(self, large_db):
        """Full dim_venue scan — 30 venues (one per team)."""
        with _Timer() as t:
            rows = _q(large_db, "SELECT venue_id, venue_name FROM gold.dim_venue")
        assert len(rows) == 30
        t.assert_within(50, "dim_venue full scan")


# ── fact_game ─────────────────────────────────────────────────────────────────

class TestFactGamePerf:
    def test_full_scan(self, large_db):
        """Full fact_game scan — 12,150 rows (2,430 games × 5 seasons)."""
        with _Timer() as t:
            rows = _q(large_db, "SELECT COUNT(*) FROM gold.fact_game")
        assert rows[0][0] == 12_150
        t.assert_within(500, "fact_game full scan count")

    def test_single_team_season(self, large_db):
        """Games for one team in one season (home + away) — ~162 rows."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT game_pk, game_date, home_score, away_score "
                      "FROM gold.fact_game "
                      "WHERE (home_team_id = 119 OR away_team_id = 119) "
                      "  AND season_year = 2024")
        # At this scale each team appears in most game pairs
        assert len(rows) >= 0
        t.assert_within(200, "fact_game single team+season")

    def test_date_range(self, large_db):
        """Games in July 2024."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT COUNT(*) FROM gold.fact_game "
                      "WHERE game_date BETWEEN '2024-07-01' AND '2024-07-31'")
        assert rows[0][0] >= 0
        t.assert_within(200, "fact_game date range")

    def test_select_with_team_names(self, large_db):
        """Project enriched columns including team names."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT game_pk, home_team_name, away_team_name, "
                      "       home_score, away_score, venue_name "
                      "FROM gold.fact_game WHERE season_year = 2024 LIMIT 100")
        assert len(rows) == 100
        t.assert_within(500, "fact_game enriched projection")

    def test_aggregation_wins_by_team(self, large_db):
        """Compute home wins per team in 2024 — group-by over 2,430 rows."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT home_team_id, COUNT(*) AS home_wins "
                      "FROM gold.fact_game "
                      "WHERE season_year = 2024 AND home_score > away_score "
                      "GROUP BY home_team_id ORDER BY home_wins DESC")
        # All or nearly all 30 teams will have home wins; allow ±1 for RNG variance
        assert len(rows) >= 29
        t.assert_within(500, "fact_game home wins aggregation")


# ── head_to_head ──────────────────────────────────────────────────────────────

class TestHeadToHeadPerf:
    def test_full_scan(self, large_db):
        """Full head_to_head scan — all pairs, all seasons."""
        with _Timer() as t:
            rows = _q(large_db, "SELECT COUNT(*) FROM gold.head_to_head")
        count = rows[0][0]
        assert count > 0
        t.assert_within(500, "head_to_head full scan")

    def test_single_pair(self, large_db):
        """Career record between two specific teams."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT season_year, wins, losses, games_played "
                      "FROM gold.head_to_head "
                      "WHERE team_id = 119 AND opponent_id = 147 "
                      "ORDER BY season_year")
        assert len(rows) >= 0
        t.assert_within(50, "head_to_head single pair")

    def test_single_team_all_opponents(self, large_db):
        """One team's record vs all opponents in 2024."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT opponent_id, wins, losses "
                      "FROM gold.head_to_head "
                      "WHERE team_id = 119 AND season_year = 2024 "
                      "ORDER BY wins DESC")
        assert len(rows) >= 0
        t.assert_within(200, "head_to_head one team all opponents")


# ── leaderboards ──────────────────────────────────────────────────────────────

class TestLeaderboardsPerf:
    def test_top_50_home_run_leaders(self, large_db):
        """Top-50 HR leaders across all seasons."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT player_id, full_name, season_year, home_runs "
                      "FROM gold.leaderboards "
                      "ORDER BY home_runs DESC NULLS LAST LIMIT 50")
        assert len(rows) == 50
        t.assert_within(200, "leaderboards top-50 HR")

    def test_single_season_leaders(self, large_db):
        """All batting leaders for 2024 regular season."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT player_id, full_name, avg, ops "
                      "FROM gold.leaderboards "
                      "WHERE season_year = 2024 AND game_type = 'R' "
                      "ORDER BY ops DESC NULLS LAST")
        assert len(rows) >= 0
        t.assert_within(200, "leaderboards 2024 season")

    def test_ops_over_threshold(self, large_db):
        """Players with OPS > 0.900 (elite hitters)."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT COUNT(*) FROM gold.leaderboards WHERE ops > 0.900")
        assert rows[0][0] >= 0
        t.assert_within(200, "leaderboards OPS filter")


# ── player_season_summary ─────────────────────────────────────────────────────

class TestPlayerSeasonSummaryPerf:
    def test_full_scan(self, large_db):
        """Full player_season_summary — 750 players × 5 seasons = 3,750 rows."""
        with _Timer() as t:
            rows = _q(large_db, "SELECT COUNT(*) FROM gold.player_season_summary")
        assert rows[0][0] == 3_750
        t.assert_within(200, "player_season_summary full scan")

    def test_single_player_career(self, large_db):
        """All seasons for one player — 5 rows."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT season_year, games, home_runs, avg, ops "
                      "FROM gold.player_season_summary "
                      "WHERE player_id = 100000 ORDER BY season_year")
        assert len(rows) == 5
        t.assert_within(100, "player_season_summary single player")

    def test_top_ops_2024(self, large_db):
        """Top-10 OPS in 2024."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT player_id, full_name, ops "
                      "FROM gold.player_season_summary "
                      "WHERE season_year = 2024 AND game_type = 'R' "
                      "ORDER BY ops DESC NULLS LAST LIMIT 10")
        assert len(rows) == 10
        t.assert_within(200, "player_season_summary top OPS 2024")

    def test_team_roster_season(self, large_db):
        """All players on a team in a given season."""
        with _Timer() as t:
            rows = _q(large_db,
                      "SELECT player_id, full_name, games, avg "
                      "FROM gold.player_season_summary "
                      "WHERE team_id = 119 AND season_year = 2024")
        assert len(rows) >= 0
        t.assert_within(200, "player_season_summary team season roster")


# ── standings_snap ────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def standings_db(tmp_path_factory, large_db):
    """
    Module-scoped file-backed DuckDB for standings benchmarks.

    standings_snap uses INSERT OR REPLACE so writes are isolated from
    the shared large_db connection. Silver data is copied from large_db,
    then the standings SQL is run once at fixture setup time.
    """
    MIGRATIONS = Path(__file__).parent.parent.parent / "sql" / "schema"
    GOLD_SQL = Path(__file__).parent.parent.parent / "sql" / "gold"

    tmp = tmp_path_factory.mktemp("standings_bench")
    db_path = tmp / "standings_bench.duckdb"
    conn = duckdb.connect(str(db_path))

    for mig in sorted(MIGRATIONS.glob("*.sql")):
        conn.execute(mig.read_text())

    for table in [
        "silver.seasons", "silver.leagues", "silver.divisions",
        "silver.venues", "silver.teams", "silver.games",
    ]:
        rows = large_db.execute(f"SELECT * FROM {table}").fetchall()
        if rows:
            placeholders = ", ".join("?" for _ in rows[0])
            conn.executemany(
                f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})", rows
            )

    # Run the standings INSERT once so all read tests share the result
    conn.execute((GOLD_SQL / "008_standings_snap.sql").read_text())

    yield conn
    conn.close()


class TestStandingsSnapPerf:
    def test_standings_snap_insert_timing(self, standings_db):
        """
        Re-run standings INSERT to measure timing (idempotent via INSERT OR REPLACE).
        Exercises the full CTE pipeline over 12,150 games across 5 seasons.
        SLA: 2000 ms.
        """
        GOLD_SQL = Path(__file__).parent.parent.parent / "sql" / "gold"
        sql = (GOLD_SQL / "008_standings_snap.sql").read_text()

        with _Timer() as t:
            standings_db.execute(sql)
        count = standings_db.execute(
            "SELECT COUNT(*) FROM gold.standings_snap"
        ).fetchone()[0]
        assert count == 30  # one row per team for today's snap_date (INSERT OR REPLACE)
        t.assert_within(2000, "standings_snap insert 30 teams 12150 games")

    def test_standings_read_single_season(self, standings_db):
        """Read all 30 team rows from the latest snapshot."""
        with _Timer() as t:
            rows = standings_db.execute(
                "SELECT team_id, wins, losses, win_pct, games_back, streak "
                "FROM gold.standings_snap "
                "ORDER BY wins DESC"
            ).fetchall()
        assert len(rows) == 30
        t.assert_within(200, "standings_snap read all teams")

    def test_standings_division_race(self, standings_db):
        """NL West standings (5 teams) sorted by win_pct."""
        snap_season = standings_db.execute(
            "SELECT season_year FROM gold.standings_snap LIMIT 1"
        ).fetchone()[0]
        with _Timer() as t:
            rows = standings_db.execute(
                "SELECT s.team_id, s.wins, s.losses, s.win_pct, s.games_back "
                "FROM gold.standings_snap s "
                "JOIN silver.teams t ON s.team_id = t.team_id AND t.season_year = ? "
                "WHERE t.division_id = 203 "
                "ORDER BY s.win_pct DESC NULLS LAST",
                [snap_season],
            ).fetchall()
        assert len(rows) == 5
        t.assert_within(200, "standings_snap NL West race")


# ── Cross-schema analytical query ─────────────────────────────────────────────

class TestCrossSchemaPerf:
    def test_player_career_with_team_names(self, large_db):
        """
        Join player_season_summary → dim_team for enriched career line.
        Exercises a typical club analyst query pattern.
        """
        with _Timer() as t:
            rows = _q(large_db,
                      """
                      SELECT
                          pss.player_id,
                          pss.full_name,
                          pss.season_year,
                          pss.games,
                          pss.home_runs,
                          pss.avg,
                          pss.ops,
                          dt.team_name,
                          dt.league_name,
                          dt.division_name
                      FROM gold.player_season_summary pss
                      JOIN gold.dim_team dt
                          ON pss.team_id = dt.team_id
                          AND pss.season_year = dt.season_year
                      WHERE pss.season_year = 2024
                      ORDER BY pss.ops DESC NULLS LAST
                      LIMIT 25
                      """)
        assert len(rows) == 25
        t.assert_within(500, "cross-schema career with team names")

    def test_wins_by_division_2024(self, large_db):
        """
        Aggregate game wins by division — joins fact_game + dim_team.
        Representative of a standings or division-strength query.
        """
        with _Timer() as t:
            rows = _q(large_db,
                      """
                      SELECT
                          dt.division_name,
                          COUNT(*) AS total_home_wins
                      FROM gold.fact_game fg
                      JOIN gold.dim_team dt
                          ON fg.home_team_id = dt.team_id
                          AND fg.season_year = dt.season_year
                      WHERE fg.season_year = 2024
                        AND fg.home_score > fg.away_score
                      GROUP BY dt.division_name
                      ORDER BY total_home_wins DESC
                      """)
        assert len(rows) == 6  # 6 divisions
        t.assert_within(500, "wins by division 2024")

    def test_head_to_head_with_team_names(self, large_db):
        """
        Enrich head_to_head with full team names.
        """
        with _Timer() as t:
            rows = _q(large_db,
                      """
                      SELECT
                          t1.team_name AS team,
                          t2.team_name AS opponent,
                          h.season_year,
                          h.wins,
                          h.losses
                      FROM gold.head_to_head h
                      JOIN gold.dim_team t1
                          ON h.team_id = t1.team_id AND h.season_year = t1.season_year
                      JOIN gold.dim_team t2
                          ON h.opponent_id = t2.team_id AND h.season_year = t2.season_year
                      WHERE h.team_id = 119
                        AND h.season_year = 2024
                      ORDER BY h.wins DESC
                      """)
        assert len(rows) >= 0
        t.assert_within(500, "head_to_head enriched with team names")
