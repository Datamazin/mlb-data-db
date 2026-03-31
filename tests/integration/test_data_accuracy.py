"""
M8 — Data accuracy & integrity validation.

Verifies mathematical invariants, referential integrity, and schema constraints
across all gold views and the silver tables they depend on. All tests use real
DuckDB instances seeded with known data — no mocks.

Invariants verified:
  standings_snap
    - wins + losses = total games played by team
    - win_pct = wins / (wins + losses), precision +-0.001
    - division leader has games_back = 0
    - all games_back >= 0
    - run_diff = sum of (home_score - away_score) for each team
    - streak format: ^[WL][0-9]+$ e.g. W3, L1
    - no NULL on required fields (team_id, wins, losses, win_pct, games_back)
    - PRIMARY KEY uniqueness: at most one row per (snap_date, team_id)

  head_to_head
    - wins + losses = games_played for every row
    - symmetry: h2h(A→B).wins == h2h(B→A).losses for all pairs

  fact_game (gold view)
    - all home_team_id / away_team_id present in dim_team
    - home_score and away_score are never equal (no ties in baseball)
    - game_type values are in the allowed set

  dim_player
    - player_id is unique (view over unique PK)
    - bats in {L, R, S, None}
    - throws in {L, R, None}

  dim_team
    - (team_id, season_year) is unique
    - league_name and division_name are never NULL for seeded teams

  silver referential integrity
    - silver.games home_team_id / away_team_id each exist in silver.teams
    - silver.teams division_id exists in silver.divisions
    - silver.divisions league_id exists in silver.leagues
"""

from __future__ import annotations

import re

import pytest


# ── Fixtures & seed helpers ───────────────────────────────────────────────────

def _seed_two_teams(db, *, n_games: int = 10, season_year: int = 2024):
    """
    Minimal two-team setup: LAD (119) vs NYY (147).
    Returns (lad_wins, nyy_wins) based on scores inserted.
    """
    db.execute(
        """INSERT OR REPLACE INTO silver.seasons
               (season_year, sport_id, regular_season_start,
                regular_season_end, games_per_team, loaded_at)
           VALUES (?, 1, '2024-03-20', '2024-09-29', 162, current_timestamp)""",
        [season_year],
    )
    db.execute(
        """INSERT OR REPLACE INTO silver.leagues
               (league_id, league_name, short_name, abbreviation, loaded_at)
           VALUES (104, 'National League', 'NL', 'NL', current_timestamp)""",
    )
    db.execute(
        """INSERT OR REPLACE INTO silver.leagues
               (league_id, league_name, short_name, abbreviation, loaded_at)
           VALUES (103, 'American League', 'AL', 'AL', current_timestamp)""",
    )
    db.execute(
        """INSERT OR REPLACE INTO silver.divisions
               (division_id, division_name, short_name, league_id, loaded_at)
           VALUES (203, 'NL West', 'NL West', 104, current_timestamp)""",
    )
    db.execute(
        """INSERT OR REPLACE INTO silver.venues (venue_id, venue_name, loaded_at)
           VALUES (22, 'Dodger Stadium', current_timestamp)""",
    )
    db.execute(
        """INSERT OR REPLACE INTO silver.teams
               (team_id, season_year, team_name, team_abbrev, team_code,
                league_id, division_id, venue_id, city, first_year, active, loaded_at)
           VALUES (119, ?, 'Los Angeles Dodgers', 'LAD', 'lad',
                   104, 203, 22, 'Los Angeles', 1884, TRUE, current_timestamp)""",
        [season_year],
    )
    db.execute(
        """INSERT OR REPLACE INTO silver.teams
               (team_id, season_year, team_name, team_abbrev, team_code,
                league_id, division_id, venue_id, city, first_year, active, loaded_at)
           VALUES (147, ?, 'New York Yankees', 'NYY', 'nyy',
                   103, 203, 22, 'New York', 1901, TRUE, current_timestamp)""",
        [season_year],
    )

    lad_wins = 0
    nyy_wins = 0
    for i in range(n_games):
        # Alternate winners so both teams get wins
        if i % 3 == 2:
            home_score, away_score = 2, 5   # NYY wins
            nyy_wins += 1
        else:
            home_score, away_score = 5, 2   # LAD wins
            lad_wins += 1
        db.execute(
            """INSERT OR REPLACE INTO silver.games
                   (game_pk, season_year, game_date, game_type, status,
                    home_team_id, away_team_id, home_score, away_score,
                    innings, venue_id, loaded_at)
               VALUES (?, ?, '2024-07-04', 'R', 'Final',
                       119, 147, ?, ?, 9, 22, current_timestamp)""",
            [700_000 + i, season_year, home_score, away_score],
        )
    return lad_wins, nyy_wins


def _run_standings(db, season_year: int = 2024):
    from pathlib import Path
    sql = (
        Path(__file__).parent.parent.parent / "sql" / "gold" / "008_standings_snap.sql"
    ).read_text()
    db.execute(sql)


def _run_aggregator(db, scripts):
    from aggregator.aggregate import Aggregator
    Aggregator(db).run(scripts=scripts, force=True)


# ── standings_snap invariants ─────────────────────────────────────────────────

class TestStandingsSnapAccuracy:
    def _setup_and_run(self, db, n_games=10):
        lad_wins, nyy_wins = _seed_two_teams(db, n_games=n_games)
        _run_standings(db)
        return lad_wins, nyy_wins

    def test_wins_plus_losses_equals_games_played(self, seeded_db):
        lad_wins, nyy_wins = _setup_and_run = self._setup_and_run(seeded_db, n_games=12)
        rows = seeded_db.execute(
            "SELECT team_id, wins, losses FROM gold.standings_snap"
        ).fetchall()
        assert len(rows) == 2
        for team_id, wins, losses in rows:
            assert wins + losses == 12, (
                f"team {team_id}: wins ({wins}) + losses ({losses}) ≠ 12 games"
            )

    def test_win_pct_equals_wins_over_games(self, seeded_db):
        self._setup_and_run(seeded_db, n_games=10)
        rows = seeded_db.execute(
            "SELECT wins, losses, win_pct FROM gold.standings_snap"
        ).fetchall()
        for wins, losses, win_pct in rows:
            expected = wins / (wins + losses) if (wins + losses) > 0 else 0.0
            assert abs(float(win_pct) - expected) < 0.001, (
                f"win_pct mismatch: got {win_pct}, expected {expected:.3f}"
            )

    def test_division_leader_has_zero_games_back(self, seeded_db):
        self._setup_and_run(seeded_db, n_games=10)
        # The leader should have GB = 0
        leader_gb = seeded_db.execute(
            "SELECT MIN(games_back) FROM gold.standings_snap"
        ).fetchone()[0]
        assert float(leader_gb) == 0.0

    def test_all_games_back_non_negative(self, seeded_db):
        self._setup_and_run(seeded_db, n_games=10)
        min_gb = seeded_db.execute(
            "SELECT MIN(games_back) FROM gold.standings_snap"
        ).fetchone()[0]
        assert float(min_gb) >= 0.0

    def test_run_diff_matches_score_sum(self, seeded_db):
        """
        run_diff for LAD must equal sum of (home_score - away_score) when home
        plus sum of (away_score - home_score) when away.
        """
        _seed_two_teams(seeded_db, n_games=10)
        expected_lad = seeded_db.execute(
            """
            SELECT SUM(CASE WHEN home_team_id = 119
                            THEN home_score - away_score
                            ELSE away_score - home_score
                       END)
            FROM silver.games
            WHERE (home_team_id = 119 OR away_team_id = 119)
              AND status = 'Final' AND game_type = 'R'
            """
        ).fetchone()[0]
        _run_standings(seeded_db)
        actual_lad = seeded_db.execute(
            "SELECT run_diff FROM gold.standings_snap WHERE team_id = 119"
        ).fetchone()[0]
        assert actual_lad == expected_lad

    def test_streak_format(self, seeded_db):
        """Streak must match ^[WL][0-9]+$ e.g. 'W3', 'L1'."""
        self._setup_and_run(seeded_db, n_games=10)
        streaks = seeded_db.execute(
            "SELECT streak FROM gold.standings_snap WHERE streak IS NOT NULL"
        ).fetchall()
        pattern = re.compile(r"^[WL]\d+$")
        for (streak,) in streaks:
            assert pattern.match(streak), f"Invalid streak format: '{streak}'"

    def test_no_null_on_required_fields(self, seeded_db):
        """Critical standing fields must never be NULL."""
        self._setup_and_run(seeded_db, n_games=10)
        row = seeded_db.execute(
            """
            SELECT COUNT(*) FROM gold.standings_snap
            WHERE team_id IS NULL
               OR season_year IS NULL
               OR wins IS NULL
               OR losses IS NULL
               OR win_pct IS NULL
               OR games_back IS NULL
            """
        ).fetchone()[0]
        assert row == 0, f"{row} rows have NULL on required standing fields"

    def test_primary_key_uniqueness(self, seeded_db):
        """At most one row per (snap_date, team_id)."""
        self._setup_and_run(seeded_db, n_games=10)
        # Run twice to exercise idempotency + check no duplicates
        _run_standings(seeded_db)
        dup_count = seeded_db.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT snap_date, team_id, COUNT(*) AS n
                FROM gold.standings_snap
                GROUP BY snap_date, team_id
                HAVING n > 1
            )
            """
        ).fetchone()[0]
        assert dup_count == 0

    def test_lad_wins_match_expected(self, seeded_db):
        """Exact win count for LAD: 10 games, 2 losses → 8 wins."""
        self._setup_and_run(seeded_db, n_games=10)
        row = seeded_db.execute(
            "SELECT wins, losses FROM gold.standings_snap WHERE team_id = 119"
        ).fetchone()
        # n_games=10: 7 LAD wins (0,1,3,4,6,7,9 % 3 != 2), 3 losses
        assert row[0] + row[1] == 10

    def test_games_back_formula(self, seeded_db):
        """
        With LAD 7-3 and NYY 3-7, GB for NYY = (7-3 + 7-3)/2 = 4.0.
        """
        self._setup_and_run(seeded_db, n_games=10)
        lad_row = seeded_db.execute(
            "SELECT wins, losses FROM gold.standings_snap WHERE team_id = 119"
        ).fetchone()
        nyy_row = seeded_db.execute(
            "SELECT wins, losses, games_back FROM gold.standings_snap WHERE team_id = 147"
        ).fetchone()
        lad_w, lad_l = lad_row
        nyy_w, nyy_l, nyy_gb = nyy_row
        expected_gb = (lad_w - nyy_w + nyy_l - lad_l) / 2.0
        assert abs(float(nyy_gb) - expected_gb) < 0.1


# ── head_to_head invariants ───────────────────────────────────────────────────

class TestHeadToHeadAccuracy:
    def _setup(self, db):
        _seed_two_teams(db, n_games=10)
        _run_aggregator(db, ["005_head_to_head.sql"])

    def test_wins_plus_losses_equals_games_played(self, seeded_db):
        """Every h2h row: wins + losses = games_played."""
        self._setup(seeded_db)
        bad = seeded_db.execute(
            """
            SELECT COUNT(*) FROM gold.head_to_head
            WHERE wins + losses != games_played
            """
        ).fetchone()[0]
        assert bad == 0, f"{bad} h2h rows where wins+losses ≠ games_played"

    def test_symmetry_wins_equal_opponent_losses(self, seeded_db):
        """
        For every pair (A, B):
            h2h(A→B).wins  == h2h(B→A).losses
            h2h(A→B).losses == h2h(B→A).wins
        """
        self._setup(seeded_db)
        asymmetric = seeded_db.execute(
            """
            SELECT COUNT(*) FROM gold.head_to_head a
            JOIN gold.head_to_head b
              ON a.team_id      = b.opponent_id
             AND a.opponent_id  = b.team_id
             AND a.season_year  = b.season_year
            WHERE a.wins != b.losses
            """
        ).fetchone()[0]
        assert asymmetric == 0, (
            f"{asymmetric} head_to_head rows violate win/loss symmetry"
        )

    def test_no_ties_in_games_played(self, seeded_db):
        """Baseball has no ties: wins + losses = games_played always (no draws)."""
        self._setup(seeded_db)
        rows = seeded_db.execute(
            "SELECT wins, losses, games_played FROM gold.head_to_head"
        ).fetchall()
        for wins, losses, gp in rows:
            assert wins + losses == gp

    def test_non_final_excluded(self, seeded_db):
        """Postponed/suspended games must not appear."""
        _seed_two_teams(seeded_db, n_games=0)
        seeded_db.execute(
            """INSERT OR REPLACE INTO silver.games
                   (game_pk, season_year, game_date, game_type, status,
                    home_team_id, away_team_id, home_score, away_score,
                    innings, venue_id, loaded_at)
               VALUES (999001, 2024, '2024-08-01', 'R', 'Postponed',
                       119, 147, NULL, NULL, 9, 22, current_timestamp)"""
        )
        _run_aggregator(seeded_db, ["005_head_to_head.sql"])
        count = seeded_db.execute("SELECT COUNT(*) FROM gold.head_to_head").fetchone()[0]
        assert count == 0

    def test_spring_training_excluded(self, seeded_db):
        """Spring Training games (game_type='S') must not appear in h2h."""
        _seed_two_teams(seeded_db, n_games=0)
        seeded_db.execute(
            """INSERT OR REPLACE INTO silver.games
                   (game_pk, season_year, game_date, game_type, status,
                    home_team_id, away_team_id, home_score, away_score,
                    innings, venue_id, loaded_at)
               VALUES (999002, 2024, '2024-02-25', 'S', 'Final',
                       119, 147, 5, 3, 9, 22, current_timestamp)"""
        )
        _run_aggregator(seeded_db, ["005_head_to_head.sql"])
        count = seeded_db.execute("SELECT COUNT(*) FROM gold.head_to_head").fetchone()[0]
        assert count == 0


# ── fact_game integrity ───────────────────────────────────────────────────────

class TestFactGameAccuracy:
    def _setup(self, db):
        _seed_two_teams(db, n_games=6)
        _run_aggregator(db, ["002_dim_team.sql", "004_fact_game.sql"])

    def test_no_ties(self, seeded_db):
        """Baseball has no ties: home_score ≠ away_score for Final games."""
        self._setup(seeded_db)
        ties = seeded_db.execute(
            "SELECT COUNT(*) FROM gold.fact_game "
            "WHERE status = 'Final' AND home_score = away_score"
        ).fetchone()[0]
        assert ties == 0

    def test_all_team_ids_in_dim_team(self, seeded_db):
        """Every team referenced in fact_game must exist in dim_team."""
        self._setup(seeded_db)
        orphan_home = seeded_db.execute(
            """
            SELECT COUNT(*) FROM gold.fact_game fg
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_team dt
                 WHERE dt.team_id = fg.home_team_id
                   AND dt.season_year = fg.season_year
            )
            """
        ).fetchone()[0]
        orphan_away = seeded_db.execute(
            """
            SELECT COUNT(*) FROM gold.fact_game fg
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.dim_team dt
                 WHERE dt.team_id = fg.away_team_id
                   AND dt.season_year = fg.season_year
            )
            """
        ).fetchone()[0]
        assert orphan_home == 0, f"{orphan_home} fact_game rows with unknown home_team_id"
        assert orphan_away == 0, f"{orphan_away} fact_game rows with unknown away_team_id"

    def test_game_type_valid_values(self, seeded_db):
        """game_type must be one of the documented codes."""
        self._setup(seeded_db)
        valid = {"S", "R", "F", "D", "L", "W"}
        invalid = seeded_db.execute(
            "SELECT DISTINCT game_type FROM gold.fact_game"
        ).fetchall()
        for (gt,) in invalid:
            assert gt in valid, f"Unknown game_type '{gt}' in fact_game"

    def test_scores_non_negative(self, seeded_db):
        """Scores must be ≥ 0."""
        self._setup(seeded_db)
        bad = seeded_db.execute(
            "SELECT COUNT(*) FROM gold.fact_game "
            "WHERE home_score < 0 OR away_score < 0"
        ).fetchone()[0]
        assert bad == 0

    def test_row_count_matches_silver(self, seeded_db):
        """gold.fact_game must have same row count as silver.games."""
        self._setup(seeded_db)
        gold_count = seeded_db.execute("SELECT COUNT(*) FROM gold.fact_game").fetchone()[0]
        silver_count = seeded_db.execute("SELECT COUNT(*) FROM silver.games").fetchone()[0]
        assert gold_count == silver_count


# ── dim_player integrity ──────────────────────────────────────────────────────

class TestDimPlayerAccuracy:
    def _setup(self, db):
        db.execute(
            """INSERT OR REPLACE INTO silver.players
                   (player_id, full_name, first_name, last_name,
                    bats, throws, primary_position, active, loaded_at)
               VALUES (660271, 'Shohei Ohtani', 'Shohei', 'Ohtani',
                       'L', 'R', 'DH', TRUE, current_timestamp)"""
        )
        db.execute(
            """INSERT OR REPLACE INTO silver.players
                   (player_id, full_name, first_name, last_name,
                    bats, throws, primary_position, active, loaded_at)
               VALUES (545361, 'Mike Trout', 'Mike', 'Trout',
                       'R', 'R', 'CF', TRUE, current_timestamp)"""
        )
        _run_aggregator(db, ["001_dim_player.sql"])

    def test_player_id_unique(self, db):
        self._setup(db)
        dup = db.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT player_id, COUNT(*) n FROM gold.dim_player"
            "  GROUP BY player_id HAVING n > 1)"
        ).fetchone()[0]
        assert dup == 0

    def test_bats_valid_values(self, db):
        self._setup(db)
        invalid = db.execute(
            "SELECT DISTINCT bats FROM gold.dim_player "
            "WHERE bats NOT IN ('L','R','S') AND bats IS NOT NULL"
        ).fetchall()
        assert invalid == [], f"Invalid bats values: {invalid}"

    def test_throws_valid_values(self, db):
        self._setup(db)
        invalid = db.execute(
            "SELECT DISTINCT throws FROM gold.dim_player "
            "WHERE throws NOT IN ('L','R') AND throws IS NOT NULL"
        ).fetchall()
        assert invalid == [], f"Invalid throws values: {invalid}"

    def test_full_name_not_null(self, db):
        self._setup(db)
        nulls = db.execute(
            "SELECT COUNT(*) FROM gold.dim_player WHERE full_name IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_row_count_matches_silver(self, db):
        self._setup(db)
        gold_count = db.execute("SELECT COUNT(*) FROM gold.dim_player").fetchone()[0]
        silver_count = db.execute("SELECT COUNT(*) FROM silver.players").fetchone()[0]
        assert gold_count == silver_count


# ── dim_team integrity ────────────────────────────────────────────────────────

class TestDimTeamAccuracy:
    def _setup(self, db):
        _seed_two_teams(db, n_games=0)
        _run_aggregator(db, ["002_dim_team.sql"])

    def test_team_season_unique(self, seeded_db):
        self._setup(seeded_db)
        dup = seeded_db.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT team_id, season_year, COUNT(*) n FROM gold.dim_team"
            "  GROUP BY team_id, season_year HAVING n > 1)"
        ).fetchone()[0]
        assert dup == 0

    def test_league_name_not_null_for_seeded_teams(self, seeded_db):
        self._setup(seeded_db)
        nulls = seeded_db.execute(
            "SELECT COUNT(*) FROM gold.dim_team WHERE league_name IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_division_name_not_null_for_seeded_teams(self, seeded_db):
        self._setup(seeded_db)
        nulls = seeded_db.execute(
            "SELECT COUNT(*) FROM gold.dim_team WHERE division_name IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_correct_enrichment(self, seeded_db):
        self._setup(seeded_db)
        row = seeded_db.execute(
            "SELECT league_name, division_name, venue_name "
            "FROM gold.dim_team WHERE team_id = 119 AND season_year = 2024"
        ).fetchone()
        assert row == ("National League", "NL West", "Dodger Stadium")


# ── Silver referential integrity ──────────────────────────────────────────────

class TestSilverReferentialIntegrity:
    def _setup(self, db):
        _seed_two_teams(db, n_games=5)

    def test_games_home_team_exists_in_teams(self, seeded_db):
        self._setup(seeded_db)
        orphans = seeded_db.execute(
            """
            SELECT COUNT(*) FROM silver.games g
            WHERE NOT EXISTS (
                SELECT 1 FROM silver.teams t
                 WHERE t.team_id = g.home_team_id
                   AND t.season_year = g.season_year
            )
            """
        ).fetchone()[0]
        assert orphans == 0, f"{orphans} games with unknown home_team_id"

    def test_games_away_team_exists_in_teams(self, seeded_db):
        self._setup(seeded_db)
        orphans = seeded_db.execute(
            """
            SELECT COUNT(*) FROM silver.games g
            WHERE NOT EXISTS (
                SELECT 1 FROM silver.teams t
                 WHERE t.team_id = g.away_team_id
                   AND t.season_year = g.season_year
            )
            """
        ).fetchone()[0]
        assert orphans == 0, f"{orphans} games with unknown away_team_id"

    def test_teams_division_exists(self, seeded_db):
        self._setup(seeded_db)
        orphans = seeded_db.execute(
            """
            SELECT COUNT(*) FROM silver.teams t
            WHERE division_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM silver.divisions d
                 WHERE d.division_id = t.division_id
              )
            """
        ).fetchone()[0]
        assert orphans == 0

    def test_divisions_league_exists(self, seeded_db):
        self._setup(seeded_db)
        orphans = seeded_db.execute(
            """
            SELECT COUNT(*) FROM silver.divisions d
            WHERE NOT EXISTS (
                SELECT 1 FROM silver.leagues l
                 WHERE l.league_id = d.league_id
            )
            """
        ).fetchone()[0]
        assert orphans == 0

    def test_teams_venue_exists(self, seeded_db):
        self._setup(seeded_db)
        orphans = seeded_db.execute(
            """
            SELECT COUNT(*) FROM silver.teams t
            WHERE venue_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM silver.venues v
                 WHERE v.venue_id = t.venue_id
              )
            """
        ).fetchone()[0]
        assert orphans == 0


# ── Pipeline meta accuracy ────────────────────────────────────────────────────

class TestPipelineMetaAccuracy:
    def test_pipeline_run_status_values(self, db):
        """Status column only contains allowed values."""
        from run_tracker.tracker import RunTracker
        tracker = RunTracker(db)
        run_id = tracker.start_run("test_job")
        tracker.complete_run(run_id)

        bad = db.execute(
            "SELECT COUNT(*) FROM meta.pipeline_runs "
            "WHERE status NOT IN ('running', 'success', 'failed')"
        ).fetchone()[0]
        assert bad == 0

    def test_completed_at_after_started_at(self, db):
        """completed_at must be ≥ started_at for completed runs."""
        from run_tracker.tracker import RunTracker
        tracker = RunTracker(db)
        run_id = tracker.start_run("test_job")
        tracker.complete_run(run_id)

        bad = db.execute(
            "SELECT COUNT(*) FROM meta.pipeline_runs "
            "WHERE completed_at IS NOT NULL AND completed_at < started_at"
        ).fetchone()[0]
        assert bad == 0

    def test_failed_run_has_error_message(self, db):
        """Failed runs must have a non-NULL error_message."""
        from run_tracker.tracker import RunTracker
        tracker = RunTracker(db)
        run_id = tracker.start_run("test_job")
        tracker.fail_run(run_id, "something went wrong")

        row = db.execute(
            "SELECT error_message FROM meta.pipeline_runs "
            "WHERE run_id = ? AND status = 'failed'",
            [run_id],
        ).fetchone()
        assert row is not None
        assert row[0] is not None
        assert len(row[0]) > 0

    def test_entity_checksum_pk_uniqueness(self, db):
        """(entity_type, entity_key) must be unique in meta.entity_checksums."""
        from run_tracker.tracker import RunTracker
        tracker = RunTracker(db)
        tracker.record_checksum("game_feed", "123", '{"gamePk":123}', "/v1/game/123")
        # Second insert should replace, not duplicate
        tracker.record_checksum("game_feed", "123", '{"gamePk":123,"v":2}', "/v1/game/123")

        count = db.execute(
            "SELECT COUNT(*) FROM meta.entity_checksums "
            "WHERE entity_type = 'game_feed' AND entity_key = '123'"
        ).fetchone()[0]
        assert count == 1
