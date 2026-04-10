"""
Integration tests for M9 — monitoring health checker and SLA validation.

All tests use real DuckDB (no mocks). Pipeline run rows are inserted directly
into meta.pipeline_runs to simulate various pipeline states.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from monitoring.health import HealthChecker, run_health_check
from monitoring.sla import GAME_FRESHNESS_HOURS, SLA_REGISTRY, SLASpec

# ── Helpers ────────────────────────────────────────────────────────────────────

NOW = datetime(2026, 3, 31, 8, 0, 0, tzinfo=UTC)   # fixed reference time


def _checker(db, db_path=None):
    return HealthChecker(db, db_path=db_path, now=NOW)


def _record_success(
    db, job_name: str, hours_ago: float = 1.0, base_now: datetime | None = None
) -> None:
    """Insert a successful pipeline run completed `hours_ago` hours before base_now."""
    ref = base_now or NOW
    completed_at = ref - timedelta(hours=hours_ago)
    started_at = completed_at - timedelta(minutes=5)
    db.execute(
        """
        INSERT INTO meta.pipeline_runs
            (run_id, job_name, status, started_at, completed_at,
             records_loaded, pipeline_version)
        VALUES (gen_random_uuid()::VARCHAR, ?, 'success', ?, ?, 10, '0.1.0')
        """,
        [job_name, started_at, completed_at],
    )


def _record_failure(db, job_name: str, hours_ago: float = 0.5) -> None:
    """Insert a failed pipeline run."""
    completed_at = NOW - timedelta(hours=hours_ago)
    started_at = completed_at - timedelta(minutes=2)
    db.execute(
        """
        INSERT INTO meta.pipeline_runs
            (run_id, job_name, status, started_at, completed_at,
             error_message, pipeline_version)
        VALUES (gen_random_uuid()::VARCHAR, ?, 'failed', ?, ?,
                'something went wrong', '0.1.0')
        """,
        [job_name, started_at, completed_at],
    )


def _ins_game(db, game_date: str, status: str = "Final") -> None:
    db.execute(
        """INSERT OR IGNORE INTO silver.seasons
               (season_year, sport_id, regular_season_start,
                regular_season_end, games_per_team, loaded_at)
           VALUES (2026, 1, '2026-03-27', '2026-10-01', 162, current_timestamp)""",
    )
    db.execute(
        """INSERT OR REPLACE INTO silver.games
               (game_pk, season_year, game_date, game_type, status,
                home_team_id, away_team_id, home_score, away_score,
                innings, loaded_at)
           VALUES (99999, 2026, ?, 'R', ?, 119, 147, 5, 3, 9, current_timestamp)""",
        [game_date, status],
    )


# ── SLA checks ────────────────────────────────────────────────────────────────

class TestSLACheck:
    def test_recent_success_passes(self, db):
        _record_success(db, "nightly_incremental", hours_ago=3)
        spec = SLASpec("nightly_incremental", max_age_hours=26, description="test")
        result = _checker(db).check_sla(spec)
        assert result.ok is True
        assert "OK" in result.message

    def test_stale_success_fails(self, db):
        _record_success(db, "nightly_incremental", hours_ago=30)
        spec = SLASpec("nightly_incremental", max_age_hours=26, description="test")
        result = _checker(db).check_sla(spec)
        assert result.ok is False
        assert "SLA breached" in result.message
        assert result.details["age_hours"] == pytest.approx(30, abs=0.2)

    def test_never_run_fails(self, db):
        spec = SLASpec("nightly_incremental", max_age_hours=26, description="test")
        result = _checker(db).check_sla(spec)
        assert result.ok is False
        assert "No successful run" in result.message
        assert result.details["last_success"] is None

    def test_failed_run_does_not_satisfy_sla(self, db):
        """A recent failed run must NOT count toward SLA compliance."""
        _record_failure(db, "nightly_incremental", hours_ago=0.5)
        spec = SLASpec("nightly_incremental", max_age_hours=26, description="test")
        result = _checker(db).check_sla(spec)
        assert result.ok is False

    def test_most_recent_success_counts(self, db):
        """Old success + newer success: only the newer one matters."""
        _record_success(db, "nightly_incremental", hours_ago=30)
        _record_success(db, "nightly_incremental", hours_ago=2)
        spec = SLASpec("nightly_incremental", max_age_hours=26, description="test")
        result = _checker(db).check_sla(spec)
        assert result.ok is True

    def test_sla_registry_has_four_jobs(self):
        job_names = {s.job_name for s in SLA_REGISTRY}
        assert "nightly_incremental" in job_names
        assert "roster_sync" in job_names
        assert "standings_snapshot" in job_names
        assert "distribution" in job_names
        assert len(SLA_REGISTRY) == 4


# ── Failed run detection ──────────────────────────────────────────────────────

class TestFailedRunCheck:
    def test_no_failures_passes(self, db):
        result = _checker(db).check_failed_runs()
        assert result.ok is True
        assert "No failed runs" in result.message

    def test_single_failure_detected(self, db):
        _record_failure(db, "nightly_incremental")
        result = _checker(db).check_failed_runs()
        assert result.ok is False
        assert result.details["failed"][0]["job_name"] == "nightly_incremental"

    def test_multiple_failures_reported(self, db):
        _record_failure(db, "nightly_incremental")
        _record_failure(db, "roster_sync")
        result = _checker(db).check_failed_runs()
        assert result.ok is False
        jobs = {r["job_name"] for r in result.details["failed"]}
        assert "nightly_incremental" in jobs
        assert "roster_sync" in jobs

    def test_success_after_failure_not_reported(self, db):
        """A subsequent success does not hide a prior failure."""
        _record_failure(db, "nightly_incremental", hours_ago=2)
        _record_success(db, "nightly_incremental", hours_ago=1)
        # The failed row is still in the table; check_failed_runs reports it
        result = _checker(db).check_failed_runs()
        assert result.ok is False


# ── Data freshness ────────────────────────────────────────────────────────────

class TestDataFreshnessCheck:
    def test_recent_game_passes(self, seeded_db):
        # Use today's date — midnight-to-NOW is only 8h, well within 30h threshold
        recent = NOW.strftime("%Y-%m-%d")
        _ins_game(seeded_db, recent)
        result = _checker(seeded_db).check_data_freshness()
        assert result.ok is True
        assert "ago" in result.message

    def test_stale_game_fails_in_season(self, seeded_db):
        stale = (NOW - timedelta(hours=GAME_FRESHNESS_HOURS + 10)).strftime("%Y-%m-%d")
        _ins_game(seeded_db, stale)
        result = _checker(seeded_db).check_data_freshness()
        assert result.ok is False
        assert "Stale" in result.message

    def test_no_games_fails_in_season(self, seeded_db):
        # March is in-season; no games → should fail
        result = _checker(seeded_db).check_data_freshness()
        assert result.ok is False
        assert "No Final games" in result.message

    def test_no_games_ok_in_offseason(self, db):
        # Simulate December (off-season)
        dec_now = datetime(2026, 12, 15, 8, 0, 0, tzinfo=UTC)
        checker = HealthChecker(db, db_path=None, now=dec_now)
        result = checker.check_data_freshness()
        assert result.ok is True
        assert "Off-season" in result.message

    def test_non_final_games_ignored(self, seeded_db):
        recent = (NOW - timedelta(hours=1)).strftime("%Y-%m-%d")
        _ins_game(seeded_db, recent, status="Postponed")
        # No Final games → fails in-season
        result = _checker(seeded_db).check_data_freshness()
        assert result.ok is False


# ── Artifact check ────────────────────────────────────────────────────────────

class TestArtifactCheck:
    def test_skipped_without_path(self, db):
        result = _checker(db, db_path=None).check_artifact()
        assert result.ok is True
        assert "skipped" in result.message

    def test_missing_file_fails(self, db, tmp_path):
        missing = tmp_path / "nonexistent.duckdb"
        checker = HealthChecker(db, db_path=missing, now=NOW)
        result = checker.check_artifact()
        assert result.ok is False
        assert "not found" in result.message

    def test_existing_file_passes(self, db, tmp_path):
        db_file = tmp_path / "mlb.duckdb"
        db_file.write_bytes(b"x" * 50_000)   # 50 KB — above min, below max
        checker = HealthChecker(db, db_path=db_file, now=NOW)
        result = checker.check_artifact()
        assert result.ok is True
        assert "MB" in result.message

    def test_empty_file_fails(self, db, tmp_path):
        db_file = tmp_path / "mlb.duckdb"
        db_file.write_bytes(b"")
        checker = HealthChecker(db, db_path=db_file, now=NOW)
        result = checker.check_artifact()
        assert result.ok is False
        assert "small" in result.message


# ── Row count sanity ──────────────────────────────────────────────────────────

class TestRowCountCheck:
    def test_no_silver_data_skipped(self, db):
        result = _checker(db).check_row_counts()
        assert result.ok is True
        assert "skipped" in result.message

    def test_silver_data_with_populated_gold_passes(self, seeded_db):
        # Seed enough to make gold views non-empty (seeded_db already has seasons)
        seeded_db.execute(
            """INSERT OR REPLACE INTO silver.leagues
                   (league_id, league_name, short_name, abbreviation, loaded_at)
               VALUES (104, 'NL', 'NL', 'NL', current_timestamp)"""
        )
        seeded_db.execute(
            """INSERT OR REPLACE INTO silver.divisions
                   (division_id, division_name, short_name, league_id, loaded_at)
               VALUES (203, 'NL West', 'NL West', 104, current_timestamp)"""
        )
        seeded_db.execute(
            """INSERT OR REPLACE INTO silver.venues
                   (venue_id, venue_name, loaded_at)
               VALUES (22, 'Dodger Stadium', current_timestamp)"""
        )
        seeded_db.execute(
            """INSERT OR REPLACE INTO silver.teams
                   (team_id, season_year, team_name, team_abbrev, team_code,
                    league_id, division_id, venue_id, city, first_year, active, loaded_at)
               VALUES (119, 2026, 'LAD', 'LAD', 'lad', 104, 203, 22,
                       'LA', 1884, TRUE, current_timestamp)"""
        )
        # Insert game directly (season 2026 already exists in seeded_db)
        seeded_db.execute(
            """INSERT OR REPLACE INTO silver.games
                   (game_pk, season_year, game_date, game_type, status,
                    home_team_id, away_team_id, home_score, away_score,
                    innings, loaded_at)
               VALUES (99999, 2026, '2026-03-30', 'R', 'Final',
                       119, 147, 5, 3, 9, current_timestamp)"""
        )

        # dim_player, dim_team, fact_game are views; they'll have rows after seeding
        from aggregator.aggregate import Aggregator
        Aggregator(seeded_db).run(
            scripts=["001_dim_player.sql", "002_dim_team.sql", "004_fact_game.sql"],
            force=True,
        )
        result = _checker(seeded_db).check_row_counts()
        assert result.ok is True


# ── Full HealthReport ─────────────────────────────────────────────────────────

class TestHealthReport:
    def test_all_slas_passing_report_healthy(self, db):
        for spec in SLA_REGISTRY:
            _record_success(db, spec.job_name, hours_ago=2)
        report = _checker(db).run()
        # SLA checks pass; other checks may warn but not fail
        sla_results = [c for c in report.checks if c.name.startswith("sla:")]
        assert all(c.ok for c in sla_results)

    def test_sla_breach_makes_report_unhealthy(self, db):
        # Only supply nightly_incremental success; rest are missing
        _record_success(db, "nightly_incremental", hours_ago=2)
        report = _checker(db).run()
        assert report.healthy is False
        assert len(report.failures) > 0

    def test_report_contains_all_check_categories(self, db):
        report = _checker(db).run()
        names = {c.name for c in report.checks}
        # SLA checks
        for spec in SLA_REGISTRY:
            assert f"sla:{spec.job_name}" in names
        # Non-SLA checks
        assert "failed_runs" in names
        assert "data_freshness" in names
        assert "artifact" in names
        assert "row_counts" in names

    def test_to_dict_is_json_serialisable(self, db):
        import json
        report = _checker(db).run()
        d = report.to_dict()
        serialised = json.dumps(d)
        assert '"healthy"' in serialised
        assert '"checks"' in serialised

    def test_failures_property_lists_failing_checks(self, db):
        report = _checker(db).run()
        for failure in report.failures:
            assert failure.ok is False


# ── run_health_check (RunTracker integration) ─────────────────────────────────

class TestRunHealthCheck:
    def test_unhealthy_records_failed_run(self, db_file_path):
        db_path, conn = db_file_path
        # No pipeline runs seeded → SLAs all breached → unhealthy
        report = run_health_check(db_path=db_path, duckdb_conn=conn, now=NOW)
        assert report.healthy is False
        row = conn.execute(
            "SELECT status FROM meta.pipeline_runs WHERE job_name = 'health_check'"
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"

    def test_healthy_records_success(self, db_file_path):
        db_path, conn = db_file_path
        # Use December to skip data_freshness failure; record runs relative to that time
        dec_now = datetime(2026, 12, 15, 8, 0, 0, tzinfo=UTC)
        for spec in SLA_REGISTRY:
            _record_success(conn, spec.job_name, hours_ago=1, base_now=dec_now)
        run_health_check(db_path=db_path, duckdb_conn=conn, now=dec_now)
        row = conn.execute(
            "SELECT status FROM meta.pipeline_runs WHERE job_name = 'health_check'"
        ).fetchone()
        assert row is not None
        assert row[0] == "success"


# ── Scheduler wiring ──────────────────────────────────────────────────────────

class TestHealthCheckSchedulerWiring:
    def _pending_map(self, scheduler):
        return {job.id: job.trigger for job, *_ in scheduler._pending_jobs}

    def test_health_check_registered(self):
        from scheduler.jobs import build_scheduler
        scheduler = build_scheduler()
        assert "health_check" in self._pending_map(scheduler)

    def test_health_check_fires_at_7am(self):
        from apscheduler.triggers.cron import CronTrigger

        from scheduler.jobs import build_scheduler
        scheduler = build_scheduler()
        trigger = self._pending_map(scheduler)["health_check"]
        assert isinstance(trigger, CronTrigger)
        hour_field = next(f for f in trigger.fields if f.name == "hour")
        assert str(hour_field) == "7"
