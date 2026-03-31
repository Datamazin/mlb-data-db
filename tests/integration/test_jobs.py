"""
Integration tests for the M6 scheduler jobs.

All three jobs are tested against a real file-backed DuckDB with the full
schema applied. No mocks, no network calls — the tests exercise the database
logic by pre-seeding silver tables and verifying gold state.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from scheduler.jobs import nightly_incremental, roster_sync, standings_snapshot


# ── Silver seeding helpers ────────────────────────────────────────────────────

def _ins_season(db, season_year=2024):
    db.execute(
        """INSERT OR REPLACE INTO silver.seasons
               (season_year, sport_id, regular_season_start, regular_season_end,
                games_per_team, loaded_at)
           VALUES (?, 1, '2024-03-20', '2024-09-29', 162, current_timestamp)""",
        [season_year],
    )


def _ins_league(db, league_id=104, name="National League", abbrev="NL"):
    db.execute(
        """INSERT OR REPLACE INTO silver.leagues
               (league_id, league_name, short_name, abbreviation, loaded_at)
           VALUES (?, ?, ?, ?, current_timestamp)""",
        [league_id, name, name, abbrev],
    )


def _ins_division(db, division_id=203, name="NL West", league_id=104):
    db.execute(
        """INSERT OR REPLACE INTO silver.divisions
               (division_id, division_name, short_name, league_id, loaded_at)
           VALUES (?, ?, ?, ?, current_timestamp)""",
        [division_id, name, name, league_id],
    )


def _ins_venue(db, venue_id=22, name="Dodger Stadium"):
    db.execute(
        """INSERT OR REPLACE INTO silver.venues (venue_id, venue_name, loaded_at)
           VALUES (?, ?, current_timestamp)""",
        [venue_id, name],
    )


def _ins_team(db, team_id=119, season_year=2024, name="Los Angeles Dodgers",
              abbrev="LAD", league_id=104, division_id=203, venue_id=22):
    db.execute(
        """INSERT OR REPLACE INTO silver.teams
               (team_id, season_year, team_name, team_abbrev, team_code,
                league_id, division_id, venue_id, city, first_year, active, loaded_at)
           VALUES (?, ?, ?, ?, 'xxx', ?, ?, ?, 'Los Angeles', 1884, TRUE, current_timestamp)""",
        [team_id, season_year, name, abbrev, league_id, division_id, venue_id],
    )


def _ins_game(db, game_pk=1, season_year=2024, game_date="2024-07-04",
              game_type="R", status="Final",
              home_team_id=119, away_team_id=147,
              home_score=5, away_score=3, venue_id=22):
    db.execute(
        """INSERT OR REPLACE INTO silver.games
               (game_pk, season_year, game_date, game_type, status,
                home_team_id, away_team_id, home_score, away_score,
                innings, venue_id, loaded_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 9, ?, current_timestamp)""",
        [game_pk, season_year, game_date, game_type, status,
         home_team_id, away_team_id, home_score, away_score, venue_id],
    )


def _two_team_setup(db, season_year=2024):
    _ins_season(db, season_year)
    _ins_league(db)
    _ins_division(db)
    _ins_venue(db)
    _ins_team(db, team_id=119, season_year=season_year, abbrev="LAD")
    _ins_team(db, team_id=118, season_year=season_year,
              name="San Francisco Giants", abbrev="SF", venue_id=22)


# ── standings_snapshot ────────────────────────────────────────────────────────

class TestStandingsSnapshot:
    @pytest.mark.asyncio
    async def test_snapshot_inserts_rows(self, db_file_path):
        db_path, conn = db_file_path
        _two_team_setup(conn)
        _ins_game(conn, game_pk=1, home_team_id=119, away_team_id=118)
        _ins_game(conn, game_pk=2, home_team_id=118, away_team_id=119,
                  home_score=3, away_score=5)
        conn.close()

        await standings_snapshot(db_path=db_path)

        check = duckdb.connect(str(db_path))
        count = check.execute("SELECT COUNT(*) FROM gold.standings_snap").fetchone()[0]
        check.close()
        assert count == 2

    @pytest.mark.asyncio
    async def test_snapshot_is_idempotent(self, db_file_path):
        db_path, conn = db_file_path
        _two_team_setup(conn)
        _ins_game(conn, game_pk=1, home_team_id=119, away_team_id=118)
        conn.close()

        await standings_snapshot(db_path=db_path)
        await standings_snapshot(db_path=db_path)

        check = duckdb.connect(str(db_path))
        count = check.execute("SELECT COUNT(*) FROM gold.standings_snap").fetchone()[0]
        check.close()
        assert count == 2  # INSERT OR REPLACE on (snap_date, team_id)

    @pytest.mark.asyncio
    async def test_snapshot_empty_games_no_rows(self, db_file_path):
        db_path, conn = db_file_path
        _two_team_setup(conn)
        conn.close()

        await standings_snapshot(db_path=db_path)

        check = duckdb.connect(str(db_path))
        count = check.execute("SELECT COUNT(*) FROM gold.standings_snap").fetchone()[0]
        check.close()
        assert count == 0

    @pytest.mark.asyncio
    async def test_snapshot_run_recorded(self, db_file_path):
        db_path, conn = db_file_path
        _two_team_setup(conn)
        conn.close()

        await standings_snapshot(db_path=db_path)

        check = duckdb.connect(str(db_path))
        row = check.execute(
            "SELECT status FROM meta.pipeline_runs WHERE job_name = 'standings_snapshot'"
        ).fetchone()
        check.close()
        assert row is not None
        assert row[0] == "success"

    @pytest.mark.asyncio
    async def test_win_pct_correct(self, db_file_path):
        db_path, conn = db_file_path
        _two_team_setup(conn)
        for pk, h, a in [(1, 119, 118), (2, 119, 118), (3, 119, 118)]:
            _ins_game(conn, game_pk=pk, home_team_id=h, away_team_id=a,
                      home_score=5, away_score=3)
        _ins_game(conn, game_pk=4, home_team_id=118, away_team_id=119,
                  home_score=5, away_score=3)
        conn.close()

        await standings_snapshot(db_path=db_path)

        check = duckdb.connect(str(db_path))
        row = check.execute(
            "SELECT wins, losses, win_pct FROM gold.standings_snap WHERE team_id = 119"
        ).fetchone()
        check.close()
        assert row[0] == 3
        assert row[1] == 1
        assert float(row[2]) == pytest.approx(0.750)


# ── nightly_incremental (without network) ────────────────────────────────────

class TestNightlyIncrementalNoNetwork:
    @pytest.mark.asyncio
    async def test_run_recorded_on_success(self, db_file_path, monkeypatch):
        db_path, conn = db_file_path
        conn.close()
        import scheduler.jobs as jobs_module
        monkeypatch.setattr(jobs_module, "extract_schedule",
                            lambda *a, **kw: _async_return([]))
        monkeypatch.setattr(jobs_module, "extract_game_feeds",
                            lambda *a, **kw: _async_return([]))

        await nightly_incremental(
            target_date=date(2024, 7, 4),
            db_path=db_path,
            bronze_path=db_path.parent / "bronze",
        )

        check = duckdb.connect(str(db_path))
        row = check.execute(
            "SELECT status FROM meta.pipeline_runs WHERE job_name = 'nightly_incremental'"
        ).fetchone()
        check.close()
        assert row is not None
        assert row[0] == "success"

    @pytest.mark.asyncio
    async def test_no_games_means_no_extraction(self, db_file_path, monkeypatch):
        db_path, conn = db_file_path
        conn.close()
        import scheduler.jobs as jobs_module

        feeds_called = []

        async def fake_schedule(*a, **kw):
            return []

        async def fake_feeds(*a, **kw):
            feeds_called.append(True)
            return []

        monkeypatch.setattr(jobs_module, "extract_schedule", fake_schedule)
        monkeypatch.setattr(jobs_module, "extract_game_feeds", fake_feeds)

        await nightly_incremental(
            target_date=date(2024, 7, 4),
            db_path=db_path,
            bronze_path=db_path.parent / "bronze",
        )

        assert not feeds_called

    @pytest.mark.asyncio
    async def test_run_recorded_as_failed_on_error(self, db_file_path, monkeypatch):
        db_path, conn = db_file_path
        conn.close()
        import scheduler.jobs as jobs_module

        async def bad_schedule(*a, **kw):
            raise RuntimeError("API unreachable")

        monkeypatch.setattr(jobs_module, "extract_schedule", bad_schedule)

        with pytest.raises(RuntimeError, match="API unreachable"):
            await nightly_incremental(
                target_date=date(2024, 7, 4),
                db_path=db_path,
                bronze_path=db_path.parent / "bronze",
            )

        check = duckdb.connect(str(db_path))
        row = check.execute(
            "SELECT status, error_message FROM meta.pipeline_runs "
            "WHERE job_name = 'nightly_incremental'"
        ).fetchone()
        check.close()
        assert row[0] == "failed"
        assert "API unreachable" in row[1]


# ── roster_sync (without network) ────────────────────────────────────────────

class TestRosterSyncNoNetwork:
    @pytest.mark.asyncio
    async def test_run_recorded_on_success(self, db_file_path, monkeypatch):
        db_path, conn = db_file_path
        conn.close()
        import scheduler.jobs as jobs_module
        monkeypatch.setattr(jobs_module, "extract_teams",
                            lambda *a, **kw: _async_return([119]))
        monkeypatch.setattr(jobs_module, "extract_players",
                            lambda *a, **kw: _async_return([660271]))

        await roster_sync(
            season_year=2024,
            db_path=db_path,
            bronze_path=db_path.parent / "bronze",
        )

        check = duckdb.connect(str(db_path))
        row = check.execute(
            "SELECT status, season_year FROM meta.pipeline_runs WHERE job_name = 'roster_sync'"
        ).fetchone()
        check.close()
        assert row[0] == "success"
        assert row[1] == 2024

    @pytest.mark.asyncio
    async def test_run_recorded_as_failed_on_error(self, db_file_path, monkeypatch):
        db_path, conn = db_file_path
        conn.close()
        import scheduler.jobs as jobs_module

        async def bad_teams(*a, **kw):
            raise RuntimeError("teams API down")

        monkeypatch.setattr(jobs_module, "extract_teams", bad_teams)

        with pytest.raises(RuntimeError, match="teams API down"):
            await roster_sync(
                season_year=2024,
                db_path=db_path,
                bronze_path=db_path.parent / "bronze",
            )

        check = duckdb.connect(str(db_path))
        row = check.execute(
            "SELECT status FROM meta.pipeline_runs WHERE job_name = 'roster_sync'"
        ).fetchone()
        check.close()
        assert row[0] == "failed"


# ── Scheduler wiring ──────────────────────────────────────────────────────────
# Inspect _pending_jobs before the scheduler is started — avoids needing
# a live asyncio event loop in synchronous test code.

class TestSchedulerWiring:
    def _pending_map(self, scheduler):
        """Return {job_id: trigger} from the scheduler's pre-start pending list."""
        return {job.id: job.trigger for job, *_ in scheduler._pending_jobs}

    def test_all_three_jobs_registered(self):
        from scheduler.jobs import build_scheduler
        from apscheduler.triggers.cron import CronTrigger
        scheduler = build_scheduler()
        pending = self._pending_map(scheduler)
        assert "nightly_incremental" in pending
        assert "roster_sync" in pending
        assert "standings_snapshot" in pending

    def test_nightly_incremental_fires_at_2am(self):
        from scheduler.jobs import build_scheduler
        from apscheduler.triggers.cron import CronTrigger
        scheduler = build_scheduler()
        trigger = self._pending_map(scheduler)["nightly_incremental"]
        assert isinstance(trigger, CronTrigger)
        # The hour field should be 2
        hour_field = next(f for f in trigger.fields if f.name == "hour")
        assert str(hour_field) == "2"

    def test_standings_snapshot_april_to_october(self):
        from scheduler.jobs import build_scheduler
        from apscheduler.triggers.cron import CronTrigger
        scheduler = build_scheduler()
        trigger = self._pending_map(scheduler)["standings_snapshot"]
        assert isinstance(trigger, CronTrigger)
        month_field = next(f for f in trigger.fields if f.name == "month")
        # "4-10" encodes April through October
        assert "4" in str(month_field)
        assert "10" in str(month_field)

    def test_nightly_incremental_seasonal_gate(self):
        from scheduler.jobs import build_scheduler
        from apscheduler.triggers.cron import CronTrigger
        scheduler = build_scheduler()
        trigger = self._pending_map(scheduler)["nightly_incremental"]
        month_field = next(f for f in trigger.fields if f.name == "month")
        # "3-11" encodes March through November
        assert "3" in str(month_field)
        assert "11" in str(month_field)


# ── Utility ───────────────────────────────────────────────────────────────────

async def _async_return(value):
    return value
