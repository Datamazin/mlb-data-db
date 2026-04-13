"""
M6/M7/M9 — Scheduled pipeline jobs.

Five recurring jobs:

  nightly_incremental   02:00 ET daily (Mar–Nov)
      Extract prior-day games, transform bronze→silver, aggregate silver→gold.

  roster_sync           06:00 ET daily
      Pull 40-man rosters + player bios for the current season;
      re-run silver transform so silver.players stays current.

  standings_snapshot    03:00 ET daily (Apr–Oct)
      Recompute gold.standings_snap from all Final regular-season games.

  distribution          04:30 ET daily
      Upload gold mlb.duckdb to Azure Blob and provision per-club SAS URLs.

  health_check          07:00 ET daily
      Verify SLA compliance, data freshness, and artifact integrity.
      Logs structured warnings for any breaches; records outcome in
      meta.pipeline_runs.

Each job:
  - Opens its own DuckDB connection (safe for single-process async use)
  - Records a RunTracker lifecycle (start → complete/fail)
  - Is idempotent — safe to re-run for the same date

Scheduler entry point:
    uv run python -m src.scheduler.jobs
    uv run python -m src.scheduler.jobs --run nightly_incremental
    uv run python -m src.scheduler.jobs --run nightly_incremental --date 2026-04-07
    uv run python -m src.scheduler.jobs --run roster_sync
    uv run python -m src.scheduler.jobs --run standings_snapshot
    uv run python -m src.scheduler.jobs --run distribution
    uv run python -m src.scheduler.jobs --run health_check
    uv run python -m src.scheduler.jobs --run catchup --start-date 2026-03-27 --end-date 2026-04-13
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from aggregator.aggregate import Aggregator
from distribution.sync import run_distribution
from extractor.client import MLBClient
from extractor.extract import extract_game_feeds, extract_players, extract_schedule, extract_teams
from extractor.writer import BronzeWriter
from monitoring.health import run_health_check
from run_tracker.tracker import RunTracker
from transformer.transform import Transformer

log = structlog.get_logger(__name__)

DB_PATH = Path("data/gold/mlb.duckdb")
BRONZE_PATH = Path("data/bronze")

# Active season: the year whose games are currently being played.
# Updated each calendar year; used for roster/team extraction.
ACTIVE_SEASON = 2026

# Game types to extract for the nightly incremental job.
INCREMENTAL_GAME_TYPES = "R,F,D,L,W"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _open_db() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


# ── Job: Nightly Incremental ───────────────────────────────────────────────────

async def nightly_incremental(
    target_date: date | None = None,
    db_path: Path = DB_PATH,
    bronze_path: Path = BRONZE_PATH,
) -> None:
    """
    Extract prior-day games, transform bronze→silver, aggregate silver→gold.

    Called at 02:00 ET daily so yesterday's completed games are included.
    If target_date is not provided, defaults to yesterday.
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    log.info("nightly_incremental_start", target_date=str(target_date))

    conn = duckdb.connect(str(db_path))
    tracker = RunTracker(conn)
    writer = BronzeWriter(bronze_path)
    run_id = tracker.start_run("nightly_incremental", target_date=target_date)

    try:
        async with MLBClient() as client:
            # 1 — Schedule for the target date
            season_year = target_date.year
            game_pks = await extract_schedule(
                client, writer,
                start_date=target_date,
                end_date=target_date,
                season_year=season_year,
                game_types=INCREMENTAL_GAME_TYPES,
            )

            # 2 — Filter already-extracted games
            new_pks = tracker.filter_unextracted(
                "game_feed", [str(pk) for pk in game_pks]
            )
            skipped = len(game_pks) - len(new_pks)
            if skipped:
                log.info("nightly_incremental_skip", skipped=skipped)

            extracted_count = 0
            if new_pks:
                # 3 — Fetch game feeds
                extracted_pks = await extract_game_feeds(
                    client, writer, [int(pk) for pk in new_pks]
                )
                extracted_count = len(extracted_pks)

                # 4 — Record checksums
                tracker.record_checksums_bulk(
                    "game_feed",
                    [
                        {
                            "entity_key": str(pk),
                            "raw_json": f'{{"gamePk":{pk}}}',
                            "source_url": f"/v1.1/game/{pk}/feed/live",
                        }
                        for pk in extracted_pks
                    ],
                )

        # 5 — Transform bronze → silver
        transformer = Transformer(conn, bronze_path)
        transform_result = transformer.run()
        if not transform_result.success:
            raise RuntimeError(f"Transform failed: {transform_result.errors}")

        # 6 — Aggregate silver → gold
        aggregator = Aggregator(conn)
        agg_result = aggregator.run()
        if not agg_result.success:
            raise RuntimeError(f"Aggregate failed: {agg_result.errors}")

        tracker.complete_run(
            run_id,
            records_extracted=extracted_count,
            records_loaded=transform_result.total_rows_loaded,
        )
        log.info(
            "nightly_incremental_done",
            target_date=str(target_date),
            games_found=len(game_pks),
            games_extracted=extracted_count,
            rows_loaded=transform_result.total_rows_loaded,
        )

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        log.error("nightly_incremental_failed", target_date=str(target_date), error=str(exc))
        raise
    finally:
        conn.close()


# ── Job: Catch-up ─────────────────────────────────────────────────────────────

async def catchup(
    start_date: date,
    end_date: date | None = None,
    db_path: Path = DB_PATH,
    bronze_path: Path = BRONZE_PATH,
) -> None:
    """
    Extract all games from start_date through end_date (inclusive), then run
    one transform + aggregate pass over all the newly loaded data.

    Used to back-fill missed days when the nightly schedule was not running.
    More efficient than calling nightly_incremental per day because
    transform/aggregate only execute once at the end.

    Example:
        --run catchup --start-date 2026-03-27 --end-date 2026-04-13
    """
    if end_date is None:
        end_date = date.today() - timedelta(days=1)

    log.info("catchup_start", start_date=str(start_date), end_date=str(end_date))

    conn = duckdb.connect(str(db_path))
    tracker = RunTracker(conn)
    writer = BronzeWriter(bronze_path)
    run_id = tracker.start_run("catchup", start_date=str(start_date), end_date=str(end_date))

    total_found = 0
    total_extracted = 0

    try:
        async with MLBClient() as client:
            current = start_date
            while current <= end_date:
                season_year = current.year
                game_pks = await extract_schedule(
                    client, writer,
                    start_date=current,
                    end_date=current,
                    season_year=season_year,
                    game_types=INCREMENTAL_GAME_TYPES,
                )
                total_found += len(game_pks)

                new_pks = tracker.filter_unextracted(
                    "game_feed", [str(pk) for pk in game_pks]
                )
                skipped = len(game_pks) - len(new_pks)
                if skipped:
                    log.info("catchup_skip_existing", date=str(current), skipped=skipped)

                if new_pks:
                    extracted_pks = await extract_game_feeds(
                        client, writer, [int(pk) for pk in new_pks]
                    )
                    total_extracted += len(extracted_pks)
                    tracker.record_checksums_bulk(
                        "game_feed",
                        [
                            {
                                "entity_key": str(pk),
                                "raw_json": f'{{"gamePk":{pk}}}',
                                "source_url": f"/v1.1/game/{pk}/feed/live",
                            }
                            for pk in extracted_pks
                        ],
                    )

                log.info("catchup_date_done", date=str(current), found=len(game_pks))
                current += timedelta(days=1)

        # One final transform + aggregate for all extracted data
        transformer = Transformer(conn, bronze_path)
        transform_result = transformer.run()
        if not transform_result.success:
            raise RuntimeError(f"Transform failed: {transform_result.errors}")

        aggregator = Aggregator(conn)
        agg_result = aggregator.run()
        if not agg_result.success:
            raise RuntimeError(f"Aggregate failed: {agg_result.errors}")

        tracker.complete_run(
            run_id,
            records_extracted=total_extracted,
            records_loaded=transform_result.total_rows_loaded,
        )
        log.info(
            "catchup_done",
            start_date=str(start_date),
            end_date=str(end_date),
            total_found=total_found,
            total_extracted=total_extracted,
            rows_loaded=transform_result.total_rows_loaded,
        )

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        log.error("catchup_failed", start_date=str(start_date), end_date=str(end_date), error=str(exc))
        raise
    finally:
        conn.close()


# ── Job: Roster Sync ───────────────────────────────────────────────────────────

async def roster_sync(
    season_year: int = ACTIVE_SEASON,
    db_path: Path = DB_PATH,
    bronze_path: Path = BRONZE_PATH,
) -> None:
    """
    Pull 40-man rosters and player biographies for the current season.

    Re-runs silver.teams and silver.players transforms so DL moves and
    trades are reflected in the silver layer within hours.

    Called at 06:00 ET daily.
    """
    log.info("roster_sync_start", season_year=season_year)

    conn = duckdb.connect(str(db_path))
    tracker = RunTracker(conn)
    writer = BronzeWriter(bronze_path)
    run_id = tracker.start_run("roster_sync", season_year=season_year)

    try:
        async with MLBClient() as client:
            # 1 — Teams (for division/league dimension freshness)
            team_ids = await extract_teams(client, writer, season_year)
            log.info("roster_sync_teams", count=len(team_ids))

            # 2 — Players / roster bios
            player_ids = await extract_players(client, writer, season_year)
            log.info("roster_sync_players", count=len(player_ids))

        # 3 — Re-run silver transforms for teams + players only
        transformer = Transformer(conn, bronze_path)
        result = transformer.run(
            scripts=["005_teams.sql", "006_players.sql"],
            force=True,  # always re-run; roster changes don't change script checksums
        )
        if not result.success:
            raise RuntimeError(f"Transform failed: {result.errors}")

        tracker.complete_run(
            run_id,
            records_extracted=len(player_ids),
            records_loaded=result.total_rows_loaded,
        )
        log.info(
            "roster_sync_done",
            season_year=season_year,
            teams=len(team_ids),
            players=len(player_ids),
            rows_loaded=result.total_rows_loaded,
        )

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        log.error("roster_sync_failed", season_year=season_year, error=str(exc))
        raise
    finally:
        conn.close()


# ── Job: Standings Snapshot ────────────────────────────────────────────────────

async def standings_snapshot(
    db_path: Path = DB_PATH,
) -> None:
    """
    Recompute gold.standings_snap from all Final regular-season games.

    Uses INSERT OR REPLACE so each call writes today's snapshot without
    duplicating prior days. Safe to run multiple times per day.

    Called at 03:00 ET daily (Apr–Oct).
    """
    log.info("standings_snapshot_start")

    conn = duckdb.connect(str(db_path))
    tracker = RunTracker(conn)
    run_id = tracker.start_run("standings_snapshot")

    try:
        aggregator = Aggregator(conn)
        result = aggregator.run(
            scripts=["008_standings_snap.sql"],
            force=True,  # standings must always recompute regardless of script checksum
        )
        if not result.success:
            raise RuntimeError(f"Standings aggregate failed: {result.errors}")

        tracker.complete_run(run_id, records_loaded=result.total_rows_affected)
        log.info("standings_snapshot_done", rows=result.total_rows_affected)

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        log.error("standings_snapshot_failed", error=str(exc))
        raise
    finally:
        conn.close()


# ── Job: Health Check ─────────────────────────────────────────────────────────

async def health_check(
    db_path: Path = DB_PATH,
) -> None:
    """
    Verify SLA compliance, data freshness, and artifact integrity.

    Runs after distribution (04:30) to confirm the full nightly pipeline
    completed successfully. Logs structured warnings for any breaches and
    records success/failure in meta.pipeline_runs.

    Called at 07:00 ET daily.
    """
    log.info("health_check_start")
    try:
        report = run_health_check(db_path=db_path)
        if report.healthy:
            log.info("health_check_done", checks=len(report.checks))
        else:
            for failure in report.failures:
                log.warning(
                    "health_check_sla_breach",
                    check=failure.name,
                    message=failure.message,
                )
    except Exception as exc:
        log.error("health_check_failed", error=str(exc))
        raise


# ── Job: Distribution ─────────────────────────────────────────────────────────

async def distribution(
    db_path: Path = DB_PATH,
    sas_expiry_hours: int = 24,
) -> None:
    """
    Upload gold mlb.duckdb to Azure Blob and provision per-club SAS URLs.

    Called at 04:30 ET daily, after nightly_incremental (02:00) and
    standings_snapshot (03:00) have completed.

    Requires AZURE_STORAGE_ACCOUNT_NAME to be set in the environment.
    If the variable is absent the job fails cleanly and records the error
    in meta.pipeline_runs without crashing the scheduler process.
    """
    log.info("distribution_start")
    conn = _open_db()
    try:
        result = run_distribution(
            db_path=db_path,
            duckdb_conn=conn,
            sas_expiry_hours=sas_expiry_hours,
        )
        if result.success:
            log.info(
                "distribution_done",
                clubs_synced=result.clubs_synced,
            )
        else:
            log.error("distribution_done_with_errors", errors=result.errors)
    except Exception as exc:
        log.error("distribution_failed", error=str(exc))
        raise
    finally:
        conn.close()


# ── Scheduler wiring ───────────────────────────────────────────────────────────

def build_scheduler() -> AsyncIOScheduler:
    """
    Construct and configure the APScheduler instance.

    Jobs run in US/Eastern time.  Seasonal gates are enforced by the cron
    expressions (month ranges) rather than in-job logic.
    """
    tz = "America/New_York"
    scheduler = AsyncIOScheduler()

    # Nightly Incremental — 02:00 ET, March–November
    scheduler.add_job(
        nightly_incremental,
        CronTrigger(hour=2, minute=0, month="3-11", timezone=tz),
        id="nightly_incremental",
        name="Nightly Incremental",
        misfire_grace_time=3600,  # allow 1-hour late start before skipping
        coalesce=True,
    )

    # Roster Sync — 06:00 ET, daily year-round
    scheduler.add_job(
        roster_sync,
        CronTrigger(hour=6, minute=0, timezone=tz),
        id="roster_sync",
        name="Roster Sync",
        misfire_grace_time=3600,
        coalesce=True,
    )

    # Standings Snapshot — 03:00 ET, April–October
    scheduler.add_job(
        standings_snapshot,
        CronTrigger(hour=3, minute=0, month="4-10", timezone=tz),
        id="standings_snapshot",
        name="Standings Snapshot",
        misfire_grace_time=3600,
        coalesce=True,
    )

    # Distribution — 04:30 ET, daily year-round
    scheduler.add_job(
        distribution,
        CronTrigger(hour=4, minute=30, timezone=tz),
        id="distribution",
        name="DuckDB Distribution",
        misfire_grace_time=3600,
        coalesce=True,
    )

    # Health Check — 07:00 ET, daily year-round
    scheduler.add_job(
        health_check,
        CronTrigger(hour=7, minute=0, timezone=tz),
        id="health_check",
        name="Pipeline Health Check",
        misfire_grace_time=3600,
        coalesce=True,
    )

    return scheduler


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MLB pipeline scheduler — run as a long-lived daemon or trigger a single job."
    )
    parser.add_argument(
        "--run",
        choices=[
            "nightly_incremental", "roster_sync", "standings_snapshot",
            "distribution", "health_check", "catchup",
        ],
        default=None,
        metavar="JOB",
        help="Run a single job immediately and exit (default: start the daemon)",
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Target date for nightly_incremental (default: yesterday)",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date for catchup (inclusive, required for catchup)",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="End date for catchup (inclusive, default: yesterday)",
    )
    parser.add_argument(
        "--season", type=int, default=ACTIVE_SEASON,
        help=f"Season year for roster_sync (default: {ACTIVE_SEASON})",
    )
    parser.add_argument(
        "--db", type=Path, default=DB_PATH,
        help=f"Path to DuckDB file (default: {DB_PATH})",
    )
    parser.add_argument(
        "--bronze", type=Path, default=BRONZE_PATH,
        help=f"Path to bronze Parquet root (default: {BRONZE_PATH})",
    )
    return parser.parse_args()


async def _run_once(args: argparse.Namespace) -> None:
    if args.run == "nightly_incremental":
        await nightly_incremental(
            target_date=args.date,
            db_path=args.db,
            bronze_path=args.bronze,
        )
    elif args.run == "roster_sync":
        await roster_sync(
            season_year=args.season,
            db_path=args.db,
            bronze_path=args.bronze,
        )
    elif args.run == "standings_snapshot":
        await standings_snapshot(db_path=args.db)
    elif args.run == "distribution":
        await distribution(db_path=args.db)
    elif args.run == "health_check":
        await health_check(db_path=args.db)
    elif args.run == "catchup":
        if args.start_date is None:
            raise ValueError("--start-date is required for the catchup job")
        await catchup(
            start_date=args.start_date,
            end_date=args.end_date,
            db_path=args.db,
            bronze_path=args.bronze,
        )


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO)

    args = _parse_args()

    if args.run:
        asyncio.run(_run_once(args))
        return

    # Daemon mode — run scheduler until interrupted
    scheduler = build_scheduler()
    scheduler.start()
    log.info("scheduler_started", jobs=[j.id for j in scheduler.get_jobs()])

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown()
        log.info("scheduler_stopped")


if __name__ == "__main__":
    main()
