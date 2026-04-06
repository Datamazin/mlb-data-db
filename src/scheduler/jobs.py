"""
M6 — Scheduled pipeline jobs.

Three recurring jobs:

  nightly_incremental   02:00 ET daily (Mar–Nov)
      Extract prior-day games, transform bronze→silver, aggregate silver→gold.

  roster_sync           06:00 ET daily
      Pull 40-man rosters + player bios for the current season;
      re-run silver transform so silver.players stays current.

  standings_snapshot    03:00 ET daily (Apr–Oct)
      Recompute gold.standings_snap from all Final regular-season games.

Each job:
  - Opens its own DuckDB connection (safe for single-process async use)
  - Records a RunTracker lifecycle (start → complete/fail)
  - Is idempotent — safe to re-run for the same date

Scheduler entry point:
    uv run python -m src.scheduler.jobs
    uv run python -m src.scheduler.jobs --run nightly_incremental
    uv run python -m src.scheduler.jobs --run roster_sync
    uv run python -m src.scheduler.jobs --run standings_snapshot
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
from src.logging_config import configure_logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extractor.client import MLBClient
from extractor.extract import extract_game_feeds, extract_players, extract_schedule, extract_teams
from extractor.writer import BronzeWriter
from run_tracker.tracker import RunTracker
from transformer.transform import Transformer
from aggregator.aggregate import Aggregator

log = structlog.get_logger(__name__)

DB_PATH = Path("data/gold/mlb.duckdb")
BRONZE_PATH = Path("data/bronze")

# Active season: the year whose games are currently being played.
# Updated each calendar year; used for roster/team extraction.
ACTIVE_SEASON = 2026

# Game types to extract for the nightly incremental job.
INCREMENTAL_GAME_TYPES = "R,F,D,L,W"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _open_db(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    import tempfile
    conn = duckdb.connect(str(db_path))
    # Set memory limit BELOW physical RAM so DuckDB spills to disk instead of
    # OOM-crashing. 6 GB leaves headroom for the OS and other processes.
    # temp_directory must be set before memory_limit takes effect for spilling.
    tmp = Path(tempfile.gettempdir()) / "duckdb_mlb"
    tmp.mkdir(parents=True, exist_ok=True)
    conn.execute(f"SET temp_directory='{tmp}'")
    conn.execute("SET memory_limit='6GB'")
    conn.execute("SET threads=4")
    return conn


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

    conn = _open_db(db_path)
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
        # year_glob: scope partition scans to the active season so we don't
        # re-read multi-GB historical game feeds on every nightly run.
        # force=True: script checksums don't change between runs, so without
        # force the transformer would skip all scripts.
        transformer = Transformer(conn, bronze_path, year_glob=str(target_date.year))
        transform_result = transformer.run(force=True)
        if not transform_result.success:
            raise RuntimeError(f"Transform failed: {transform_result.errors}")

        # 6 — Aggregate silver → gold
        aggregator = Aggregator(conn)
        agg_result = aggregator.run(force=True)
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

    conn = _open_db(db_path)
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

    conn = _open_db(db_path)
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

    return scheduler


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MLB pipeline scheduler — run as a long-lived daemon or trigger a single job."
    )
    parser.add_argument(
        "--run",
        choices=["nightly_incremental", "roster_sync", "standings_snapshot"],
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


async def _run_daemon() -> None:
    scheduler = build_scheduler()
    scheduler.start()
    log.info("scheduler_started", jobs=[j.id for j in scheduler.get_jobs()])
    try:
        await asyncio.Event().wait()  # block forever until cancelled
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        pass
    finally:
        scheduler.shutdown()
        log.info("scheduler_stopped")


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


def main() -> None:
    configure_logging()

    args = _parse_args()

    if args.run:
        asyncio.run(_run_once(args))
        return

    # Daemon mode — run scheduler until interrupted
    asyncio.run(_run_daemon())


if __name__ == "__main__":
    main()
