"""
Historical back-fill job — seasons 2022 through 2025.

Extraction strategy (from the spec):
  - Parallelised by month within each season
  - Idempotent: games already in meta.entity_checksums are skipped
  - Seasons processed sequentially; months within a season run concurrently
  - A typical full back-fill of five seasons completes in under four hours
    using eight concurrent extractor workers

Entry point:
    python -m src.extractor.backfill
    python -m src.extractor.backfill --seasons 2022 2023
    python -m src.extractor.backfill --seasons 2024 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path

import duckdb
import structlog

from .client import MLBClient
from .extract import extract_game_feeds, extract_players, extract_schedule, extract_teams
from src.logging_config import configure_logging
from .writer import BronzeWriter

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from run_tracker.tracker import RunTracker

log = structlog.get_logger(__name__)

# ── Season date ranges (from the spec) ────────────────────────────────────────
# Back-fill covers the full calendar year so postseason is included.
# Spring Training (game_type='S') is extracted but flagged as opt-in.
SEASON_RANGES: dict[int, tuple[date, date]] = {
    2022: (date(2022, 3, 1), date(2022, 11, 10)),
    2023: (date(2023, 2, 1), date(2023, 11, 10)),
    2024: (date(2024, 2, 1), date(2024, 11, 5)),
    2025: (date(2025, 2, 1), date(2025, 11, 5)),
    2026: (date(2026, 3, 26), date.today() - timedelta(days=1)),
}

DEFAULT_SEASONS = [2022, 2023, 2024, 2025]
GAME_TYPES = "R,F,D,L,W,S"  # all game types including Spring Training


def _month_ranges(start: date, end: date) -> list[tuple[date, date]]:
    """
    Split a date range into (month_start, month_end) tuples.

    Example: 2024-03-20 → 2024-10-30  produces
        [(2024-03-20, 2024-03-31), (2024-04-01, 2024-04-30), ..., (2024-10-01, 2024-10-30)]
    """
    months: list[tuple[date, date]] = []
    cursor = start.replace(day=1)
    while cursor <= end:
        month_end_day = monthrange(cursor.year, cursor.month)[1]
        month_start = max(cursor, start)
        month_end = min(date(cursor.year, cursor.month, month_end_day), end)
        months.append((month_start, month_end))
        # advance to next month
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


async def _backfill_month(
    client: MLBClient,
    writer: BronzeWriter,
    tracker: RunTracker,
    season_year: int,
    month_start: date,
    month_end: date,
    month_sem: asyncio.Semaphore,
    dry_run: bool,
) -> tuple[int, int]:
    """
    Extract all games for one month. Returns (games_found, games_extracted).
    The month semaphore limits how many months run concurrently.
    """
    async with month_sem:
        log.info(
            "backfill_month_start",
            season=season_year,
            month=month_start.strftime("%Y-%m"),
        )

        if dry_run:
            log.info("backfill_dry_run_skip", month=month_start.strftime("%Y-%m"))
            return 0, 0

        # 1 — Fetch schedule, write to bronze/schedules/
        game_pks = await extract_schedule(
            client, writer, month_start, month_end, season_year, game_types=GAME_TYPES
        )

        if not game_pks:
            log.info("backfill_month_no_games", month=month_start.strftime("%Y-%m"))
            return 0, 0

        # 2 — Filter out already-extracted games (idempotency)
        new_pks = tracker.filter_unextracted(
            "game_feed", [str(pk) for pk in game_pks]
        )
        skipped = len(game_pks) - len(new_pks)
        if skipped:
            log.info("backfill_month_skip_existing", skipped=skipped)

        if not new_pks:
            return len(game_pks), 0

        # 3 — Fetch game feeds, write to bronze/games/
        extracted_pks = await extract_game_feeds(
            client, writer, [int(pk) for pk in new_pks]
        )

        # 4 — Record checksums for successfully extracted games
        checksum_entries = [
            {
                "entity_key": str(pk),
                "raw_json": f'{{"gamePk":{pk}}}',  # placeholder; real hash from file
                "source_url": f"/v1.1/game/{pk}/feed/live",
            }
            for pk in extracted_pks
        ]
        tracker.record_checksums_bulk("game_feed", checksum_entries)

        log.info(
            "backfill_month_done",
            season=season_year,
            month=month_start.strftime("%Y-%m"),
            found=len(game_pks),
            extracted=len(extracted_pks),
        )
        return len(game_pks), len(extracted_pks)


async def backfill_season(
    client: MLBClient,
    writer: BronzeWriter,
    tracker: RunTracker,
    season_year: int,
    dry_run: bool = False,
    month_concurrency: int = 3,
) -> dict[str, int]:
    """
    Back-fill a single season.

    Months run concurrently (up to month_concurrency at once). Within each
    month, game feed requests are handled by extract_game_feeds' own semaphore.

    Returns summary dict with total_found and total_extracted.
    """
    if season_year not in SEASON_RANGES:
        raise ValueError(f"No date range configured for season {season_year}. "
                         f"Valid seasons: {sorted(SEASON_RANGES)}")

    season_start, season_end = SEASON_RANGES[season_year]
    months = _month_ranges(season_start, season_end)

    log.info(
        "backfill_season_start",
        season=season_year,
        months=len(months),
        dry_run=dry_run,
    )

    run_id = tracker.start_run("backfill", season_year=season_year)
    month_sem = asyncio.Semaphore(month_concurrency)

    tasks = [
        asyncio.create_task(
            _backfill_month(
                client, writer, tracker, season_year,
                ms, me, month_sem, dry_run,
            )
        )
        for ms, me in months
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_found = total_extracted = 0
    errors = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            errors.append((months[i], result))
            log.error("backfill_month_error", month=months[i][0].strftime("%Y-%m"), error=str(result))
        else:
            found, extracted = result
            total_found += found
            total_extracted += extracted

    if errors:
        error_msg = "; ".join(f"{m[0].strftime('%Y-%m')}: {e}" for m, e in errors)
        tracker.fail_run(run_id, error_msg)
    else:
        tracker.complete_run(run_id, records_extracted=total_extracted, records_loaded=0)

    summary = {
        "season_year": season_year,
        "months_processed": len(months),
        "total_found": total_found,
        "total_extracted": total_extracted,
        "errors": len(errors),
    }
    log.info("backfill_season_done", **summary)
    return summary


async def run_backfill(
    db_path: Path,
    bronze_path: Path,
    seasons: list[int] = DEFAULT_SEASONS,
    dry_run: bool = False,
    month_concurrency: int = 3,
) -> None:
    """
    Top-level back-fill entry point.

    Also extracts teams and players for each season (dimension tables),
    then processes game feeds season by season.
    """
    conn = duckdb.connect(str(db_path))
    tracker = RunTracker(conn)
    writer = BronzeWriter(bronze_path)

    log.info("backfill_start", seasons=seasons, dry_run=dry_run)

    async with MLBClient() as client:
        for season_year in sorted(seasons):
            # Dimensions first — teams and players are needed for silver joins
            if not dry_run:
                log.info("backfill_teams", season=season_year)
                await extract_teams(client, writer, season_year)

                log.info("backfill_players", season=season_year)
                await extract_players(client, writer, season_year)

            # Game feeds — the bulk of the work
            await backfill_season(
                client, writer, tracker, season_year,
                dry_run=dry_run,
                month_concurrency=month_concurrency,
            )

    conn.close()
    log.info("backfill_complete", seasons=seasons)


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Historical back-fill: extract MLB Stats API data for past seasons."
    )
    parser.add_argument(
        "--seasons", nargs="+", type=int, default=DEFAULT_SEASONS,
        metavar="YEAR",
        help=f"Seasons to back-fill (default: {DEFAULT_SEASONS})",
    )
    parser.add_argument(
        "--db", type=Path,
        default=Path("data/gold/mlb.duckdb"),
        help="Path to DuckDB file (default: data/gold/mlb.duckdb)",
    )
    parser.add_argument(
        "--bronze", type=Path,
        default=Path("data/bronze"),
        help="Path to bronze Parquet directory (default: data/bronze)",
    )
    parser.add_argument(
        "--month-concurrency", type=int, default=3,
        help="Max months processed concurrently per season (default: 3)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be extracted without making API calls",
    )
    return parser.parse_args()


if __name__ == "__main__":
    configure_logging()

    args = _parse_args()
    asyncio.run(
        run_backfill(
            db_path=args.db,
            bronze_path=args.bronze,
            seasons=args.seasons,
            dry_run=args.dry_run,
            month_concurrency=args.month_concurrency,
        )
    )
