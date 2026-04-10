"""
Gold aggregation runner.

Executes sql/gold/*.sql scripts in alphabetical order, building gold views
and materialized tables from the clean silver DuckDB tables.

Features:
  - Checksum-based skip: unchanged scripts are not re-executed
  - --force flag to re-run all scripts regardless of checksum
  - Fail-and-continue: a script error is logged but does not abort later scripts
  - RunTracker integration for pipeline observability

Usage:
    uv run python -m src.aggregator.aggregate
    uv run python -m src.aggregator.aggregate --scripts 008_standings_snap.sql
    uv run python -m src.aggregator.aggregate --force
    uv run python -m src.aggregator.aggregate --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import structlog

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from run_tracker.tracker import RunTracker

log = structlog.get_logger(__name__)

SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "gold"

_TRACKING_DDL = """
CREATE TABLE IF NOT EXISTS meta._gold_aggregations (
    script_name   VARCHAR      PRIMARY KEY,
    checksum      VARCHAR      NOT NULL,
    last_run_at   TIMESTAMPTZ  NOT NULL,
    rows_affected INTEGER      NOT NULL DEFAULT 0
);
"""


@dataclass
class AggregateResult:
    scripts_run: int = 0
    total_rows_affected: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _split_statements(sql: str) -> list[str]:
    """Split SQL text into individual statements on ';', ignoring '--' line comments."""
    sql_no_line_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in sql_no_line_comments.split(";") if s.strip()]


class Aggregator:
    """
    Runs gold aggregation scripts against a DuckDB connection.

    Scripts may contain CREATE OR REPLACE VIEW statements (dimension/fact views)
    or INSERT OR REPLACE INTO statements for materialized tables (standings_snap,
    league_averages).
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self._bootstrap()

    def _bootstrap(self) -> None:
        """Create the aggregation-tracking table if it doesn't exist."""
        self._conn.execute(_TRACKING_DDL)

    def _is_up_to_date(self, script_name: str, checksum: str) -> bool:
        row = self._conn.execute(
            "SELECT checksum FROM meta._gold_aggregations WHERE script_name = ?",
            [script_name],
        ).fetchone()
        return row is not None and row[0] == checksum

    def _record(self, script_name: str, checksum: str, rows: int) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO meta._gold_aggregations
                (script_name, checksum, last_run_at, rows_affected)
            VALUES (?, ?, ?, ?)
            """,
            [script_name, checksum, datetime.now(UTC).isoformat(), rows],
        )

    def run_script(self, script_path: Path, dry_run: bool = False) -> int:
        """
        Execute a single SQL script. Returns the total number of rows affected.

        Each statement is executed in its own transaction. CREATE OR REPLACE VIEW
        statements return 0 rows (rowcount = -1 or 0), which is recorded as-is.
        """
        sql = script_path.read_text()
        statements = _split_statements(sql)

        if dry_run:
            log.info("aggregate_dry_run", script=script_path.name, sql_preview=sql[:120])
            return 0

        total_rows = 0
        for stmt in statements:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                cur.execute(stmt)
                rows = cur.rowcount if (cur.rowcount is not None and cur.rowcount >= 0) else 0
                cur.execute("COMMIT")
                total_rows += rows
            except Exception:
                cur.execute("ROLLBACK")
                raise

        return total_rows

    def run(
        self,
        scripts: list[str] | None = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> AggregateResult:
        """
        Execute gold aggregation scripts in alphabetical order.

        Args:
            scripts:  Optional list of specific script filenames to run.
                      If None, all *.sql files in sql/gold/ are run.
            force:    If True, re-run even if the script checksum hasn't changed.
            dry_run:  Print SQL without executing.

        Returns:
            AggregateResult with counts and any per-script error messages.
        """
        paths = sorted(SQL_DIR / s for s in scripts) if scripts else sorted(SQL_DIR.glob("*.sql"))

        if not paths:
            log.warning("aggregate_no_scripts_found", sql_dir=str(SQL_DIR))
            return AggregateResult()

        result = AggregateResult()

        for path in paths:
            if not path.exists():
                msg = f"Script not found: {path}"
                log.error("aggregate_script_missing", path=str(path))
                result.errors.append(msg)
                continue

            checksum = _sha256(path.read_text())

            if not force and not dry_run and self._is_up_to_date(path.name, checksum):
                log.info("aggregate_skip_unchanged", script=path.name)
                continue

            log.info("aggregate_run_script", script=path.name, force=force)
            try:
                rows = self.run_script(path, dry_run=dry_run)
                if not dry_run:
                    self._record(path.name, checksum, rows)
                result.scripts_run += 1
                result.total_rows_affected += rows
                log.info("aggregate_script_done", script=path.name, rows=rows)
            except Exception as exc:
                msg = f"{path.name}: {exc}"
                log.error("aggregate_script_error", script=path.name, error=str(exc))
                result.errors.append(msg)

        return result


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run gold aggregation scripts (silver → gold)."
    )
    parser.add_argument(
        "--db", type=Path, default=Path("data/gold/mlb.duckdb"),
        help="Path to DuckDB file (default: data/gold/mlb.duckdb)",
    )
    parser.add_argument(
        "--scripts", nargs="+", default=None, metavar="SCRIPT",
        help="Specific script filename(s) to run (default: all)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-run all scripts even if checksum is unchanged",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print SQL without executing",
    )
    return parser.parse_args()


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO)

    args = _parse_args()

    conn = duckdb.connect(str(args.db))
    tracker = RunTracker(conn)
    run_id = tracker.start_run("gold_aggregate")

    try:
        aggregator = Aggregator(conn)
        result = aggregator.run(
            scripts=args.scripts,
            force=args.force,
            dry_run=args.dry_run,
        )

        if result.success:
            tracker.complete_run(run_id, records_loaded=result.total_rows_affected)
            log.info(
                "aggregate_complete",
                scripts_run=result.scripts_run,
                rows_affected=result.total_rows_affected,
            )
        else:
            tracker.fail_run(run_id, "; ".join(result.errors))
            log.error("aggregate_finished_with_errors", errors=result.errors)
            sys.exit(1)

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
