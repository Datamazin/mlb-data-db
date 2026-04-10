"""
Silver transformation runner.

Executes sql/silver/*.sql scripts in alphabetical order, transforming
bronze Parquet files into the clean, typed silver DuckDB tables.

Features:
  - Checksum-based skip: unchanged scripts are not re-executed
  - --force flag to re-run all scripts regardless of checksum
  - Fail-and-continue: a script error is logged but does not abort later scripts
  - RunTracker integration for pipeline observability

Usage:
    uv run python -m src.transformer.transform
    uv run python -m src.transformer.transform --scripts 007_games.sql
    uv run python -m src.transformer.transform --force
    uv run python -m src.transformer.transform --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import structlog

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from run_tracker.tracker import RunTracker

log = structlog.get_logger(__name__)

SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "silver"

_TRACKING_DDL = """
CREATE TABLE IF NOT EXISTS meta._silver_transforms (
    script_name   VARCHAR      PRIMARY KEY,
    checksum      VARCHAR      NOT NULL,
    last_run_at   TIMESTAMPTZ  NOT NULL,
    rows_loaded   INTEGER      NOT NULL DEFAULT 0
);
"""


@dataclass
class TransformResult:
    scripts_run: int = 0
    total_rows_loaded: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _split_statements(sql: str) -> list[str]:
    """Split SQL text into individual statements on ';', ignoring '--' line comments."""
    import re
    sql_no_line_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in sql_no_line_comments.split(";") if s.strip()]


def _render_sql(template: str, bronze_path: str) -> str:
    """Substitute the {bronze_path} placeholder in SQL text using a literal replace
    so that other curly-brace patterns in SQL comments are not treated as placeholders."""
    return template.replace("{bronze_path}", bronze_path)


class Transformer:
    """
    Runs silver transform scripts against a DuckDB connection.

    Must be given the same connection that holds the silver schema so that
    INSERT OR REPLACE targets live tables.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, bronze_path: Path) -> None:
        self._conn = conn
        self._bronze_path = str(bronze_path)
        self._bootstrap()

    def _bootstrap(self) -> None:
        """Create the transform-tracking table if it doesn't exist."""
        self._conn.execute(_TRACKING_DDL)

    def _is_up_to_date(self, script_name: str, checksum: str) -> bool:
        row = self._conn.execute(
            "SELECT checksum FROM meta._silver_transforms WHERE script_name = ?",
            [script_name],
        ).fetchone()
        return row is not None and row[0] == checksum

    def _record(self, script_name: str, checksum: str, rows: int) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO meta._silver_transforms
                (script_name, checksum, last_run_at, rows_loaded)
            VALUES (?, ?, ?, ?)
            """,
            [script_name, checksum, datetime.now(UTC).isoformat(), rows],
        )

    def run_script(self, script_path: Path, dry_run: bool = False) -> int:
        """
        Execute a single SQL script. Returns the total number of rows affected.

        Scripts may contain multiple statements separated by semicolons. Each
        statement is executed in its own transaction so that a "No files found"
        result from one source (e.g. games bronze not yet extracted) does not
        abort subsequent statements in the same file.

        Raises on real errors so callers can decide how to handle failures.
        """
        sql_template = script_path.read_text()
        sql = _render_sql(sql_template, self._bronze_path)

        # Split into individual non-empty statements (comment-aware).
        statements = _split_statements(sql)

        if dry_run:
            log.info("transform_dry_run", script=script_path.name, sql_preview=sql[:120])
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
            except Exception as exc:
                cur.execute("ROLLBACK")
                if "No files found that match the pattern" in str(exc):
                    log.info("transform_stmt_no_data", script=script_path.name)
                else:
                    raise

        return total_rows

    def run(
        self,
        scripts: list[str] | None = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> TransformResult:
        """
        Execute silver transform scripts in alphabetical order.

        Args:
            scripts:  Optional list of specific script filenames to run.
                      If None, all *.sql files in sql/silver/ are run.
            force:    If True, re-run even if the script checksum hasn't changed.
            dry_run:  Print SQL without executing.

        Returns:
            TransformResult with counts and any per-script error messages.
        """
        paths = sorted(SQL_DIR / s for s in scripts) if scripts else sorted(SQL_DIR.glob("*.sql"))

        if not paths:
            log.warning("transform_no_scripts_found", sql_dir=str(SQL_DIR))
            return TransformResult()

        result = TransformResult()

        for path in paths:
            if not path.exists():
                msg = f"Script not found: {path}"
                log.error("transform_script_missing", path=str(path))
                result.errors.append(msg)
                continue

            checksum = _sha256(path.read_text())

            if not force and not dry_run and self._is_up_to_date(path.name, checksum):
                log.info("transform_skip_unchanged", script=path.name)
                continue

            log.info("transform_run_script", script=path.name, force=force)
            try:
                rows = self.run_script(path, dry_run=dry_run)
                if not dry_run:
                    self._record(path.name, checksum, rows)
                result.scripts_run += 1
                result.total_rows_loaded += rows
                log.info("transform_script_done", script=path.name, rows=rows)
            except Exception as exc:
                # "No files found" means the bronze partition is empty — treat as a
                # no-op so the runner stays idempotent when bronze data hasn't arrived yet.
                if "No files found that match the pattern" in str(exc):
                    log.info("transform_script_no_data", script=path.name)
                    if not dry_run:
                        self._record(path.name, checksum, 0)
                    result.scripts_run += 1
                else:
                    msg = f"{path.name}: {exc}"
                    log.error("transform_script_error", script=path.name, error=str(exc))
                    result.errors.append(msg)

        return result


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run silver transformation scripts (bronze → silver)."
    )
    parser.add_argument(
        "--db", type=Path, default=Path("data/gold/mlb.duckdb"),
        help="Path to DuckDB file (default: data/gold/mlb.duckdb)",
    )
    parser.add_argument(
        "--bronze", type=Path, default=Path("data/bronze"),
        help="Path to bronze Parquet root (default: data/bronze)",
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
    run_id = tracker.start_run("silver_transform")

    try:
        transformer = Transformer(conn, args.bronze)
        result = transformer.run(
            scripts=args.scripts,
            force=args.force,
            dry_run=args.dry_run,
        )

        if result.success:
            tracker.complete_run(run_id, records_loaded=result.total_rows_loaded)
            log.info(
                "transform_complete",
                scripts_run=result.scripts_run,
                rows_loaded=result.total_rows_loaded,
            )
        else:
            tracker.fail_run(run_id, "; ".join(result.errors))
            log.error("transform_finished_with_errors", errors=result.errors)
            sys.exit(1)

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
