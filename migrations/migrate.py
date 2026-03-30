"""
Migration runner for mlb.duckdb.

Applies SQL migration files from sql/schema/ in order, tracking applied
migrations in a meta._schema_migrations table so each script runs exactly once.

Usage:
    python migrations/migrate.py                     # migrate to latest
    python migrations/migrate.py --db path/to/db     # custom DB path
    python migrations/migrate.py --dry-run           # print SQL, don't execute
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


MIGRATIONS_DIR = Path(__file__).parent.parent / "sql" / "schema"
DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "gold" / "mlb.duckdb"

TRACKING_DDL = """
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta._schema_migrations (
    migration_id    VARCHAR     PRIMARY KEY,   -- filename e.g. 001_initial_schema.sql
    checksum        VARCHAR     NOT NULL,      -- SHA-256 of file contents at apply time
    applied_at      TIMESTAMPTZ NOT NULL,
    applied_by      VARCHAR     NOT NULL       -- sys.argv[0] or caller identity
);
"""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _applied_migrations(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT migration_id FROM meta._schema_migrations ORDER BY migration_id"
        ).fetchall()
    }


def _pending_migrations(applied: set[str]) -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    return [f for f in files if f.name not in applied]


def run(db_path: Path, dry_run: bool = False) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    # Bootstrap the tracking table (safe to re-run)
    if not dry_run:
        conn.execute(TRACKING_DDL)

    applied = _applied_migrations(conn) if not dry_run else set()
    pending = _pending_migrations(applied)

    if not pending:
        print("No pending migrations.")
        conn.close()
        return

    for migration_file in pending:
        sql = migration_file.read_text(encoding="utf-8")
        checksum = _sha256(migration_file)

        print(f"  {'[DRY RUN] ' if dry_run else ''}Applying {migration_file.name} ...")

        if dry_run:
            print(sql)
            continue

        conn.execute("BEGIN")
        try:
            conn.execute(sql)
            conn.execute(
                """
                INSERT INTO meta._schema_migrations (migration_id, checksum, applied_at, applied_by)
                VALUES (?, ?, ?, ?)
                """,
                [
                    migration_file.name,
                    checksum,
                    datetime.now(timezone.utc),
                    sys.argv[0],
                ],
            )
            conn.execute("COMMIT")
            print(f"    OK — {migration_file.name}")
        except Exception as exc:
            conn.execute("ROLLBACK")
            print(f"    FAILED — {migration_file.name}: {exc}", file=sys.stderr)
            conn.close()
            sys.exit(1)

    conn.close()
    print("Migrations complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply DuckDB schema migrations.")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(os.getenv("GOLD_DB_PATH", str(DEFAULT_DB_PATH))),
        help="Path to the DuckDB file (default: data/gold/mlb.duckdb)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL without executing",
    )
    args = parser.parse_args()

    print(f"Database: {args.db}")
    print(f"Migrations dir: {MIGRATIONS_DIR}")
    run(db_path=args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
