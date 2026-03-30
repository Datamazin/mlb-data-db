"""
Shared pytest fixtures for unit and integration tests.

Key design rule: integration tests use a REAL in-memory DuckDB instance,
never mocks. This ensures schema migrations and SQL transforms are validated
against actual DuckDB behaviour.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

# Path to the migration SQL files
MIGRATIONS_DIR = Path(__file__).parent.parent / "sql" / "schema"


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    """
    In-memory DuckDB connection with the full schema applied.

    Fresh for each test — changes do not persist between tests.
    Runs all migration SQL files from sql/schema/ in filename order.
    """
    conn = duckdb.connect(":memory:")
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        conn.execute(migration.read_text(encoding="utf-8"))
    yield conn
    conn.close()


@pytest.fixture
def db_file(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """
    File-backed DuckDB in a temp directory with the full schema applied.

    Use this when a test needs to exercise the migration runner itself,
    or when testing Parquet read_parquet() calls that need a real file path.
    """
    db_path = tmp_path / "test_mlb.duckdb"
    conn = duckdb.connect(str(db_path))
    for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
        conn.execute(migration.read_text(encoding="utf-8"))
    yield conn
    conn.close()


@pytest.fixture
def bronze_path(tmp_path: Path) -> Path:
    """Temporary bronze directory for Parquet writer tests."""
    path = tmp_path / "bronze"
    path.mkdir()
    return path
