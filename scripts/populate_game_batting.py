"""
CLI backfill — populate silver.game_batting from all bronze game Parquet files.

Usage:
    uv run python scripts/populate_game_batting.py
    uv run python scripts/populate_game_batting.py --db path/to/mlb.duckdb
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb
from transformer.game_batting import populate_from_files

DEFAULT_DB = Path(os.getenv("GOLD_DB_PATH", "data/gold/mlb.duckdb"))
BRONZE = Path(os.getenv("BRONZE_PATH", "data/bronze")) / "games"


def run(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    parquet_files = sorted(BRONZE.glob("year=*/month=*/*.parquet"))
    if not parquet_files:
        print(f"No bronze game files found under {BRONZE}")
        conn.close()
        return

    print(f"Processing {len(parquet_files)} file(s) into silver.game_batting …")
    total = populate_from_files(conn, parquet_files)
    conn.close()
    print(f"\nDone — {total:,} batting rows written to silver.game_batting")


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate silver.game_batting from bronze.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()
    print(f"Database: {args.db}")
    run(args.db)


if __name__ == "__main__":
    main()
