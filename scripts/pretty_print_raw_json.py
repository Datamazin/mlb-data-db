"""Pretty-print one raw_json payload from a parquet file.

Usage examples:
    uv run python scripts/pretty_print_raw_json.py
    uv run python scripts/pretty_print_raw_json.py \
      --parquet data/bronze/games/year=2026/month=04/games_20260401.parquet
    uv run python scripts/pretty_print_raw_json.py --row 3 --compact
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read raw_json from parquet and write pretty JSON to disk"
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=Path("data/bronze/games/year=2026/month=04/games_20260401.parquet"),
        help="Path to parquet file containing a raw_json column",
    )
    parser.add_argument(
        "--row",
        type=int,
        default=1,
        help="1-based row index to read from parquet (default: 1)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/debug/json/debug_raw_json.pretty.json"),
        help="Output JSON file path",
    )
    parser.add_argument(
        "--column",
        default="raw_json",
        help="Column name that contains JSON text (default: raw_json)",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Write compact JSON instead of indented output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.row < 1:
        raise SystemExit("--row must be >= 1")

    if not args.parquet.exists():
        raise SystemExit(f"Parquet file not found: {args.parquet}")

    offset = args.row - 1
    query = (
        f"SELECT {args.column} "
        f"FROM read_parquet(?) "
        f"LIMIT 1 OFFSET {offset}"
    )

    with duckdb.connect(database=":memory:") as conn:
        row = conn.execute(query, [str(args.parquet)]).fetchone()

    if not row or row[0] is None:
        raise SystemExit(
            f"No data found at row {args.row} for column '{args.column}' in {args.parquet}"
        )

    try:
        payload = json.loads(row[0])
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Column '{args.column}' is not valid JSON text: {exc}") from exc

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.compact:
        output_text = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    else:
        output_text = json.dumps(payload, indent=2, sort_keys=True)

    args.output.write_text(output_text + "\n", encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()