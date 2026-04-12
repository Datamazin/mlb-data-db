# Copilot Instructions

## Commands

Use `uv`; do not install with `pip` or hand-edit `requirements.txt`.

```bash
make install            # uv sync --all-groups
make migrate            # apply DuckDB schema migrations to data/gold/mlb.duckdb
make transform          # run silver SQL transforms
make aggregate          # run gold SQL aggregations
make test               # full pytest suite
make test-unit          # unit tests only
make test-integration   # integration tests only
make lint               # ruff check
make fmt                # ruff check --fix + ruff format
make typecheck          # mypy
make streamlit          # launch the Streamlit frontend
```

For targeted pytest runs, use `uv run pytest` directly:

```bash
uv run pytest tests/unit/test_writer.py
uv run pytest tests/unit/test_writer.py::TestBronzeWriterGames::test_writes_to_correct_path
uv run pytest tests/integration/test_jobs.py::TestStandingsSnapshot::test_snapshot_inserts_rows
```

## High-level architecture

This repository is a medallion-style MLB data pipeline:

1. **Extraction (`src/extractor/`)** calls the public MLB Stats API with `httpx` + asyncio, validates responses with Pydantic models, and writes partitioned bronze Parquet under `data/bronze/`. `MLBClient` enforces the token-bucket rate limit and retry policy. Game feeds use `/v1.1/game/{gamePk}/feed/live`, while most other endpoints are `/v1/...`.
2. **Transformation (`src/transformer/` + `sql/silver/`)** loads bronze Parquet into typed DuckDB `silver` tables. The Python runner executes numbered SQL files in alphabetical order, substitutes `{bronze_path}`, `{year_glob}`, and `{month_glob}` placeholders, and records script checksums in `meta._silver_transforms`.
3. **Aggregation (`src/aggregator/` + `sql/gold/`)** builds the consumer-facing `gold` layer from `silver`. Gold scripts are also checksum-tracked (`meta._gold_aggregations`) and are intended to be the only schema downstream consumers query.
4. **State and idempotency (`src/run_tracker/`)** live inside the same DuckDB file. `meta.pipeline_runs` tracks job lifecycle; `meta.entity_checksums` is used to skip already-extracted entities and support safe re-runs.
5. **Orchestration (`src/scheduler/jobs.py`)** wires the nightly pipeline: extract prior-day data, populate silver stats, run silver transforms, run gold aggregations, then publish the updated `data/gold/mlb.duckdb`.
6. **Frontend (`app.py`, `pages/`)** is a Streamlit app that reads the gold layer from the DuckDB artifact with short-lived read-only connections.

## Key conventions

- Treat `data/gold/mlb.duckdb` as the main delivery artifact. Migrations, run tracking, silver tables, and gold tables all live in that file.
- `bronze` is raw-but-typed Parquet, not the main query surface. Writers store flattened columns **and** the full `raw_json` blob so later transforms can recover fields without re-extracting.
- Silver and gold loading is designed to be idempotent. The common pattern is `INSERT OR REPLACE` plus `QUALIFY ROW_NUMBER() OVER (...) ORDER BY extracted_at DESC` to keep the newest record.
- Integration tests use **real DuckDB connections**, not mocked databases. Reuse fixtures from `tests/conftest.py` (`db`, `db_file`, `db_file_path`, `seeded_db`) instead of replacing DuckDB behavior with mocks.
- SQL scripts are numbered and meant to run in filename order. If you add or change a transform/aggregation, keep the sequencing model instead of introducing ad hoc execution order in Python.
- Scheduled jobs often force transform/aggregate reruns even when SQL is unchanged because the underlying data changed; checksum skipping alone is not enough for recurring loads.
- Keep timestamps in UTC.
- The Streamlit app must not hold a long-lived cached DuckDB connection; a persistent lock blocks the writer side of the nightly pipeline.
