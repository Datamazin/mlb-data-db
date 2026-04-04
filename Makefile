.PHONY: install test lint typecheck explore migrate fmt help

# ── Bootstrap ──────────────────────────────────────────────────────────────────
install:  ## Install all dependencies (creates .venv, installs from uv.lock)
	uv sync --all-groups

# ── Quality ────────────────────────────────────────────────────────────────────
test:  ## Run the full test suite
	uv run pytest

test-unit:  ## Run unit tests only
	uv run pytest tests/unit/

test-integration:  ## Run integration tests only (real DuckDB, no mocks)
	uv run pytest tests/integration/

lint:  ## Check code style with ruff
	uv run ruff check src/ tests/ scripts/

fmt:  ## Auto-fix formatting and imports
	uv run ruff check --fix src/ tests/ scripts/
	uv run ruff format src/ tests/ scripts/

typecheck:  ## Run mypy static type checking
	uv run mypy src/

# ── Pipeline ───────────────────────────────────────────────────────────────────
migrate:  ## Apply DuckDB schema migrations (creates data/gold/mlb.duckdb)
	uv run python migrations/migrate.py

migrate-dry:  ## Print migration SQL without executing
	uv run python migrations/migrate.py --dry-run

explore:  ## Validate Pydantic models against live MLB API (run before back-fill)
	uv run python scripts/explore_api.py

explore-date:  ## Probe a specific date: make explore-date DATE=2024-09-01
	uv run python scripts/explore_api.py --date $(DATE)

transform:  ## Run silver transformation (bronze → silver)
	uv run python -m src.transformer.transform

transform-force:  ## Re-run all silver transforms ignoring checksums
	uv run python -m src.transformer.transform --force

aggregate:  ## Run gold aggregation (silver → gold)
	uv run python -m src.aggregator.aggregate

aggregate-force:  ## Re-run all gold aggregations ignoring checksums
	uv run python -m src.aggregator.aggregate --force

streamlit:  ## Launch the Streamlit analytics frontend
	uv run streamlit run app.py

scheduler:  ## Start the pipeline scheduler daemon
	uv run python -m src.scheduler.jobs

run-nightly:  ## Trigger nightly incremental job once (yesterday by default)
	uv run python -m src.scheduler.jobs --run nightly_incremental

run-roster:  ## Trigger roster sync job once
	uv run python -m src.scheduler.jobs --run roster_sync

run-standings:  ## Trigger standings snapshot job once
	uv run python -m src.scheduler.jobs --run standings_snapshot

# ── Help ───────────────────────────────────────────────────────────────────────
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
