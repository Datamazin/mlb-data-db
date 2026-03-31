# MLB Stats Data Pipeline — Project Guide

## Project Overview

An enterprise-grade ETL pipeline that ingests data from the official MLB Stats API, transforms it into analytics-ready datasets, and persists it in a DuckDB analytical store. Covers **seasons 2022–2026** (Regular Season, Postseason, Spring Training). Downstream consumers are the 30 MLB club data science departments.

**Key constraint:** No API key required; rate limit is ~10 req/s per IP. Respect MLB Terms of Service.

---

## Architecture: Medallion Pattern

```
MLB Stats API  →  EXTRACTION (Python/httpx)  →  /data/bronze/  (raw Parquet)
                                                       ↓
                                          TRANSFORMATION (DuckDB SQL)  →  silver schema
                                                       ↓
                                          AGGREGATION (DuckDB SQL)  →  gold schema
                                                       ↓
                                          DISTRIBUTION  →  per-club Azure Blob Storage
```

Single delivery artifact: `mlb.duckdb` — one portable DuckDB file clubs embed locally.

---

## Technology Stack

| Technology | Role |
|---|---|
| Python 3.12 | Pipeline language (extraction, orchestration) |
| DuckDB 1.x | Embedded OLAP engine for transformation and serving |
| httpx + asyncio | Async HTTP client for concurrent API calls |
| Pydantic v2 | Schema validation and type coercion for API responses |
| Apache Parquet (Snappy) | Columnar storage for bronze and silver layers |
| APScheduler | In-process task scheduling |
| azure-storage-blob | Distribution of gold DuckDB artifact |
| pytest | Unit and integration testing |

---

## Directory Structure

```
mlb-data-db/
├── src/
│   ├── extractor/          # MLB API client, rate limiter, Pydantic models, Parquet writers
│   ├── transformer/        # DuckDB SQL scripts: bronze → silver
│   ├── aggregator/         # DuckDB SQL scripts: silver → gold views
│   ├── scheduler/          # APScheduler job definitions
│   ├── distribution/       # Azure Blob sync scripts
│   └── run_tracker/        # Pipeline run state / idempotency
├── sql/
│   ├── schema/             # DuckDB schema DDL (bronze, silver, gold, meta)
│   ├── silver/             # Silver transformation SQL scripts
│   └── gold/               # Gold view/table SQL scripts
├── data/
│   ├── bronze/             # Raw Parquet (partitioned: entity/year=YYYY/month=MM/)
│   ├── silver/             # (managed by DuckDB, not raw files)
│   └── gold/
│       └── mlb.duckdb      # The delivery artifact
├── tests/
│   ├── unit/
│   └── integration/        # Must use real DuckDB, not mocks
├── migrations/             # Alembic-style DuckDB schema migration scripts
└── CLAUDE.md
```

---

## MLB Stats API

- **Base URL:** `https://statsapi.mlb.com/api`
- **Version:** v1
- **Auth:** None required for public endpoints
- **Rate limit:** ~10 req/s per IP (pipeline uses 8 req/s with token bucket, leaving 20% headroom)
- **User-Agent:** `MLB-DataPipeline/1.0 (MLB Data Engineering; dataeng@mlb.com)`

### Key Endpoints

| Endpoint | Description |
|---|---|
| `/v1/seasons` | Season metadata |
| `/v1/schedule` | Schedule with gamePk identifiers |
| `/v1/game/{gamePk}/feed/live` | Full game feed (play-by-play, stats) |
| `/v1/game/{gamePk}/boxscore` | Structured boxscore |
| `/v1/teams` | Team/division/league/venue info |
| `/v1/teams/{teamId}/roster` | 40-man or active roster |
| `/v1/people/{personId}` | Player biographical details |
| `/v1/people/{personId}/stats` | Player hitting/pitching/fielding stats |
| `/v1/stats` | Aggregated leaderboard stats |
| `/v1/standings` | Division and wildcard standings |
| `/v1/venues/{venueId}` | Venue details |
| `/v1/sports/1/players` | Full player universe for a season |

---

## DuckDB Schema Layout

```
mlb.duckdb
├── schema: bronze      -- Raw API JSON as typed columns (pipeline-internal)
│   └── raw_api_responses
├── schema: silver      -- Cleaned, typed, deduplicated entities
│   ├── seasons         ├── teams           ├── venues
│   ├── divisions       ├── leagues         ├── players
│   ├── games           ├── game_linescore  ├── game_boxscore
│   ├── at_bats         ├── pitches         └── fielding_events
│   ├── fact_batting    └── fact_pitching
├── schema: gold        -- Aggregated & fan-ready (PRIMARY consumer layer)
│   ├── dim_player      ├── dim_team        ├── dim_venue
│   ├── fact_game       ├── fact_batting    ├── fact_pitching
│   ├── fact_fielding   ├── fact_pitch_mix  ├── fact_sprint_speed
│   ├── standings_snap  ├── leaderboards    ├── player_season_summary
│   ├── league_averages └── head_to_head
└── schema: meta        -- Pipeline run tracking & data lineage
    ├── pipeline_runs   └── entity_checksums
```

**Rule:** Club data scientists query `gold` schema only. `bronze` and `silver` are pipeline-internal.

---

## Pipeline Behavior

### Extraction (Bronze)
- Concurrency: 8 simultaneous requests (token-bucket rate limiter at 8 req/s)
- Retry: Exponential backoff with jitter on HTTP 429 and 5xx; max 5 retries
- Idempotency: Fingerprint per entity (type + date + API response hash) in `meta.entity_checksums`
- Partitioning: `bronze/games/year=2024/month=07/games_20240704.parquet`
- Compression: Snappy-compressed Parquet

### Transformation (Silver)
- Type casting: String-encoded numbers cast to DECIMAL; dates normalized to DATE
- Deduplication: `QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY loaded_at DESC) = 1`
- Null handling: Missing API fields → NULL; critical fields flagged
- SCD Type 1: Players and teams use replace semantics (latest values win)

### Aggregation (Gold)
- `gold.leaderboards` — Top-N rankings recalculated nightly during active season
- `gold.player_season_summary` — One row per player-season with advanced metrics
- `gold.standings_snap` — Daily standings snapshot with GB, streak, last-10
- `gold.head_to_head` — Career and season records between any two teams

---

## Scheduling

| Job | Schedule | Description |
|---|---|---|
| Nightly Incremental | 02:00 ET daily (Mar–Nov) | Extract prior day's games; transform + reload gold |
| Roster Sync | 06:00 ET daily | Pull 40-man rosters; catch trades and DL moves |
| Standings Snapshot | 03:00 ET daily (Apr–Oct) | Capture daily standings |
| Historical Back-fill | On-demand (one-time) | Replay seasons 2022–2025 |
| Stat Correction Patch | Monday 04:00 ET weekly | Re-pull MLB-corrected official stats |
| DuckDB Distribution | 04:30 ET daily | Sync gold mlb.duckdb to per-club Azure Blob |

---

## Data Volume

| Entity | Per Season | 5-Season Total |
|---|---|---|
| Games | ~4,950 | ~24,750 |
| At-bats | ~175,000 | ~875,000 |
| Bronze Parquet size | ~8 GB | ~40 GB |
| Gold DuckDB file | ~1.2 GB | ~6 GB compressed |

---

## Season Coverage

| Season | Date Range | Game Types |
|---|---|---|
| 2022 | April 7 – Nov 5 | Regular, WC, DS, CS, WS |
| 2023 | March 30 – Nov 4 | Regular, WC, DS, CS, WS |
| 2024 | March 20 – Oct 30 | Regular, WC, DS, CS, WS |
| 2025 | March 27 – Oct 29 | Regular, WC, DS, CS, WS |
| 2026 | March 26 – TBD | Regular (in progress), Postseason TBD |

Spring Training (`game_type = 'S'`) is captured but excluded from official stats by default.

### Game Type Codes
`S` = Spring Training, `R` = Regular Season, `F` = Wild Card, `D` = Division Series, `L` = Championship Series, `W` = World Series

---

## Implementation Roadmap

| Milestone | Phase | Deliverable |
|---|---|---|
| M1 | Infrastructure Setup | Azure Blob, Managed Identities, RBAC, DuckDB schema DDL, CI/CD |
| M2 | Extractor v1 | MLB API client, rate limiter, Pydantic models, bronze Parquet writers |
| M3 | Historical Back-fill | Full extraction + transformation of 2022–2025 |
| M4 | Silver Transformation | DuckDB SQL silver scripts, deduplication, type casting |
| M5 | Gold Aggregation | Fan-facing gold tables, leaderboards, player_season_summary |
| M6 | 2026 Incremental | Nightly scheduler, roster sync, standings snapshots |
| M7 | Distribution + Onboarding | Per-club Azure Blob sync, SAS tokens, access docs, sample queries |
| M8 | QA & Performance | Data accuracy validation, query benchmarks, load testing |
| M9 | Production Launch | All 30 clubs onboarded, monitoring live, SLA defined |

---

## Security & Compliance

- API requests honour `Retry-After` headers; back off on HTTP 429
- No scraping of premium video/audio or paid-content endpoints
- Bronze and gold data stored in Azure Blob with AES-256 SSE
- Per-club containers use Azure RBAC scoped to each club's Entra ID service principal
- No PII beyond publicly disclosed player biography data
- All data in transit uses TLS 1.2+
- Service credentials via Azure Managed Identities (no long-lived secrets)

---

## Environment Setup

**Package manager:** [uv](https://docs.astral.sh/uv/) — do not use `pip` directly.
**Python version:** 3.12 (pinned in `.python-version`).
**Lockfile:** `uv.lock` is committed — always run `uv sync` after pulling, not `pip install`.

### First-time bootstrap
```bash
brew install uv          # if not already installed
uv sync --all-groups     # creates .venv and installs from uv.lock
```

### Common commands (see Makefile for full list)
```bash
make install      # uv sync --all-groups
make migrate      # apply schema migrations → data/gold/mlb.duckdb
make explore      # validate Pydantic models against live MLB API
make test         # full test suite (unit + integration)
make test-unit    # unit tests only (fast, offline)
make lint         # ruff check
make fmt          # ruff format + fix imports
make typecheck    # mypy
```

### Running scripts directly
```bash
uv run python scripts/explore_api.py
uv run python migrations/migrate.py
uv run pytest tests/unit/
```

### Why uv over pip + requirements.txt
`pyproject.toml` is the single source of truth for dependencies. `uv.lock` provides
cryptographically pinned, reproducible installs across all machines and CI. Never
hand-edit `requirements.txt` — if a deployment target needs one, generate it:
```bash
uv pip compile pyproject.toml -o requirements.txt
```

---

## Progress

### Milestone 1 — Infrastructure Setup
- [x] `pyproject.toml` — pinned deps (duckdb 1.2.1, httpx, pydantic v2, pyarrow, apscheduler, azure-storage-blob, tenacity, structlog)
- [x] `.gitignore` + `.env.example`
- [x] Directory structure scaffolded (`src/`, `sql/`, `data/`, `tests/`, `migrations/`)
- [x] DuckDB schema DDL — `sql/schema/001_initial_schema.sql` (bronze, silver, gold, meta)
- [x] Migration runner — `migrations/migrate.py` (idempotent, transactional, checksum-tracked)

### Milestone 2 — Extractor v1
- [x] MLB API async client (`src/extractor/client.py`) — httpx + token-bucket rate limiter + tenacity retry
- [x] Pydantic models — schedule, game feed, player, team (`src/extractor/models/`)
- [x] Bronze Parquet writer (`src/extractor/writer.py`) — explicit Arrow schemas, Snappy, partitioned layout
- [x] Extraction functions (`src/extractor/extract.py`) — schedule, game feeds, teams, players

### Milestone 3 — Historical Back-fill (2022–2025)
- [x] API exploration script (`scripts/explore_api.py`) — probes all 5 endpoint types, reports ignored keys and validation errors
- [x] pytest fixtures (`tests/conftest.py`) — in-memory + file-backed DuckDB with schema applied
- [x] Unit tests — models (`tests/unit/test_models.py`), Parquet writer (`tests/unit/test_writer.py`)
- [x] Integration tests — schema migrations (`tests/integration/test_schema.py`)
- [x] `RunTracker` (`src/run_tracker/tracker.py`) — DuckDB-backed run state + bulk idempotency checks
- [x] Back-fill job (`src/extractor/backfill.py`) — month-parallel (3 concurrent), season-sequential, idempotent
- [x] Integration tests — RunTracker + back-fill helpers (`tests/integration/test_run_tracker.py`)

### Milestone 4 — Silver Transformation
- [ ] SQL transform scripts (`sql/silver/`) — type casting, deduplication, null handling
- [ ] Transformer runner (`src/transformer/transform.py`)

### Milestone 5 — Gold Aggregation
- [ ] Gold view SQL (`sql/gold/`) — leaderboards, player_season_summary, head_to_head, standings
- [ ] `gold.league_averages` population script (needed for OPS+)

### Milestone 6 — 2026 Incremental + Scheduling
- [ ] Nightly incremental job
- [ ] Roster sync job
- [ ] Standings snapshot job
- [ ] APScheduler wiring (`src/scheduler/jobs.py`)

### Milestone 7 — Distribution
- [x] Azure Blob sync (`src/distribution/sync.py`) — `DistributionSync` with master upload + server-side per-club copy
- [x] Per-club SAS token provisioning — user-delegation SAS (no storage account key), configurable expiry
- [x] `run_distribution()` — RunTracker-integrated entry point
- [x] Distribution job wired into APScheduler at 04:30 ET daily (`src/scheduler/jobs.py`)
- [x] Integration tests — 22 tests covering upload, copy, SAS, env config, RunTracker, scheduler wiring

### Milestone 8 — QA & Performance
- [x] Data accuracy validation (`tests/integration/test_data_accuracy.py`) — 38 tests covering mathematical invariants (win_pct, games_back, run_diff, streak format), referential integrity, NULL checks, PK uniqueness across all gold views and silver tables
- [x] Query benchmark suite (`tests/benchmarks/`) — 28 SLA-gated tests over a 5-season dataset (12,150 games, 750 players, 30 teams); all gold queries complete within documented SLA thresholds
- [x] `make test-accuracy` and `make benchmark` targets added to Makefile

### Milestone 9 — Production Launch
- [x] SLA definitions (`src/monitoring/sla.py`) — 4 jobs with max_age_hours windows; data freshness and artifact size bounds
- [x] Health checker (`src/monitoring/health.py`) — `HealthChecker` with 5 check categories: SLA compliance, failed run detection, data freshness (off-season aware), artifact size, gold row-count sanity
- [x] `run_health_check()` — RunTracker-integrated entry point; records outcome in `meta.pipeline_runs`
- [x] Health check job wired into APScheduler at 07:00 ET daily (`src/scheduler/jobs.py`)
- [x] CLI: `uv run python -m src.monitoring.health [--json]`
- [x] Integration tests — 30 tests covering all check categories, SLA edge cases, scheduler wiring
- [ ] All 30 clubs onboarded (operational — requires Azure credentials; distribution code is ready in `src/distribution/sync.py`)

---

## Key Design Rules

- **No mocks in integration tests** — always use a real DuckDB instance
- Club teams query `gold` schema only; never expose `bronze` or `silver` directly
- All timestamps stored as UTC
- Identifiers are integer surrogate keys from the MLB Stats API unless noted
- Pipeline is fully idempotent — safe to re-run or replay any date range
- `INSERT OR REPLACE` with `correction_source` flag for MLB retroactive corrections
- Schema changes use migration scripts applied before distribution
