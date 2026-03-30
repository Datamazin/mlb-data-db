-- =============================================================================
-- Migration 001 — Initial Schema
-- Creates all four schemas and their tables.
-- Run via: python migrations/migrate.py
-- =============================================================================

-- ── Schemas ──────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;


-- =============================================================================
-- META — Pipeline run tracking & data lineage
-- (Created first; referenced during every subsequent stage)
-- =============================================================================

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id          VARCHAR      PRIMARY KEY,   -- UUID generated at job start
    job_name        VARCHAR      NOT NULL,       -- e.g. 'nightly_incremental', 'backfill'
    status          VARCHAR      NOT NULL,       -- running | success | failed
    started_at      TIMESTAMPTZ  NOT NULL,
    completed_at    TIMESTAMPTZ,
    season_year     INTEGER,
    target_date     DATE,                        -- NULL for multi-day jobs
    records_extracted INTEGER    DEFAULT 0,
    records_loaded    INTEGER    DEFAULT 0,
    error_message   VARCHAR,
    pipeline_version VARCHAR     NOT NULL        -- semver of the pipeline codebase
);

CREATE TABLE IF NOT EXISTS meta.entity_checksums (
    entity_type     VARCHAR      NOT NULL,       -- e.g. 'game_feed', 'player', 'team'
    entity_key      VARCHAR      NOT NULL,       -- e.g. gamePk or personId as string
    response_hash   VARCHAR      NOT NULL,       -- SHA-256 of raw JSON response
    source_url      VARCHAR      NOT NULL,
    extracted_at    TIMESTAMPTZ  NOT NULL,
    transform_version VARCHAR    NOT NULL,       -- version of transform SQL applied
    correction_source VARCHAR,                   -- set when MLB publishes a retroactive fix
    PRIMARY KEY (entity_type, entity_key)
);


-- =============================================================================
-- BRONZE — Raw API responses stored as typed columns
-- One catch-all table; structured tables live in silver.
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_api_responses (
    id              BIGINT       PRIMARY KEY,    -- auto-increment surrogate
    entity_type     VARCHAR      NOT NULL,
    entity_key      VARCHAR      NOT NULL,
    source_url      VARCHAR      NOT NULL,
    response_json   JSON         NOT NULL,
    http_status     SMALLINT     NOT NULL,
    extracted_at    TIMESTAMPTZ  NOT NULL,
    file_path       VARCHAR                      -- path to the Parquet file on disk
);


-- =============================================================================
-- SILVER — Cleaned, typed, deduplicated entities
-- =============================================================================

-- ── Reference / dimension tables ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.leagues (
    league_id       INTEGER      PRIMARY KEY,
    league_name     VARCHAR      NOT NULL,
    short_name      VARCHAR,
    abbreviation    VARCHAR,
    loaded_at       TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.divisions (
    division_id     INTEGER      PRIMARY KEY,
    division_name   VARCHAR      NOT NULL,
    short_name      VARCHAR,
    league_id       INTEGER      NOT NULL REFERENCES silver.leagues(league_id),
    loaded_at       TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.venues (
    venue_id        INTEGER      PRIMARY KEY,
    venue_name      VARCHAR      NOT NULL,
    city            VARCHAR,
    state           VARCHAR,
    country         VARCHAR,
    surface         VARCHAR,     -- Grass | Artificial Turf
    capacity        INTEGER,
    roof_type       VARCHAR,     -- Open | Retractable | Fixed
    loaded_at       TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.seasons (
    season_year           INTEGER  PRIMARY KEY,
    sport_id              INTEGER  NOT NULL,     -- 1 = MLB
    regular_season_start  DATE     NOT NULL,
    regular_season_end    DATE     NOT NULL,
    postseason_start      DATE,
    world_series_end      DATE,
    games_per_team        INTEGER,
    loaded_at             TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.teams (
    team_id         INTEGER      NOT NULL,
    season_year     INTEGER      NOT NULL REFERENCES silver.seasons(season_year),
    team_name       VARCHAR      NOT NULL,
    team_abbrev     VARCHAR(3)   NOT NULL,
    team_code       VARCHAR,
    league_id       INTEGER      REFERENCES silver.leagues(league_id),
    division_id     INTEGER      REFERENCES silver.divisions(division_id),
    venue_id        INTEGER      REFERENCES silver.venues(venue_id),
    city            VARCHAR,
    first_year      INTEGER,
    active          BOOLEAN      NOT NULL DEFAULT TRUE,
    loaded_at       TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (team_id, season_year)
);

CREATE TABLE IF NOT EXISTS silver.players (
    player_id           INTEGER      PRIMARY KEY,
    full_name           VARCHAR      NOT NULL,
    first_name          VARCHAR,
    last_name           VARCHAR,
    birth_date          DATE,
    birth_city          VARCHAR,
    birth_country       VARCHAR,
    height              VARCHAR,     -- stored as feet-inches string from API e.g. "6' 2\""
    weight              INTEGER,     -- pounds
    bats                VARCHAR(1),  -- L | R | S
    throws              VARCHAR(1),  -- L | R
    primary_position    VARCHAR(2),  -- SP | RP | C | SS | OF | etc.
    mlb_debut_date      DATE,
    active              BOOLEAN      NOT NULL DEFAULT TRUE,
    loaded_at           TIMESTAMPTZ  NOT NULL
);

-- ── Game tables ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.games (
    game_pk             BIGINT       PRIMARY KEY,
    season_year         INTEGER      NOT NULL REFERENCES silver.seasons(season_year),
    game_date           DATE         NOT NULL,
    game_datetime       TIMESTAMPTZ,             -- first pitch UTC; NULL if not yet played
    game_type           VARCHAR(2)   NOT NULL,   -- R | S | F | D | L | W
    status              VARCHAR      NOT NULL,   -- Final | Postponed | Suspended | In Progress
    home_team_id        INTEGER      NOT NULL,
    away_team_id        INTEGER      NOT NULL,
    home_score          INTEGER,
    away_score          INTEGER,
    innings             INTEGER,
    venue_id            INTEGER      REFERENCES silver.venues(venue_id),
    attendance          INTEGER,
    game_duration_min   INTEGER,
    double_header       VARCHAR(1),  -- N | Y | S
    series_description  VARCHAR,     -- e.g. 'World Series', 'ALCS'
    series_game_num     INTEGER,
    loaded_at           TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.game_linescore (
    game_pk     BIGINT   NOT NULL,  -- references silver.games
    inning      INTEGER  NOT NULL,
    home_runs   INTEGER  NOT NULL DEFAULT 0,
    home_hits   INTEGER  NOT NULL DEFAULT 0,
    home_errors INTEGER  NOT NULL DEFAULT 0,
    away_runs   INTEGER  NOT NULL DEFAULT 0,
    away_hits   INTEGER  NOT NULL DEFAULT 0,
    away_errors INTEGER  NOT NULL DEFAULT 0,
    loaded_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (game_pk, inning)
);

CREATE TABLE IF NOT EXISTS silver.game_boxscore (
    game_pk             BIGINT      NOT NULL,
    team_id             INTEGER     NOT NULL,
    is_home             BOOLEAN     NOT NULL,
    runs                INTEGER,
    hits                INTEGER,
    errors              INTEGER,
    left_on_base        INTEGER,
    batting_order       JSON,       -- ordered array of player_ids
    pitching_order      JSON,       -- array of player_ids in order used
    loaded_at           TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (game_pk, team_id)
);

-- ── Fact tables (season-aggregated stats) ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.fact_batting (
    player_id       INTEGER     NOT NULL,
    team_id         INTEGER     NOT NULL,
    season_year     INTEGER     NOT NULL,
    game_type       VARCHAR(2)  NOT NULL,   -- R | P
    games           INTEGER,
    pa              INTEGER,                -- plate appearances
    ab              INTEGER,                -- at-bats
    hits            INTEGER,
    doubles         INTEGER,
    triples         INTEGER,
    home_runs       INTEGER,
    rbi             INTEGER,
    runs            INTEGER,
    walks           INTEGER,
    ibb             INTEGER,               -- intentional walks
    strikeouts      INTEGER,
    stolen_bases    INTEGER,
    caught_stealing INTEGER,
    avg             DECIMAL(5,3),
    obp             DECIMAL(5,3),
    slg             DECIMAL(5,3),
    ops             DECIMAL(5,3),
    babip           DECIMAL(5,3),
    loaded_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (player_id, team_id, season_year, game_type)
);

CREATE TABLE IF NOT EXISTS silver.fact_pitching (
    player_id           INTEGER     NOT NULL,
    team_id             INTEGER     NOT NULL,
    season_year         INTEGER     NOT NULL,
    game_type           VARCHAR(2)  NOT NULL,
    games               INTEGER,
    games_started       INTEGER,
    wins                INTEGER,
    losses              INTEGER,
    saves               INTEGER,
    holds               INTEGER,
    ip                  DECIMAL(6,1),           -- innings pitched (0.1, 0.2 = partial)
    hits_allowed        INTEGER,
    runs_allowed        INTEGER,
    earned_runs         INTEGER,
    home_runs_allowed   INTEGER,
    walks               INTEGER,
    strikeouts          INTEGER,
    era                 DECIMAL(5,2),
    whip                DECIMAL(5,3),
    k9                  DECIMAL(5,2),           -- K/9 innings
    bb9                 DECIMAL(5,2),           -- BB/9 innings
    hr9                 DECIMAL(5,2),           -- HR/9 innings
    k_bb_ratio          DECIMAL(5,2),
    fip                 DECIMAL(5,2),           -- pipeline-computed: (13*HR + 3*BB - 2*K) / IP + FIP_constant
    loaded_at           TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (player_id, team_id, season_year, game_type)
);


-- =============================================================================
-- GOLD — Aggregated & fan-ready (primary consumer layer)
-- Views and materialized tables are defined in sql/gold/*.sql.
-- Only structural scaffolding lives here.
-- =============================================================================

-- Daily standings snapshot — materialized nightly (not a view, needs history)
CREATE TABLE IF NOT EXISTS gold.standings_snap (
    snap_date       DATE        NOT NULL,
    season_year     INTEGER     NOT NULL,
    team_id         INTEGER     NOT NULL,
    division_id     INTEGER     NOT NULL,
    wins            INTEGER     NOT NULL DEFAULT 0,
    losses          INTEGER     NOT NULL DEFAULT 0,
    win_pct         DECIMAL(5,3),
    games_back      DECIMAL(5,1),
    streak          VARCHAR,    -- e.g. 'W3', 'L1'
    last_10_wins    INTEGER,
    last_10_losses  INTEGER,
    home_wins       INTEGER,
    home_losses     INTEGER,
    away_wins       INTEGER,
    away_losses     INTEGER,
    run_diff        INTEGER,
    loaded_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snap_date, team_id)
);

-- League averages — needed for OPS+ computation in player_season_summary view
CREATE TABLE IF NOT EXISTS gold.league_averages (
    season_year     INTEGER     NOT NULL,
    league_id       INTEGER     NOT NULL,
    game_type       VARCHAR(2)  NOT NULL,
    league_avg      DECIMAL(5,3),
    league_obp      DECIMAL(5,3),
    league_slg      DECIMAL(5,3),
    league_ops      DECIMAL(5,3),
    league_era      DECIMAL(5,2),
    loaded_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (season_year, league_id, game_type)
);
