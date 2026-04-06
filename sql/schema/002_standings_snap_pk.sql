-- =============================================================================
-- Migration 002 — Fix gold.standings_snap primary key
-- Adds season_year to the PK so standings for different seasons stored on the
-- same snap_date do not overwrite each other.
-- =============================================================================

-- DuckDB does not support ALTER TABLE ... DROP/ADD CONSTRAINT, so we
-- recreate the table with the corrected PK and reload from silver.
CREATE TABLE gold.standings_snap_new (
    snap_date       DATE        NOT NULL,
    season_year     INTEGER     NOT NULL,
    team_id         INTEGER     NOT NULL,
    division_id     INTEGER     NOT NULL,
    wins            INTEGER     NOT NULL DEFAULT 0,
    losses          INTEGER     NOT NULL DEFAULT 0,
    win_pct         DECIMAL(5,3),
    games_back      DECIMAL(5,1),
    streak          VARCHAR,
    last_10_wins    INTEGER,
    last_10_losses  INTEGER,
    home_wins       INTEGER,
    home_losses     INTEGER,
    away_wins       INTEGER,
    away_losses     INTEGER,
    run_diff        INTEGER,
    loaded_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snap_date, season_year, team_id)
);

INSERT INTO gold.standings_snap_new SELECT * FROM gold.standings_snap;

DROP TABLE gold.standings_snap;

ALTER TABLE gold.standings_snap_new RENAME TO standings_snap;
