-- =============================================================================
-- Migration 004 — Per-game, per-player batting stats (silver layer)
-- Populated by scripts/populate_game_batting.py from bronze raw_json.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.game_batting (
    game_pk         BIGINT      NOT NULL,
    player_id       INTEGER     NOT NULL,
    team_id         INTEGER     NOT NULL,
    is_home         BOOLEAN     NOT NULL,
    batting_order   INTEGER,            -- 100=leadoff … 900=9th; +N for subs
    position_abbrev VARCHAR(4),
    at_bats         INTEGER     NOT NULL DEFAULT 0,
    runs            INTEGER     NOT NULL DEFAULT 0,
    hits            INTEGER     NOT NULL DEFAULT 0,
    doubles         INTEGER     NOT NULL DEFAULT 0,
    triples         INTEGER     NOT NULL DEFAULT 0,
    home_runs       INTEGER     NOT NULL DEFAULT 0,
    rbi             INTEGER     NOT NULL DEFAULT 0,
    walks           INTEGER     NOT NULL DEFAULT 0,
    strikeouts      INTEGER     NOT NULL DEFAULT 0,
    left_on_base    INTEGER     NOT NULL DEFAULT 0,
    loaded_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (game_pk, player_id)
);
