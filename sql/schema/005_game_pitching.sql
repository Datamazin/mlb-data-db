-- =============================================================================
-- Migration 005 — Per-game, per-pitcher stats (silver layer)
-- Populated by src/transformer/game_pitching.py from bronze raw_json.
--
-- outs stores total batters retired (IP = outs / 3).  Using integer outs
-- avoids the MLB "5.2 = 5⅔" decimal notation for arithmetic (ERA, WHIP).
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.game_pitching (
    game_pk           BIGINT      NOT NULL,
    player_id         INTEGER     NOT NULL,
    team_id           INTEGER     NOT NULL,
    is_home           BOOLEAN     NOT NULL,
    wins              INTEGER     NOT NULL DEFAULT 0,
    losses            INTEGER     NOT NULL DEFAULT 0,
    saves             INTEGER     NOT NULL DEFAULT 0,
    holds             INTEGER     NOT NULL DEFAULT 0,
    blown_saves       INTEGER     NOT NULL DEFAULT 0,
    games_started     INTEGER     NOT NULL DEFAULT 0,
    games_finished    INTEGER     NOT NULL DEFAULT 0,
    complete_games    INTEGER     NOT NULL DEFAULT 0,
    shutouts          INTEGER     NOT NULL DEFAULT 0,
    outs              INTEGER     NOT NULL DEFAULT 0,
    hits_allowed      INTEGER     NOT NULL DEFAULT 0,
    runs_allowed      INTEGER     NOT NULL DEFAULT 0,
    earned_runs       INTEGER     NOT NULL DEFAULT 0,
    home_runs_allowed INTEGER     NOT NULL DEFAULT 0,
    walks             INTEGER     NOT NULL DEFAULT 0,
    strikeouts        INTEGER     NOT NULL DEFAULT 0,
    hit_by_pitch      INTEGER     NOT NULL DEFAULT 0,
    pitches_thrown    INTEGER     NOT NULL DEFAULT 0,
    strikes           INTEGER     NOT NULL DEFAULT 0,
    loaded_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (game_pk, player_id)
);
