-- =============================================================================
-- Gold 007 — player_season_summary
-- M6 stub: one row per player-season with batting + pitching lines.
-- Returns 0 rows until silver.fact_batting is populated (M6).
-- TODO(M6): add pitching, OPS+, FIP once league_averages and fact_pitching exist.
-- =============================================================================

CREATE OR REPLACE VIEW gold.player_season_summary AS
SELECT
    p.player_id,
    p.full_name,
    p.primary_position,
    fb.team_id,
    fb.season_year,
    fb.game_type,
    fb.games,
    fb.pa,
    fb.ab,
    fb.hits,
    fb.doubles,
    fb.triples,
    fb.home_runs,
    fb.rbi,
    fb.runs,
    fb.walks,
    fb.strikeouts,
    fb.stolen_bases,
    fb.caught_stealing,
    fb.avg,
    fb.obp,
    fb.slg,
    fb.ops,
    fb.babip
FROM silver.fact_batting fb
JOIN silver.players p ON fb.player_id = p.player_id;
