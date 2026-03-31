-- =============================================================================
-- Gold 006 — leaderboards
-- M6 stub: silver.fact_batting is empty until /v1/people/{id}/stats bronze
-- extraction is implemented. This view will return 0 rows until then.
-- TODO(M6): replace stub with real leaderboard rankings once fact_batting is populated.
-- =============================================================================

CREATE OR REPLACE VIEW gold.leaderboards AS
SELECT
    fb.player_id,
    p.full_name,
    fb.team_id,
    fb.season_year,
    fb.game_type,
    fb.games,
    fb.pa,
    fb.ab,
    fb.hits,
    fb.home_runs,
    fb.rbi,
    fb.runs,
    fb.walks,
    fb.strikeouts,
    fb.stolen_bases,
    fb.avg,
    fb.obp,
    fb.slg,
    fb.ops,
    fb.babip
FROM silver.fact_batting fb
LEFT JOIN silver.players p ON fb.player_id = p.player_id
ORDER BY fb.home_runs DESC NULLS LAST;
