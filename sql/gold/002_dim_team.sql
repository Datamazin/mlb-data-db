-- =============================================================================
-- Gold 002 — dim_team
-- Enriches silver.teams with league, division, and venue names.
-- One row per (team_id, season_year) — teams may change divisions across years.
-- =============================================================================

CREATE OR REPLACE VIEW gold.dim_team AS
SELECT
    t.team_id,
    t.season_year,
    t.team_name,
    t.team_abbrev,
    t.city,
    l.league_id,
    l.league_name,
    l.abbreviation       AS league_abbrev,
    d.division_id,
    d.division_name,
    d.short_name         AS division_short_name,
    v.venue_id,
    v.venue_name,
    t.first_year,
    t.active
FROM silver.teams t
LEFT JOIN silver.leagues   l ON t.league_id   = l.league_id
LEFT JOIN silver.divisions d ON t.division_id  = d.division_id
LEFT JOIN silver.venues    v ON t.venue_id     = v.venue_id;
