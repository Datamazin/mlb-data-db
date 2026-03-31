-- =============================================================================
-- Gold 004 — fact_game
-- Game-level fact view: enriches silver.games with home/away team names and
-- venue name so analysts can query without additional joins.
-- =============================================================================

CREATE OR REPLACE VIEW gold.fact_game AS
SELECT
    g.game_pk,
    g.season_year,
    g.game_date,
    g.game_datetime,
    g.game_type,
    g.status,
    g.home_team_id,
    ht.team_name        AS home_team_name,
    ht.team_abbrev      AS home_team_abbrev,
    g.away_team_id,
    at.team_name        AS away_team_name,
    at.team_abbrev      AS away_team_abbrev,
    g.home_score,
    g.away_score,
    g.innings,
    g.venue_id,
    v.venue_name,
    g.attendance,
    g.game_duration_min,
    g.double_header,
    g.series_description,
    g.series_game_num
FROM silver.games g
LEFT JOIN silver.teams ht ON g.home_team_id = ht.team_id AND g.season_year = ht.season_year
LEFT JOIN silver.teams at ON g.away_team_id = at.team_id AND g.season_year = at.season_year
LEFT JOIN silver.venues v  ON g.venue_id    = v.venue_id;
