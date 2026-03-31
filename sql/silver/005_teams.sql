-- =============================================================================
-- Silver 005 — Teams
-- Loads from bronze/teams flat columns. PK is (team_id, season_year) so each
-- team appears once per season. Excludes seasons not present in silver.seasons.
-- =============================================================================

INSERT OR REPLACE INTO silver.teams
    (team_id, season_year, team_name, team_abbrev, team_code,
     league_id, division_id, venue_id, city, first_year, active, loaded_at)
SELECT
    team_id,
    season_year,
    team_name,
    team_abbrev,
    team_code,
    league_id,
    division_id,
    venue_id,
    city,
    first_year,
    active,
    current_timestamp AS loaded_at
FROM read_parquet('{bronze_path}/teams/season=*/*.parquet')
WHERE season_year IN (SELECT season_year FROM silver.seasons)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY team_id, season_year
    ORDER BY extracted_at DESC
) = 1;
