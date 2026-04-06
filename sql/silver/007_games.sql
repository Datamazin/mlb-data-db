-- =============================================================================
-- Silver 007 — Games
-- Loads from bronze/games flat columns. Casts string dates to DATE/TIMESTAMPTZ.
-- Excludes games whose season_year is not in silver.seasons (FK guard).
-- Spring Training (game_type='S') is included; gold layer filters it out.
-- =============================================================================

INSERT OR REPLACE INTO silver.games
    (game_pk, season_year, game_date, game_datetime, game_type,
     status, home_team_id, away_team_id, home_score, away_score,
     innings, venue_id, attendance, game_duration_min,
     double_header, series_description, series_game_num,
     wp_id, lp_id, sv_id, loaded_at)
SELECT
    game_pk,
    season_year,
    TRY_CAST(game_date     AS DATE)        AS game_date,
    TRY_CAST(game_datetime AS TIMESTAMPTZ) AS game_datetime,
    game_type,
    status_detailed_state                  AS status,
    home_team_id,
    away_team_id,
    home_score,
    away_score,
    innings,
    venue_id,
    attendance,
    game_duration_min,
    LEFT(double_header, 1)                 AS double_header,
    series_description,
    series_game_num,
    -- Extract pitcher decisions from raw JSON; falls back to NULL if not present.
    -- This works for both old bronze files (no structured column) and new ones.
    TRY_CAST(json_extract_string(raw_json, '$.liveData.decisions.winner.id') AS INTEGER) AS wp_id,
    TRY_CAST(json_extract_string(raw_json, '$.liveData.decisions.loser.id')  AS INTEGER) AS lp_id,
    TRY_CAST(json_extract_string(raw_json, '$.liveData.decisions.save.id')   AS INTEGER) AS sv_id,
    current_timestamp                      AS loaded_at
FROM read_parquet('{bronze_path}/games/year={year_glob}/month={month_glob}/*.parquet')
WHERE game_date IS NOT NULL
  AND season_year IN (SELECT season_year FROM silver.seasons)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_pk
    ORDER BY extracted_at DESC
) = 1;
