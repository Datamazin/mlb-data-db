-- =============================================================================
-- Silver 002 — Leagues
-- Extracts distinct leagues from bronze/teams raw_json.
-- Deduplicates by league_id; latest extracted_at wins.
-- =============================================================================

INSERT OR REPLACE INTO silver.leagues (league_id, league_name, short_name, abbreviation, loaded_at)
SELECT
    CAST(json_extract_string(raw_json, '$.league.id')           AS INTEGER) AS league_id,
    json_extract_string(raw_json, '$.league.name')                          AS league_name,
    json_extract_string(raw_json, '$.league.nameShort')                     AS short_name,
    json_extract_string(raw_json, '$.league.abbreviation')                  AS abbreviation,
    current_timestamp                                                        AS loaded_at
FROM read_parquet('{bronze_path}/teams/season=*/*.parquet')
WHERE json_extract_string(raw_json, '$.league.id') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(json_extract_string(raw_json, '$.league.id') AS INTEGER)
    ORDER BY extracted_at DESC
) = 1;
