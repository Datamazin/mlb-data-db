-- =============================================================================
-- Silver 003 — Divisions
-- Extracts distinct divisions from bronze/teams raw_json.
-- Includes league_id for FK documentation order (run after 002_leagues.sql).
-- =============================================================================

INSERT OR REPLACE INTO silver.divisions (division_id, division_name, short_name, league_id, loaded_at)
SELECT
    CAST(json_extract_string(raw_json, '$.division.id')         AS INTEGER) AS division_id,
    json_extract_string(raw_json, '$.division.name')                        AS division_name,
    json_extract_string(raw_json, '$.division.nameShort')                   AS short_name,
    CAST(json_extract_string(raw_json, '$.league.id')           AS INTEGER) AS league_id,
    current_timestamp                                                        AS loaded_at
FROM read_parquet('{bronze_path}/teams/season=*/*.parquet')
WHERE json_extract_string(raw_json, '$.division.id') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(json_extract_string(raw_json, '$.division.id') AS INTEGER)
    ORDER BY extracted_at DESC
) = 1;
