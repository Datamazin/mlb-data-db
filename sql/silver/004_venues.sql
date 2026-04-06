-- =============================================================================
-- Silver 004 — Venues
-- Two-pass approach so each source is independent: a missing bronze partition
-- for one source does not prevent the other from running.
--
-- Pass 1: game venues (fallback — written first, lower priority)
-- Pass 2: team venues (authoritative — written second, overwrites pass 1
--          for the same venue_id via INSERT OR REPLACE)
--
-- City, state, capacity, surface, roof_type are NULL until a dedicated
-- /v1/venues/{venueId} extractor is added (future milestone).
-- =============================================================================

-- Pass 1: game venues (lower priority — may be overwritten by pass 2)
INSERT OR REPLACE INTO silver.venues
    (venue_id, venue_name, city, state, country, surface, capacity, roof_type, loaded_at)
SELECT
    CAST(json_extract_string(raw_json, '$.gameData.venue.id')   AS INTEGER) AS venue_id,
    json_extract_string(raw_json, '$.gameData.venue.name')                  AS venue_name,
    NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR,
    NULL::VARCHAR, NULL::INTEGER, NULL::VARCHAR,
    current_timestamp AS loaded_at
FROM read_parquet('{bronze_path}/games/year={year_glob}/month={month_glob}/*.parquet')
WHERE json_extract_string(raw_json, '$.gameData.venue.id')   IS NOT NULL
  AND json_extract_string(raw_json, '$.gameData.venue.name') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(json_extract_string(raw_json, '$.gameData.venue.id') AS INTEGER)
    ORDER BY extracted_at DESC
) = 1;

-- Pass 2: team venues (authoritative — overwrites game venue entries)
INSERT OR REPLACE INTO silver.venues
    (venue_id, venue_name, city, state, country, surface, capacity, roof_type, loaded_at)
SELECT
    venue_id,
    json_extract_string(raw_json, '$.venue.name') AS venue_name,
    NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR,
    NULL::VARCHAR, NULL::INTEGER, NULL::VARCHAR,
    current_timestamp AS loaded_at
FROM read_parquet('{bronze_path}/teams/season=*/*.parquet')
WHERE venue_id IS NOT NULL
  AND json_extract_string(raw_json, '$.venue.name') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY venue_id ORDER BY extracted_at DESC) = 1;
