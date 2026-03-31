-- =============================================================================
-- Silver 008 — Game linescore
-- Unnests the liveData.linescore.innings JSON array from bronze/games raw_json.
-- One row per (game_pk, inning). COALESCE null runs/hits/errors to 0 to match
-- the NOT NULL DEFAULT 0 DDL constraints.
-- =============================================================================

INSERT OR REPLACE INTO silver.game_linescore
    (game_pk, inning, home_runs, home_hits, home_errors,
     away_runs, away_hits, away_errors, loaded_at)
WITH latest_feeds AS (
    -- Deduplicate to the most recent feed per game before unnesting
    SELECT
        game_pk,
        CAST(json_extract(raw_json, '$.liveData.linescore.innings') AS JSON) AS innings_json
    FROM read_parquet('{bronze_path}/games/year=*/month=*/*.parquet')
    WHERE json_extract(raw_json, '$.liveData.linescore.innings') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY extracted_at DESC) = 1
),
exploded AS (
    SELECT
        game_pk,
        CAST(json_extract_string(inning_elem, '$.num')          AS INTEGER)     AS inning,
        COALESCE(CAST(json_extract_string(inning_elem, '$.home.runs')   AS INTEGER), 0) AS home_runs,
        COALESCE(CAST(json_extract_string(inning_elem, '$.home.hits')   AS INTEGER), 0) AS home_hits,
        COALESCE(CAST(json_extract_string(inning_elem, '$.home.errors') AS INTEGER), 0) AS home_errors,
        COALESCE(CAST(json_extract_string(inning_elem, '$.away.runs')   AS INTEGER), 0) AS away_runs,
        COALESCE(CAST(json_extract_string(inning_elem, '$.away.hits')   AS INTEGER), 0) AS away_hits,
        COALESCE(CAST(json_extract_string(inning_elem, '$.away.errors') AS INTEGER), 0) AS away_errors
    FROM latest_feeds,
         UNNEST(json_extract(innings_json, '$[*]')) AS t(inning_elem)
)
SELECT
    game_pk, inning,
    home_runs, home_hits, home_errors,
    away_runs, away_hits, away_errors,
    current_timestamp AS loaded_at
FROM exploded
WHERE inning IS NOT NULL;
