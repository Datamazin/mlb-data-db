-- =============================================================================
-- Silver 006 — Players
-- SCD Type 1: latest row per player_id wins across all seasons.
-- TRY_CAST for dates — malformed values become NULL rather than erroring.
-- LEFT(bats/throws, 1) guards against API returning full words ("Left", "Right").
-- =============================================================================

INSERT OR REPLACE INTO silver.players
    (player_id, full_name, first_name, last_name, birth_date,
     birth_city, birth_country, height, weight,
     bats, throws, primary_position, mlb_debut_date, active, loaded_at)
SELECT
    player_id,
    full_name,
    first_name,
    last_name,
    TRY_CAST(birth_date      AS DATE)  AS birth_date,
    birth_city,
    birth_country,
    height,
    weight,
    LEFT(bats,   1)                    AS bats,
    LEFT(throws, 1)                    AS throws,
    primary_position,
    TRY_CAST(mlb_debut_date  AS DATE)  AS mlb_debut_date,
    active,
    current_timestamp                  AS loaded_at
FROM read_parquet('{bronze_path}/players/season=*/*.parquet')
WHERE full_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY player_id
    ORDER BY extracted_at DESC
) = 1;
