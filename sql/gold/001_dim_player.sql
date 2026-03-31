-- =============================================================================
-- Gold 001 — dim_player
-- Thin view over silver.players; exposes the player dimension to club analysts.
-- =============================================================================

CREATE OR REPLACE VIEW gold.dim_player AS
SELECT
    player_id,
    full_name,
    first_name,
    last_name,
    birth_date,
    birth_city,
    birth_country,
    height,
    weight,
    bats,
    throws,
    primary_position,
    mlb_debut_date,
    active
FROM silver.players;
