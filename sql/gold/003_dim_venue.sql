-- =============================================================================
-- Gold 003 — dim_venue
-- Thin view over silver.venues.
-- city / state / capacity etc. are NULL until the /v1/venues extractor is added.
-- =============================================================================

CREATE OR REPLACE VIEW gold.dim_venue AS
SELECT
    venue_id,
    venue_name,
    city,
    state,
    country,
    surface,
    capacity,
    roof_type
FROM silver.venues;
