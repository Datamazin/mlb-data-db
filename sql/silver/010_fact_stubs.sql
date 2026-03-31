-- =============================================================================
-- Silver 010 — Fact table stubs (no-op)
-- fact_batting and fact_pitching require /v1/people/{id}/stats API data which
-- is not yet extracted into bronze. These tables will be populated in a later
-- milestone once bronze/stats/ Parquet files exist.
-- TODO(M6): replace with real transforms reading bronze/stats/season=*/*.parquet
-- =============================================================================

SELECT 1 WHERE 1 = 0;
