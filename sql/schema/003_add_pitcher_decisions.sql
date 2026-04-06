-- =============================================================================
-- Migration 003 — Add winning/losing/save pitcher IDs to silver.games
-- =============================================================================

ALTER TABLE silver.games ADD COLUMN IF NOT EXISTS wp_id INTEGER;  -- winning pitcher
ALTER TABLE silver.games ADD COLUMN IF NOT EXISTS lp_id INTEGER;  -- losing pitcher
ALTER TABLE silver.games ADD COLUMN IF NOT EXISTS sv_id INTEGER;  -- save pitcher (nullable)
