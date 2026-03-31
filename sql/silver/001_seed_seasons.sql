-- =============================================================================
-- Silver 001 — Seed season reference data (2022–2026)
-- Static INSERT OR REPLACE; safe to re-run. Postseason dates for 2026 are
-- NULL until the postseason schedule is finalised.
-- =============================================================================

INSERT OR REPLACE INTO silver.seasons
    (season_year, sport_id, regular_season_start, regular_season_end,
     postseason_start, world_series_end, games_per_team, loaded_at)
VALUES
    (2022, 1, '2022-04-07', '2022-10-05', '2022-10-07', '2022-11-05', 162, current_timestamp),
    (2023, 1, '2023-03-30', '2023-10-01', '2023-10-03', '2023-11-04', 162, current_timestamp),
    (2024, 1, '2024-03-20', '2024-09-29', '2024-10-01', '2024-10-30', 162, current_timestamp),
    (2025, 1, '2025-03-27', '2025-09-28', '2025-10-01', '2025-10-29', 162, current_timestamp),
    (2026, 1, '2026-03-26', '2026-09-27',  NULL,          NULL,        162, current_timestamp);
