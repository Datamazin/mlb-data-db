-- =============================================================================
-- Gold 008 — standings_snap
-- Computes current-season standings from all Final Regular Season games in
-- silver.games. Inserts/replaces one row per team keyed on (snap_date, team_id).
-- Re-running on the same calendar date is idempotent.
-- Returns 0 rows when silver.games is empty.
-- =============================================================================

INSERT OR REPLACE INTO gold.standings_snap (
    snap_date, season_year, team_id, division_id,
    wins, losses, win_pct, games_back, streak,
    last_10_wins, last_10_losses,
    home_wins, home_losses, away_wins, away_losses,
    run_diff, loaded_at
)
WITH all_team_games AS (
    SELECT
        g.game_pk,
        g.season_year,
        g.game_date,
        g.home_team_id             AS team_id,
        TRUE                       AS is_home,
        (g.home_score > g.away_score) AS won,
        g.home_score - g.away_score   AS run_margin
    FROM silver.games g
    WHERE g.status = 'Final' AND g.game_type = 'R'

    UNION ALL

    SELECT
        g.game_pk,
        g.season_year,
        g.game_date,
        g.away_team_id             AS team_id,
        FALSE                      AS is_home,
        (g.away_score > g.home_score) AS won,
        g.away_score - g.home_score   AS run_margin
    FROM silver.games g
    WHERE g.status = 'Final' AND g.game_type = 'R'
),
ranked_games AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date DESC, game_pk DESC
        ) AS game_rank
    FROM all_team_games
),
season_records AS (
    SELECT
        team_id,
        season_year,
        SUM(CASE WHEN won           THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN NOT won       THEN 1 ELSE 0 END) AS losses,
        SUM(CASE WHEN is_home AND won            THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN is_home AND NOT won        THEN 1 ELSE 0 END) AS home_losses,
        SUM(CASE WHEN NOT is_home AND won        THEN 1 ELSE 0 END) AS away_wins,
        SUM(CASE WHEN NOT is_home AND NOT won    THEN 1 ELSE 0 END) AS away_losses,
        SUM(run_margin)                                AS run_diff
    FROM all_team_games
    GROUP BY team_id, season_year
),
last_10 AS (
    SELECT
        team_id,
        season_year,
        SUM(CASE WHEN won     THEN 1 ELSE 0 END) AS last_10_wins,
        SUM(CASE WHEN NOT won THEN 1 ELSE 0 END) AS last_10_losses
    FROM ranked_games
    WHERE game_rank <= 10
    GROUP BY team_id, season_year
),
-- Detect where the current streak breaks.
-- Step 1: label the most recent game result per team so we can compare.
streak_latest AS (
    SELECT team_id, season_year, won AS latest_result
    FROM ranked_games
    WHERE game_rank = 1
),
-- Step 2: mark each game as "still in streak" (0) or "streak broken" (1).
-- Cumulative sum lets us count only games before the first break.
streak_data AS (
    SELECT
        rg.team_id, rg.season_year, rg.won, rg.game_rank,
        SUM(
            CASE WHEN rg.won != sl.latest_result THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY rg.team_id, rg.season_year
            ORDER BY rg.game_rank
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS change_count
    FROM ranked_games rg
    JOIN streak_latest sl
      ON rg.team_id = sl.team_id AND rg.season_year = sl.season_year
),
current_streak AS (
    SELECT
        team_id,
        season_year,
        MAX(CASE WHEN game_rank = 1 THEN won::INTEGER END) AS current_result,
        COUNT(*) FILTER (WHERE change_count = 0)            AS streak_len
    FROM streak_data
    GROUP BY team_id, season_year
),
streak_label AS (
    SELECT
        team_id,
        season_year,
        CASE WHEN current_result = 1 THEN 'W' ELSE 'L' END
            || streak_len::VARCHAR AS streak
    FROM current_streak
),
-- Best record per division (leader for games-back calculation)
division_leaders AS (
    SELECT
        t.division_id,
        sr.season_year,
        sr.wins     AS leader_wins,
        sr.losses   AS leader_losses
    FROM season_records sr
    JOIN silver.teams t ON sr.team_id = t.team_id AND sr.season_year = t.season_year
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.division_id, sr.season_year
        ORDER BY sr.wins DESC, sr.losses ASC
    ) = 1
)
SELECT
    current_date                                               AS snap_date,
    sr.season_year,
    sr.team_id,
    t.division_id,
    sr.wins,
    sr.losses,
    ROUND(sr.wins::DECIMAL / NULLIF(sr.wins + sr.losses, 0), 3)                          AS win_pct,
    ROUND((dl.leader_wins - sr.wins + sr.losses - dl.leader_losses)::DECIMAL / 2.0, 1)  AS games_back,
    sl.streak,
    l10.last_10_wins,
    l10.last_10_losses,
    sr.home_wins,
    sr.home_losses,
    sr.away_wins,
    sr.away_losses,
    sr.run_diff,
    current_timestamp                                          AS loaded_at
FROM season_records sr
JOIN silver.teams t
    ON sr.team_id = t.team_id
    AND sr.season_year = t.season_year
JOIN division_leaders dl
    ON t.division_id = dl.division_id
    AND sr.season_year = dl.season_year
LEFT JOIN last_10 l10
    ON sr.team_id = l10.team_id
    AND sr.season_year = l10.season_year
LEFT JOIN streak_label sl
    ON sr.team_id = sl.team_id
    AND sr.season_year = sl.season_year;
