-- =============================================================================
-- Gold 005 — head_to_head
-- Season win/loss record for every team pair in Regular Season games.
-- Returns empty when silver.games has no Final records.
-- =============================================================================

CREATE OR REPLACE VIEW gold.head_to_head AS
WITH game_results AS (
    SELECT
        season_year,
        home_team_id                         AS team_id,
        away_team_id                         AS opponent_id,
        (home_score > away_score)            AS won
    FROM silver.games
    WHERE status = 'Final' AND game_type = 'R'

    UNION ALL

    SELECT
        season_year,
        away_team_id                         AS team_id,
        home_team_id                         AS opponent_id,
        (away_score > home_score)            AS won
    FROM silver.games
    WHERE status = 'Final' AND game_type = 'R'
)
SELECT
    team_id,
    opponent_id,
    season_year,
    SUM(CASE WHEN won     THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN NOT won THEN 1 ELSE 0 END) AS losses,
    COUNT(*)                                  AS games_played
FROM game_results
GROUP BY team_id, opponent_id, season_year;
