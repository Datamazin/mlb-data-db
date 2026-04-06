-- =============================================================================
-- Silver 009 — Game boxscore
-- Extracts home and away team stats from liveData.boxscore.teams in raw_json.
-- Two rows per game_pk (is_home=TRUE and is_home=FALSE).
-- batting_order and pitching_order stored as JSON arrays.
-- =============================================================================

INSERT OR REPLACE INTO silver.game_boxscore
    (game_pk, team_id, is_home, runs, hits, errors,
     left_on_base, batting_order, pitching_order, loaded_at)
WITH latest_feeds AS (
    SELECT
        game_pk,
        CAST(json_extract(raw_json, '$.liveData.boxscore.teams') AS JSON) AS teams_json
    FROM read_parquet('{bronze_path}/games/year={year_glob}/month={month_glob}/*.parquet')
    WHERE json_extract(raw_json, '$.liveData.boxscore.teams') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY game_pk ORDER BY extracted_at DESC) = 1
),
home_side AS (
    SELECT
        game_pk,
        CAST(json_extract_string(teams_json, '$.home.team.id')                      AS INTEGER) AS team_id,
        TRUE                                                                                      AS is_home,
        CAST(json_extract_string(teams_json, '$.home.teamStats.batting.runs')        AS INTEGER) AS runs,
        CAST(json_extract_string(teams_json, '$.home.teamStats.batting.hits')        AS INTEGER) AS hits,
        CAST(json_extract_string(teams_json, '$.home.teamStats.fielding.errors')     AS INTEGER) AS errors,
        CAST(json_extract_string(teams_json, '$.home.teamStats.batting.leftOnBase')  AS INTEGER) AS left_on_base,
        json_extract(teams_json, '$.home.battingOrder')                                          AS batting_order,
        json_extract(teams_json, '$.home.pitchers')                                              AS pitching_order
    FROM latest_feeds
),
away_side AS (
    SELECT
        game_pk,
        CAST(json_extract_string(teams_json, '$.away.team.id')                      AS INTEGER) AS team_id,
        FALSE                                                                                     AS is_home,
        CAST(json_extract_string(teams_json, '$.away.teamStats.batting.runs')        AS INTEGER) AS runs,
        CAST(json_extract_string(teams_json, '$.away.teamStats.batting.hits')        AS INTEGER) AS hits,
        CAST(json_extract_string(teams_json, '$.away.teamStats.fielding.errors')     AS INTEGER) AS errors,
        CAST(json_extract_string(teams_json, '$.away.teamStats.batting.leftOnBase')  AS INTEGER) AS left_on_base,
        json_extract(teams_json, '$.away.battingOrder')                                          AS batting_order,
        json_extract(teams_json, '$.away.pitchers')                                              AS pitching_order
    FROM latest_feeds
)
SELECT *, current_timestamp AS loaded_at FROM home_side WHERE team_id IS NOT NULL
UNION ALL
SELECT *, current_timestamp AS loaded_at FROM away_side WHERE team_id IS NOT NULL;
