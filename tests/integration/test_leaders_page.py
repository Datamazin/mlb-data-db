"""
Integration test for the Streamlit Leaders page.

Uses a real file-backed DuckDB with the repository migrations applied and verifies
that the page renders batting and pitching data without query errors.
"""

from __future__ import annotations

from streamlit.testing.v1 import AppTest

import app
from aggregator.aggregate import Aggregator


def _ins_season(db, season_year=2026):
    db.execute(
        """INSERT OR REPLACE INTO silver.seasons
               (season_year, sport_id, regular_season_start, regular_season_end,
                games_per_team, loaded_at)
           VALUES (?, 1, ?, ?, 162, current_timestamp)""",
        [season_year, f"{season_year}-03-26", f"{season_year}-09-27"],
    )


def _ins_league(db, league_id=104, name="National League", abbrev="NL"):
    db.execute(
        """INSERT OR REPLACE INTO silver.leagues
               (league_id, league_name, short_name, abbreviation, loaded_at)
           VALUES (?, ?, ?, ?, current_timestamp)""",
        [league_id, name, name, abbrev],
    )


def _ins_division(db, division_id=203, name="NL West", league_id=104):
    db.execute(
        """INSERT OR REPLACE INTO silver.divisions
               (division_id, division_name, short_name, league_id, loaded_at)
           VALUES (?, ?, ?, ?, current_timestamp)""",
        [division_id, name, name, league_id],
    )


def _ins_venue(db, venue_id=22, name="Dodger Stadium"):
    db.execute(
        """INSERT OR REPLACE INTO silver.venues
               (venue_id, venue_name, loaded_at)
           VALUES (?, ?, current_timestamp)""",
        [venue_id, name],
    )


def _ins_team(
    db,
    team_id=119,
    season_year=2026,
    name="Los Angeles Dodgers",
    abbrev="LAD",
    league_id=104,
    division_id=203,
    venue_id=22,
):
    db.execute(
        """INSERT OR REPLACE INTO silver.teams
               (team_id, season_year, team_name, team_abbrev, team_code,
                league_id, division_id, venue_id, city, first_year, active, loaded_at)
           VALUES (?, ?, ?, ?, 'lan', ?, ?, ?, 'Los Angeles', 1884, TRUE, current_timestamp)""",
        [team_id, season_year, name, abbrev, league_id, division_id, venue_id],
    )


def _ins_player(
    db,
    player_id=660271,
    full_name="Shohei Ohtani",
    primary_position="DH",
):
    db.execute(
        """INSERT OR REPLACE INTO silver.players
               (player_id, full_name, first_name, last_name, bats, throws,
                primary_position, active, loaded_at)
           VALUES (?, ?, 'Shohei', 'Ohtani', 'L', 'R', ?, TRUE, current_timestamp)""",
        [player_id, full_name, primary_position],
    )


def _ins_game(
    db,
    game_pk=1,
    season_year=2026,
    home_team_id=119,
    away_team_id=147,
    home_score=5,
    away_score=3,
    game_date=None,
):
    if game_date is None:
        game_date = f"{season_year}-04-11"
    db.execute(
        """INSERT OR REPLACE INTO silver.games
               (game_pk, season_year, game_date, game_type, status,
                home_team_id, away_team_id, home_score, away_score,
                innings, venue_id, loaded_at)
           VALUES (?, ?, ?, 'R', 'Final', ?, ?, ?, ?, 9, 22, current_timestamp)""",
        [game_pk, season_year, game_date, home_team_id, away_team_id, home_score, away_score],
    )


def _ins_game_batting(
    db,
    game_pk=1,
    player_id=660271,
    team_id=119,
    position_abbrev="DH",
):
    db.execute(
        """INSERT OR REPLACE INTO silver.game_batting
               (game_pk, player_id, team_id, is_home, batting_order, position_abbrev,
                at_bats, runs, hits, doubles, triples, home_runs, rbi, walks,
                strikeouts, left_on_base, loaded_at)
           VALUES (?, ?, ?, TRUE, 300, ?, 4, 2, 2, 1, 0, 1, 3, 1, 1, 0, current_timestamp)""",
        [game_pk, player_id, team_id, position_abbrev],
    )


def _ins_game_pitching(
    db,
    game_pk=1,
    player_id=123456,
    team_id=119,
):
    db.execute(
        """INSERT OR REPLACE INTO silver.game_pitching
               (game_pk, player_id, team_id, is_home,
                wins, losses, saves, holds, blown_saves,
                games_started, games_finished, complete_games, shutouts,
                outs, hits_allowed, runs_allowed, earned_runs,
                home_runs_allowed, walks, strikeouts,
                hit_by_pitch, pitches_thrown, strikes, loaded_at)
           VALUES (?, ?, ?, TRUE, 1, 0, 0, 0, 0, 1, 0, 0, 0,
                   18, 4, 1, 1, 0, 2, 7, 0, 92, 61, current_timestamp)""",
        [game_pk, player_id, team_id],
    )


def _seed_leaders_page(db) -> None:
    _ins_season(db)
    _ins_league(db)
    _ins_division(db)
    _ins_league(db, league_id=103, name="American League", abbrev="AL")
    _ins_division(db, division_id=201, name="AL East", league_id=103)
    _ins_venue(db)
    _ins_team(db, team_id=119, season_year=2026, abbrev="LAD")
    _ins_team(
        db,
        team_id=147,
        season_year=2026,
        name="New York Yankees",
        abbrev="NYY",
        league_id=103,
        division_id=201,
        venue_id=22,
    )
    _ins_player(db, player_id=660271, full_name="Shohei Ohtani", primary_position="DH")
    _ins_player(db, player_id=123456, full_name="Tyler Glasnow", primary_position="SP")
    _ins_game(db)
    _ins_game_batting(db)
    _ins_game_pitching(db)
    Aggregator(db).run(scripts=["002_dim_team.sql"], force=True)


def test_leaders_page_renders_stats(db_file_path, monkeypatch):
    db_path, conn = db_file_path
    _seed_leaders_page(conn)
    conn.close()

    monkeypatch.setenv("MLB_DB_PATH", str(db_path))
    monkeypatch.setattr(app, "DB_PATH", db_path)

    at = AppTest.from_file("pages/6_Leaders.py").run(timeout=30)

    assert len(at.error) == 0
    assert len(at.info) == 0
    assert len(at.dataframe) == 2
    assert sum(1 for selectbox in at.selectbox if selectbox.label == "Choose player") == 2
    assert any("players — sorted by" in caption.value for caption in at.caption)
    assert any("pitchers — sorted by" in caption.value for caption in at.caption)


def test_player_profile_supports_career_stats(db_file_path, monkeypatch):
    db_path, conn = db_file_path
    _seed_leaders_page(conn)
    _ins_season(conn, season_year=2025)
    _ins_team(conn, team_id=119, season_year=2025, abbrev="LAD")
    _ins_team(
        conn,
        team_id=147,
        season_year=2025,
        name="New York Yankees",
        abbrev="NYY",
        league_id=103,
        division_id=201,
        venue_id=22,
    )
    _ins_game(conn, game_pk=2, season_year=2025, game_date="2025-08-10")
    _ins_game_batting(conn, game_pk=2, player_id=660271, team_id=119, position_abbrev="DH")
    _ins_game_pitching(conn, game_pk=2, player_id=123456, team_id=119)
    Aggregator(conn).run(scripts=["002_dim_team.sql"], force=True)
    conn.close()

    monkeypatch.setenv("MLB_DB_PATH", str(db_path))
    monkeypatch.setattr(app, "DB_PATH", db_path)

    at = AppTest.from_file("pages/7_Player_Profile.py")
    at.session_state["profile_player_id"] = 660271
    at.session_state["profile_season"] = "Career"
    at = at.run(timeout=30)

    assert len(at.error) == 0
    choose_player = next(selectbox for selectbox in at.selectbox if selectbox.label == "Choose Player")
    season_select = next(selectbox for selectbox in at.selectbox if selectbox.label == "Season")
    assert choose_player.value[1] == "Shohei Ohtani"
    assert season_select.options[0] == "Career"
    assert any("Career | Regular Season" in caption.value for caption in at.caption)

    batting_summary_df = at.dataframe[0].value
    assert int(batting_summary_df.iloc[0]["g"]) == 2


def test_players_page_has_profile_picker(db_file_path, monkeypatch):
    db_path, conn = db_file_path
    _seed_leaders_page(conn)
    Aggregator(conn).run(scripts=["001_dim_player.sql"], force=True)
    conn.close()

    monkeypatch.setenv("MLB_DB_PATH", str(db_path))
    monkeypatch.setattr(app, "DB_PATH", db_path)

    at = AppTest.from_file("pages/5_Players.py").run(timeout=30)

    assert len(at.error) == 0
    choose_player = next(selectbox for selectbox in at.selectbox if selectbox.label == "Choose Player")
    assert choose_player.value == (None, "Choose player")
