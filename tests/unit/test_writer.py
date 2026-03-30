"""
Unit tests for the bronze Parquet writer.

Validates that:
  - Files are written to the correct partitioned paths
  - Written files are readable by DuckDB with the expected columns and types
  - Empty record lists produce no file (no zero-byte Parquet files)
  - Required fields are present; nullable fields may be None
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from extractor.writer import (
    BronzeWriter,
    GAME_SCHEMA,
    PLAYER_SCHEMA,
    TEAM_SCHEMA,
    _now_utc,
)


def _game_record(**overrides) -> dict:
    base = {
        "game_pk":               745525,
        "season_year":           2024,
        "game_date":             "2024-07-04",
        "game_datetime":         "2024-07-04T17:10:00+00:00",
        "game_type":             "R",
        "status_detailed_state": "Final",
        "home_team_id":          111,
        "away_team_id":          147,
        "home_score":            5,
        "away_score":            3,
        "innings":               9,
        "venue_id":              3,
        "attendance":            37755,
        "game_duration_min":     185,
        "double_header":         "N",
        "series_description":    "Regular Season",
        "series_game_num":       1,
        "raw_json":              '{"gamePk": 745525}',
        "extracted_at":          _now_utc(),
        "source_url":            "/v1/game/745525/feed/live",
    }
    return {**base, **overrides}


def _player_record(**overrides) -> dict:
    base = {
        "player_id":        660271,
        "full_name":        "Shohei Ohtani",
        "first_name":       "Shohei",
        "last_name":        "Ohtani",
        "birth_date":       "1994-07-05",
        "birth_city":       "Oshu",
        "birth_country":    "Japan",
        "height":           "6' 4\"",
        "weight":           210,
        "bats":             "L",
        "throws":           "R",
        "primary_position": "DH",
        "mlb_debut_date":   "2018-03-29",
        "active":           True,
        "raw_json":         '{"id": 660271}',
        "extracted_at":     _now_utc(),
        "source_url":       "/v1/people/660271",
    }
    return {**base, **overrides}


def _team_record(**overrides) -> dict:
    base = {
        "team_id":     119,
        "season_year": 2024,
        "team_name":   "Los Angeles Dodgers",
        "team_abbrev": "LAD",
        "team_code":   "lan",
        "league_id":   104,
        "division_id": 203,
        "venue_id":    22,
        "city":        "Los Angeles",
        "first_year":  1884,
        "active":      True,
        "raw_json":    '{"id": 119}',
        "extracted_at": _now_utc(),
        "source_url":  "/v1/teams?sportId=1&season=2024",
    }
    return {**base, **overrides}


class TestBronzeWriterGames:
    def test_writes_to_correct_path(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        for_date = date(2024, 7, 4)
        path = writer.write_games([_game_record()], for_date=for_date)
        assert path.exists()
        assert "year=2024" in str(path)
        assert "month=07" in str(path)
        assert "games_20240704.parquet" in str(path)

    def test_duckdb_reads_columns(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        for_date = date(2024, 7, 4)
        path = writer.write_games([_game_record()], for_date=for_date)

        conn = duckdb.connect(":memory:")
        row = conn.execute(f"SELECT * FROM read_parquet('{path}')").fetchone()
        assert row is not None

        cols = [desc[0] for desc in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()]
        for field in GAME_SCHEMA:
            assert field.name in cols, f"Column '{field.name}' missing from Parquet output"

    def test_key_values_round_trip(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        for_date = date(2024, 7, 4)
        writer.write_games([_game_record()], for_date=for_date)

        conn = duckdb.connect(":memory:")
        row = conn.execute(
            f"SELECT game_pk, home_score, away_score, status_detailed_state "
            f"FROM read_parquet('{bronze_path}/games/year=2024/month=07/games_20240704.parquet')"
        ).fetchone()
        assert row == (745525, 5, 3, "Final")

    def test_nullable_fields_accepted(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        record = _game_record(
            home_score=None, away_score=None, innings=None,
            attendance=None, game_duration_min=None, venue_id=None,
            series_description=None, series_game_num=None, game_datetime=None,
        )
        path = writer.write_games([record], for_date=date(2024, 7, 4))
        assert path.exists()

    def test_empty_records_no_file(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        path = writer.write_games([], for_date=date(2024, 7, 4))
        assert not path.exists()

    def test_multiple_games_same_date(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        records = [_game_record(game_pk=745525), _game_record(game_pk=745526)]
        path = writer.write_games(records, for_date=date(2024, 7, 4))

        conn = duckdb.connect(":memory:")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        assert count == 2


class TestBronzeWriterPlayers:
    def test_writes_to_correct_path(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        path = writer.write_players([_player_record()], season_year=2024)
        assert path.exists()
        assert "season=2024" in str(path)
        assert "players_2024.parquet" in str(path)

    def test_duckdb_reads_player_columns(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        path = writer.write_players([_player_record()], season_year=2024)

        conn = duckdb.connect(":memory:")
        cols = [desc[0] for desc in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()]
        for field in PLAYER_SCHEMA:
            assert field.name in cols

    def test_player_values_round_trip(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        path = writer.write_players([_player_record()], season_year=2024)

        conn = duckdb.connect(":memory:")
        row = conn.execute(
            f"SELECT player_id, full_name, bats, throws FROM read_parquet('{path}')"
        ).fetchone()
        assert row == (660271, "Shohei Ohtani", "L", "R")


class TestBronzeWriterTeams:
    def test_writes_to_correct_path(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        path = writer.write_teams([_team_record()], season_year=2024)
        assert path.exists()
        assert "season=2024" in str(path)

    def test_team_values_round_trip(self, bronze_path):
        writer = BronzeWriter(bronze_path)
        path = writer.write_teams([_team_record()], season_year=2024)

        conn = duckdb.connect(":memory:")
        row = conn.execute(
            f"SELECT team_id, team_name, team_abbrev, league_id FROM read_parquet('{path}')"
        ).fetchone()
        assert row == (119, "Los Angeles Dodgers", "LAD", 104)
