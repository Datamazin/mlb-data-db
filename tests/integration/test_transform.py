"""
Integration tests for the silver transformation layer (M4).

All tests use real DuckDB (no mocks). Bronze fixture data is written via
BronzeWriter so the exact same Parquet files the production pipeline produces
are used as transform inputs.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

from extractor.writer import BronzeWriter, _now_utc
from transformer.transform import Transformer, TransformResult

# ── Helpers ───────────────────────────────────────────────────────────────────

SILVER_SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "silver"


def _transformer(db: duckdb.DuckDBPyConnection, bronze_path: Path) -> Transformer:
    return Transformer(db, bronze_path)


def _team_raw(
    team_id: int = 119,
    team_name: str = "Los Angeles Dodgers",
    league_id: int = 104,
    league_name: str = "National League",
    league_abbrev: str = "NL",
    division_id: int = 203,
    division_name: str = "NL West",
    venue_id: int = 22,
    venue_name: str = "Dodger Stadium",
) -> dict:
    return {
        "id": team_id,
        "name": team_name,
        "league": {"id": league_id, "name": league_name,
                   "abbreviation": league_abbrev, "nameShort": league_name},
        "division": {"id": division_id, "name": division_name,
                     "abbreviation": "NLW", "nameShort": division_name},
        "venue": {"id": venue_id, "name": venue_name},
    }


def _team_record(
    team_id: int = 119,
    season_year: int = 2024,
    venue_id: int = 22,
    league_id: int = 104,
    division_id: int = 203,
    extracted_at: str | None = None,
    **raw_overrides,
) -> dict:
    raw = _team_raw(team_id=team_id)
    raw.update(raw_overrides)
    return {
        "team_id": team_id,
        "season_year": season_year,
        "team_name": "Los Angeles Dodgers",
        "team_abbrev": "LAD",
        "team_code": "lan",
        "league_id": league_id,
        "division_id": division_id,
        "venue_id": venue_id,
        "city": "Los Angeles",
        "first_year": 1884,
        "active": True,
        "raw_json": json.dumps(raw),
        "extracted_at": extracted_at or _now_utc(),
        "source_url": "/v1/teams?sportId=1&season=2024",
    }


def _game_raw(
    game_pk: int = 745525,
    venue_id: int = 3,
    venue_name: str = "Fenway Park",
    innings: int = 9,
    home_runs: int = 5,
    away_runs: int = 3,
) -> dict:
    return {
        "gamePk": game_pk,
        "gameData": {
            "venue": {"id": venue_id, "name": venue_name},
        },
        "liveData": {
            "linescore": {
                "innings": [
                    {
                        "num": i,
                        "home": {"runs": home_runs if i == 1 else 0, "hits": 1, "errors": 0},
                        "away": {"runs": away_runs if i == 1 else 0, "hits": 1, "errors": 0},
                    }
                    for i in range(1, innings + 1)
                ],
            },
            "boxscore": {
                "teams": {
                    "home": {
                        "team": {"id": 111, "name": "Boston Red Sox"},
                        "teamStats": {
                            "batting": {"runs": home_runs, "hits": 8, "leftOnBase": 6},
                            "fielding": {"errors": 0},
                        },
                        "battingOrder": [100001, 100002, 100003],
                        "pitchers": [200001],
                    },
                    "away": {
                        "team": {"id": 147, "name": "New York Yankees"},
                        "teamStats": {
                            "batting": {"runs": away_runs, "hits": 7, "leftOnBase": 5},
                            "fielding": {"errors": 1},
                        },
                        "battingOrder": [300001, 300002, 300003],
                        "pitchers": [400001],
                    },
                },
            },
        },
    }


def _game_record(
    game_pk: int = 745525,
    season_year: int = 2024,
    game_date: str = "2024-07-04",
    game_type: str = "R",
    extracted_at: str | None = None,
    venue_id: int = 3,
    home_runs: int = 5,
    away_runs: int = 3,
    innings: int = 9,
) -> dict:
    raw = _game_raw(game_pk=game_pk, venue_id=venue_id,
                    home_runs=home_runs, away_runs=away_runs, innings=innings)
    return {
        "game_pk": game_pk,
        "season_year": season_year,
        "game_date": game_date,
        "game_datetime": "2024-07-04T17:10:00Z",
        "game_type": game_type,
        "status_detailed_state": "Final",
        "home_team_id": 111,
        "away_team_id": 147,
        "home_score": home_runs,
        "away_score": away_runs,
        "innings": innings,
        "venue_id": venue_id,
        "attendance": 37755,
        "game_duration_min": 185,
        "double_header": "N",
        "series_description": "Regular Season",
        "series_game_num": 1,
        "raw_json": json.dumps(raw),
        "extracted_at": extracted_at or _now_utc(),
        "source_url": f"/v1.1/game/{game_pk}/feed/live",
    }


def _player_record(
    player_id: int = 660271,
    season_year: int = 2024,
    bats: str = "L",
    throws: str = "R",
    birth_date: str = "1994-07-05",
    mlb_debut_date: str = "2018-03-29",
    extracted_at: str | None = None,
) -> dict:
    return {
        "player_id": player_id,
        "full_name": "Shohei Ohtani",
        "first_name": "Shohei",
        "last_name": "Ohtani",
        "birth_date": birth_date,
        "birth_city": "Oshu",
        "birth_country": "Japan",
        "height": "6' 4\"",
        "weight": 210,
        "bats": bats,
        "throws": throws,
        "primary_position": "DH",
        "mlb_debut_date": mlb_debut_date,
        "active": True,
        "raw_json": json.dumps({"id": player_id}),
        "extracted_at": extracted_at or _now_utc(),
        "source_url": f"/v1/people/{player_id}",
    }


# ── Seasons ───────────────────────────────────────────────────────────────────

class TestSeedSeasons:
    def test_all_five_seasons_seeded(self, db):
        t = _transformer(db, Path("/nonexistent"))
        t.run(scripts=["001_seed_seasons.sql"])
        count = db.execute("SELECT COUNT(*) FROM silver.seasons").fetchone()[0]
        assert count == 5

    def test_correct_season_years(self, db):
        t = _transformer(db, Path("/nonexistent"))
        t.run(scripts=["001_seed_seasons.sql"])
        years = {r[0] for r in db.execute("SELECT season_year FROM silver.seasons").fetchall()}
        assert years == {2022, 2023, 2024, 2025, 2026}

    def test_rerun_is_idempotent(self, db):
        t = _transformer(db, Path("/nonexistent"))
        t.run(scripts=["001_seed_seasons.sql"], force=True)
        t.run(scripts=["001_seed_seasons.sql"], force=True)
        count = db.execute("SELECT COUNT(*) FROM silver.seasons").fetchone()[0]
        assert count == 5

    def test_2026_postseason_is_null(self, db):
        t = _transformer(db, Path("/nonexistent"))
        t.run(scripts=["001_seed_seasons.sql"])
        row = db.execute(
            "SELECT postseason_start, world_series_end FROM silver.seasons WHERE season_year = 2026"
        ).fetchone()
        assert row == (None, None)


# ── Leagues ───────────────────────────────────────────────────────────────────

class TestLeaguesTransform:
    def test_league_extracted_from_raw_json(self, db, bronze_path):
        BronzeWriter(bronze_path).write_teams([_team_record()], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["002_leagues.sql"])
        row = db.execute("SELECT league_id, league_name, abbreviation FROM silver.leagues").fetchone()
        assert row == (104, "National League", "NL")

    def test_deduplication_latest_wins(self, db, bronze_path):
        w = BronzeWriter(bronze_path)
        w.write_teams(
            [_team_record(extracted_at="2024-01-01T00:00:00Z")], season_year=2024
        )
        updated = _team_record(season_year=2024, extracted_at="2024-06-01T00:00:00Z")
        updated["raw_json"] = json.dumps(_team_raw(league_name="National League Updated"))
        w.write_teams([updated], season_year=2024)

        t = _transformer(db, bronze_path)
        t.run(scripts=["002_leagues.sql"])
        row = db.execute("SELECT league_name FROM silver.leagues WHERE league_id = 104").fetchone()
        assert row[0] == "National League Updated"

    def test_multiple_teams_same_league_one_row(self, db, bronze_path):
        BronzeWriter(bronze_path).write_teams(
            [_team_record(team_id=119), _team_record(team_id=118)], season_year=2024
        )
        t = _transformer(db, bronze_path)
        t.run(scripts=["002_leagues.sql"])
        count = db.execute("SELECT COUNT(*) FROM silver.leagues WHERE league_id = 104").fetchone()[0]
        assert count == 1


# ── Divisions ─────────────────────────────────────────────────────────────────

class TestDivisionsTransform:
    def test_division_includes_league_id(self, db, bronze_path):
        BronzeWriter(bronze_path).write_teams([_team_record()], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql"])
        row = db.execute(
            "SELECT division_id, division_name, league_id FROM silver.divisions"
        ).fetchone()
        assert row == (203, "NL West", 104)


# ── Venues ────────────────────────────────────────────────────────────────────

class TestVenuesTransform:
    def test_venue_from_teams_bronze(self, db, bronze_path):
        BronzeWriter(bronze_path).write_teams([_team_record()], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["004_venues.sql"])
        row = db.execute("SELECT venue_id, venue_name FROM silver.venues WHERE venue_id = 22").fetchone()
        assert row == (22, "Dodger Stadium")

    def test_venue_from_games_bronze(self, seeded_db, bronze_path):
        # Use venue_id=22 which also appears in the team record so venues resolves it
        BronzeWriter(bronze_path).write_teams([_team_record(venue_id=22)], season_year=2024)
        rec = _game_record(venue_id=22)
        BronzeWriter(bronze_path).write_games([rec], for_date=date(2024, 7, 4))
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["004_venues.sql"])
        row = seeded_db.execute("SELECT venue_id, venue_name FROM silver.venues WHERE venue_id = 22").fetchone()
        assert row == (22, "Dodger Stadium")

    def test_nullable_columns_are_null(self, db, bronze_path):
        BronzeWriter(bronze_path).write_teams([_team_record()], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["004_venues.sql"])
        row = db.execute("SELECT city, surface, capacity FROM silver.venues WHERE venue_id = 22").fetchone()
        assert row == (None, None, None)


# ── Teams ─────────────────────────────────────────────────────────────────────

class TestTeamsTransform:
    def _seed_refs(self, t: Transformer, bronze_path: Path) -> None:
        """Load leagues, divisions, venues prerequisites for teams."""
        BronzeWriter(bronze_path).write_teams([_team_record()], season_year=2024)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql", "004_venues.sql"], force=True)

    def test_team_loaded_correctly(self, seeded_db, bronze_path):
        t = _transformer(seeded_db, bronze_path)
        self._seed_refs(t, bronze_path)
        t.run(scripts=["005_teams.sql"], force=True)
        row = seeded_db.execute(
            "SELECT team_id, team_name, team_abbrev, season_year FROM silver.teams"
        ).fetchone()
        assert row == (119, "Los Angeles Dodgers", "LAD", 2024)

    def test_same_team_two_seasons_two_rows(self, seeded_db, bronze_path):
        w = BronzeWriter(bronze_path)
        w.write_teams([_team_record(season_year=2024)], season_year=2024)
        w.write_teams([_team_record(season_year=2025)], season_year=2025)
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql", "004_venues.sql"])
        t.run(scripts=["005_teams.sql"])
        count = seeded_db.execute(
            "SELECT COUNT(*) FROM silver.teams WHERE team_id = 119"
        ).fetchone()[0]
        assert count == 2

    def test_unknown_season_excluded(self, seeded_db, bronze_path):
        # season_year=2021 is not in silver.seasons — row should be excluded
        BronzeWriter(bronze_path).write_teams([_team_record(season_year=2021)], season_year=2021)
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql", "004_venues.sql", "005_teams.sql"])
        count = seeded_db.execute("SELECT COUNT(*) FROM silver.teams").fetchone()[0]
        assert count == 0


# ── Players ───────────────────────────────────────────────────────────────────

class TestPlayersTransform:
    def test_player_loaded_with_correct_fields(self, db, bronze_path):
        BronzeWriter(bronze_path).write_players([_player_record()], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["006_players.sql"])
        row = db.execute(
            "SELECT player_id, full_name, bats, throws, birth_country FROM silver.players"
        ).fetchone()
        assert row == (660271, "Shohei Ohtani", "L", "R", "Japan")

    def test_birth_date_cast_to_date(self, db, bronze_path):
        BronzeWriter(bronze_path).write_players([_player_record()], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["006_players.sql"])
        row = db.execute("SELECT birth_date FROM silver.players WHERE player_id = 660271").fetchone()
        assert row[0] == date(1994, 7, 5)

    def test_invalid_birth_date_becomes_null(self, db, bronze_path):
        rec = _player_record(birth_date="not-a-date", mlb_debut_date="")
        BronzeWriter(bronze_path).write_players([rec], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["006_players.sql"])
        row = db.execute(
            "SELECT birth_date, mlb_debut_date FROM silver.players WHERE player_id = 660271"
        ).fetchone()
        assert row == (None, None)

    def test_scd_type1_latest_wins(self, db, bronze_path):
        w = BronzeWriter(bronze_path)
        w.write_players([_player_record(bats="L", extracted_at="2024-01-01T00:00:00Z")], season_year=2024)
        w.write_players([_player_record(bats="S", extracted_at="2024-06-01T00:00:00Z")], season_year=2025)
        t = _transformer(db, bronze_path)
        t.run(scripts=["006_players.sql"])
        row = db.execute("SELECT bats FROM silver.players WHERE player_id = 660271").fetchone()
        assert row[0] == "S"

    def test_bats_throws_truncated(self, db, bronze_path):
        rec = _player_record(bats="Left", throws="Right")
        BronzeWriter(bronze_path).write_players([rec], season_year=2024)
        t = _transformer(db, bronze_path)
        t.run(scripts=["006_players.sql"])
        row = db.execute("SELECT bats, throws FROM silver.players WHERE player_id = 660271").fetchone()
        assert row == ("L", "R")


# ── Games ─────────────────────────────────────────────────────────────────────

class TestGamesTransform:
    def _seed_venues(self, t: Transformer, bronze_path: Path, game_record: dict) -> None:
        """Populate silver.venues from a game record so the FK is satisfied."""
        BronzeWriter(bronze_path).write_games([game_record], for_date=date(2024, 7, 4))
        t.run(scripts=["004_venues.sql"], force=True)

    def test_game_loaded_with_correct_fields(self, seeded_db, bronze_path):
        rec = _game_record()
        t = _transformer(seeded_db, bronze_path)
        self._seed_venues(t, bronze_path, rec)
        t.run(scripts=["007_games.sql"], force=True)
        row = seeded_db.execute(
            "SELECT game_pk, season_year, game_type, home_score, away_score, innings "
            "FROM silver.games"
        ).fetchone()
        assert row == (745525, 2024, "R", 5, 3, 9)

    def test_game_date_cast_to_date(self, seeded_db, bronze_path):
        rec = _game_record()
        t = _transformer(seeded_db, bronze_path)
        self._seed_venues(t, bronze_path, rec)
        t.run(scripts=["007_games.sql"], force=True)
        row = seeded_db.execute("SELECT game_date FROM silver.games WHERE game_pk = 745525").fetchone()
        assert row[0] == date(2024, 7, 4)

    def test_spring_training_game_included(self, seeded_db, bronze_path):
        rec = _game_record(game_pk=900001, game_date="2024-03-05", game_type="S")
        t = _transformer(seeded_db, bronze_path)
        self._seed_venues(t, bronze_path, rec)
        t.run(scripts=["007_games.sql"], force=True)
        row = seeded_db.execute(
            "SELECT game_type FROM silver.games WHERE game_pk = 900001"
        ).fetchone()
        assert row[0] == "S"

    def test_unknown_season_excluded(self, seeded_db, bronze_path):
        rec = _game_record(game_pk=800001, season_year=2021, game_date="2021-07-01")
        t = _transformer(seeded_db, bronze_path)
        self._seed_venues(t, bronze_path, rec)
        t.run(scripts=["007_games.sql"], force=True)
        count = seeded_db.execute("SELECT COUNT(*) FROM silver.games WHERE game_pk = 800001").fetchone()[0]
        assert count == 0

    def test_dedup_latest_wins(self, seeded_db, bronze_path):
        w = BronzeWriter(bronze_path)
        rec_v1 = _game_record(home_runs=1, extracted_at="2024-07-04T20:00:00Z")
        rec_v2 = _game_record(home_runs=5, extracted_at="2024-07-04T23:00:00Z")
        w.write_games([rec_v1], for_date=date(2024, 7, 4))
        w.write_games([rec_v2], for_date=date(2024, 7, 5))
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["004_venues.sql", "007_games.sql"])
        row = seeded_db.execute("SELECT home_score FROM silver.games WHERE game_pk = 745525").fetchone()
        assert row[0] == 5


# ── Linescore ─────────────────────────────────────────────────────────────────

class TestLinescoreTransform:
    def test_inning_rows_created(self, seeded_db, bronze_path):
        BronzeWriter(bronze_path).write_games([_game_record(innings=9)], for_date=date(2024, 7, 4))
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["004_venues.sql", "007_games.sql", "008_game_linescore.sql"])
        count = seeded_db.execute(
            "SELECT COUNT(*) FROM silver.game_linescore WHERE game_pk = 745525"
        ).fetchone()[0]
        assert count == 9

    def test_runs_values_correct(self, seeded_db, bronze_path):
        BronzeWriter(bronze_path).write_games(
            [_game_record(home_runs=5, away_runs=3, innings=1)], for_date=date(2024, 7, 4)
        )
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["004_venues.sql", "007_games.sql", "008_game_linescore.sql"])
        row = seeded_db.execute(
            "SELECT home_runs, away_runs FROM silver.game_linescore WHERE game_pk = 745525 AND inning = 1"
        ).fetchone()
        assert row == (5, 3)

    def test_null_runs_coalesced_to_zero(self, seeded_db, bronze_path):
        # Inning with no runs field in JSON → should coalesce to 0
        raw = _game_raw(game_pk=745525)
        # Remove 'runs' from inning 1
        raw["liveData"]["linescore"]["innings"][0]["home"].pop("runs", None)
        raw["liveData"]["linescore"]["innings"][0]["away"].pop("runs", None)
        rec = _game_record()
        rec["raw_json"] = json.dumps(raw)
        BronzeWriter(bronze_path).write_games([rec], for_date=date(2024, 7, 4))
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["004_venues.sql", "007_games.sql", "008_game_linescore.sql"])
        row = seeded_db.execute(
            "SELECT home_runs, away_runs FROM silver.game_linescore WHERE game_pk = 745525 AND inning = 1"
        ).fetchone()
        assert row == (0, 0)


# ── Boxscore ──────────────────────────────────────────────────────────────────

class TestBoxscoreTransform:
    def _load_game(self, seeded_db: duckdb.DuckDBPyConnection, bronze_path: Path) -> None:
        BronzeWriter(bronze_path).write_games([_game_record()], for_date=date(2024, 7, 4))
        t = _transformer(seeded_db, bronze_path)
        t.run(scripts=["004_venues.sql", "007_games.sql", "009_game_boxscore.sql"])

    def test_home_and_away_rows_created(self, seeded_db, bronze_path):
        self._load_game(seeded_db, bronze_path)
        count = seeded_db.execute(
            "SELECT COUNT(*) FROM silver.game_boxscore WHERE game_pk = 745525"
        ).fetchone()[0]
        assert count == 2

    def test_is_home_flag(self, seeded_db, bronze_path):
        self._load_game(seeded_db, bronze_path)
        flags = {r[0] for r in seeded_db.execute(
            "SELECT is_home FROM silver.game_boxscore WHERE game_pk = 745525"
        ).fetchall()}
        assert flags == {True, False}

    def test_batting_order_is_json(self, seeded_db, bronze_path):
        self._load_game(seeded_db, bronze_path)
        row = seeded_db.execute(
            "SELECT batting_order FROM silver.game_boxscore WHERE game_pk = 745525 AND is_home = TRUE"
        ).fetchone()
        order = json.loads(row[0])
        assert isinstance(order, list)
        assert len(order) == 3


# ── Transformer runner ─────────────────────────────────────────────────────────

class TestTransformerRunner:
    def test_run_all_scripts_succeeds(self, seeded_db, bronze_path):
        # With no bronze data, fact/venue scripts should be no-ops (not errors)
        t = _transformer(seeded_db, bronze_path)
        result = t.run(force=True)
        assert result.success
        assert result.scripts_run == len(list((Path(__file__).parent.parent.parent / "sql" / "silver").glob("*.sql")))

    def test_run_specific_scripts(self, db, bronze_path):
        t = _transformer(db, bronze_path)
        result = t.run(scripts=["001_seed_seasons.sql"])
        assert result.scripts_run == 1
        assert db.execute("SELECT COUNT(*) FROM silver.seasons").fetchone()[0] == 5

    def test_dry_run_does_not_write(self, db, bronze_path):
        t = _transformer(db, bronze_path)
        result = t.run(scripts=["001_seed_seasons.sql"], dry_run=True)
        assert db.execute("SELECT COUNT(*) FROM silver.seasons").fetchone()[0] == 0

    def test_checksum_skips_unchanged_script(self, db, bronze_path):
        t = _transformer(db, bronze_path)
        t.run(scripts=["001_seed_seasons.sql"])
        result = t.run(scripts=["001_seed_seasons.sql"])
        # Second run: script checksum unchanged → skipped
        assert result.scripts_run == 0

    def test_force_reruns_script(self, db, bronze_path):
        t = _transformer(db, bronze_path)
        t.run(scripts=["001_seed_seasons.sql"])
        result = t.run(scripts=["001_seed_seasons.sql"], force=True)
        assert result.scripts_run == 1

    def test_tracking_table_created(self, db, bronze_path):
        _transformer(db, bronze_path)  # bootstrap happens in __init__
        tables = {r[0] for r in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'meta'"
        ).fetchall()}
        assert "_silver_transforms" in tables

    def test_tracking_row_recorded(self, db, bronze_path):
        t = _transformer(db, bronze_path)
        t.run(scripts=["001_seed_seasons.sql"])
        row = db.execute(
            "SELECT script_name FROM meta._silver_transforms"
        ).fetchone()
        assert row[0] == "001_seed_seasons.sql"
