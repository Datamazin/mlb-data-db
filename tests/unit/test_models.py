"""
Unit tests for Pydantic extraction models.

These tests validate model parsing against representative API response shapes.
They run offline — no network calls, no DuckDB.

The fixture payloads are deliberately minimal: they include only the fields
our models actually use. Extra fields (extra='ignore') are NOT tested here
because they are, by definition, silently dropped. The explore_api.py script
is the right tool for discovering what extra fields exist in the live API.
"""

from __future__ import annotations

import pytest

from extractor.models.game_feed import GameFeedResponse
from extractor.models.player import PersonResponse
from extractor.models.schedule import ScheduleResponse
from extractor.models.team import RosterResponse, TeamsResponse


# ── Schedule ───────────────────────────────────────────────────────────────────

SCHEDULE_PAYLOAD = {
    "totalItems": 2,
    "totalEvents": 0,
    "totalGames": 2,
    "totalGamesInProgress": 0,
    "dates": [
        {
            "date": "2024-07-04",
            "totalItems": 2,
            "totalGames": 2,
            "games": [
                {
                    "gamePk": 745525,
                    "gameType": "R",
                    "season": "2024",
                    "gameDate": "2024-07-04T17:10:00Z",
                    "officialDate": "2024-07-04",
                    "status": {
                        "detailedState": "Final",
                        "abstractGameState": "Final",
                    },
                    "teams": {
                        "home": {"team": {"id": 111, "name": "Boston Red Sox"}, "score": 5, "isWinner": True},
                        "away": {"team": {"id": 147, "name": "New York Yankees"}, "score": 3, "isWinner": False},
                    },
                    "venue": {"id": 3, "name": "Fenway Park"},
                    "doubleHeader": "N",
                    "gamesInSeries": 3,
                    "seriesGameNumber": 1,
                    "seriesDescription": "Regular Season",
                },
                {
                    "gamePk": 745600,
                    "gameType": "R",
                    "season": "2024",
                    "gameDate": "2024-07-04T20:10:00Z",
                    "officialDate": "2024-07-04",
                    "status": {"detailedState": "Final", "abstractGameState": "Final"},
                    "teams": {
                        "home": {"team": {"id": 119, "name": "Los Angeles Dodgers"}, "score": 8},
                        "away": {"team": {"id": 135, "name": "San Diego Padres"}, "score": 2},
                    },
                    "doubleHeader": "N",
                },
            ],
        }
    ],
}


class TestScheduleResponse:
    def test_parses_total_games(self):
        model = ScheduleResponse.model_validate(SCHEDULE_PAYLOAD)
        assert model.total_games == 2

    def test_all_game_pks(self):
        model = ScheduleResponse.model_validate(SCHEDULE_PAYLOAD)
        assert model.all_game_pks() == [745525, 745600]

    def test_game_details(self):
        model = ScheduleResponse.model_validate(SCHEDULE_PAYLOAD)
        game = model.dates[0].games[0]
        assert game.game_pk == 745525
        assert game.game_type == "R"
        assert game.official_date == "2024-07-04"
        assert game.status.detailed_state == "Final"
        assert game.teams.home.team.id == 111
        assert game.teams.away.score == 3
        assert game.venue.id == 3

    def test_optional_venue_is_none(self):
        """Games without a venue reference should not fail validation."""
        model = ScheduleResponse.model_validate(SCHEDULE_PAYLOAD)
        game = model.dates[0].games[1]
        assert game.venue is None

    def test_extra_fields_ignored(self):
        """Unknown API fields must not raise a validation error."""
        payload = dict(SCHEDULE_PAYLOAD, unknownFutureField="surprise", totalEvents=99)
        model = ScheduleResponse.model_validate(payload)
        assert model.total_games == 2  # parsed correctly despite extra field

    def test_empty_response(self):
        model = ScheduleResponse.model_validate({"totalItems": 0, "totalGames": 0, "dates": []})
        assert model.all_game_pks() == []


# ── Game feed ──────────────────────────────────────────────────────────────────

GAME_FEED_PAYLOAD = {
    "gamePk": 745525,
    "gameData": {
        "game": {
            "pk": 745525,
            "type": "R",
            "doubleHeader": "N",
            "gameNumber": 1,
            "season": "2024",
        },
        "datetime": {
            "dateTime": "2024-07-04T17:10:00Z",
            "officialDate": "2024-07-04",
            "dayNight": "day",
        },
        "status": {
            "detailedState": "Final",
            "abstractGameState": "Final",
            "codedGameState": "F",
        },
        "teams": {
            "home": {"id": 111, "name": "Boston Red Sox", "abbreviation": "BOS"},
            "away": {"id": 147, "name": "New York Yankees", "abbreviation": "NYY"},
        },
        "venue": {"id": 3, "name": "Fenway Park"},
        "gameInfo": {
            "attendance": 37755,
            "gameDurationMinutes": 185,
        },
        "seriesDescription": "Regular Season",
        "seriesGameNumber": 1,
        "gamesInSeries": 3,
    },
    "liveData": {
        "linescore": {
            "currentInning": 9,
            "innings": [
                {"num": i, "home": {"runs": 0, "hits": 1, "errors": 0}, "away": {"runs": 0, "hits": 1, "errors": 0}}
                for i in range(1, 10)
            ],
            "teams": {
                "home": {"runs": 5, "hits": 8, "errors": 0, "leftOnBase": 6},
                "away": {"runs": 3, "hits": 7, "errors": 1, "leftOnBase": 5},
            },
        },
        "boxscore": {
            "teams": {
                "home": {
                    "team": {"id": 111, "name": "Boston Red Sox"},
                    "battingOrder": [646240, 664034, 596019],
                    "pitchers": [592789],
                    "batters": [646240, 664034, 596019],
                },
                "away": {
                    "team": {"id": 147, "name": "New York Yankees"},
                    "battingOrder": [547989, 592450, 519317],
                    "pitchers": [543037],
                    "batters": [547989, 592450, 519317],
                },
            },
        },
    },
}


class TestGameFeedResponse:
    def test_parses_game_pk(self):
        model = GameFeedResponse.model_validate(GAME_FEED_PAYLOAD)
        assert model.game_pk == 745525

    def test_game_data_fields(self):
        model = GameFeedResponse.model_validate(GAME_FEED_PAYLOAD)
        gd = model.game_data
        assert gd.game.type == "R"
        assert gd.game.season == "2024"
        assert gd.datetime.official_date == "2024-07-04"
        assert gd.status.detailed_state == "Final"
        assert gd.teams.home.id == 111
        assert gd.teams.away.abbreviation == "NYY"
        assert gd.venue.id == 3

    def test_game_info(self):
        model = GameFeedResponse.model_validate(GAME_FEED_PAYLOAD)
        gi = model.game_data.game_info
        assert gi.attendance == 37755
        assert gi.game_duration_minutes == 185

    def test_score_properties(self):
        model = GameFeedResponse.model_validate(GAME_FEED_PAYLOAD)
        assert model.home_score == 5
        assert model.away_score == 3

    def test_innings_played(self):
        model = GameFeedResponse.model_validate(GAME_FEED_PAYLOAD)
        assert model.innings_played == 9

    def test_linescore_innings(self):
        model = GameFeedResponse.model_validate(GAME_FEED_PAYLOAD)
        ls = model.live_data.linescore
        assert ls is not None
        assert len(ls.innings) == 9
        assert ls.innings[0].num == 1

    def test_boxscore_batting_order(self):
        model = GameFeedResponse.model_validate(GAME_FEED_PAYLOAD)
        bs = model.live_data.boxscore
        assert bs is not None
        assert 646240 in bs.teams.home.batting_order

    def test_missing_game_info_is_none(self):
        payload = {
            k: v for k, v in GAME_FEED_PAYLOAD.items()
        }
        payload["gameData"] = {**GAME_FEED_PAYLOAD["gameData"]}
        del payload["gameData"]["gameInfo"]
        model = GameFeedResponse.model_validate(payload)
        assert model.game_data.game_info is None
        assert model.home_score == 5  # linescore still works

    def test_extra_fields_ignored(self):
        payload = dict(GAME_FEED_PAYLOAD, newField="future_value")
        model = GameFeedResponse.model_validate(payload)
        assert model.game_pk == 745525


# ── Player ─────────────────────────────────────────────────────────────────────

PERSON_PAYLOAD = {
    "people": [
        {
            "id": 660271,
            "fullName": "Shohei Ohtani",
            "firstName": "Shohei",
            "lastName": "Ohtani",
            "birthDate": "1994-07-05",
            "birthCity": "Oshu",
            "birthCountry": "Japan",
            "height": "6' 4\"",
            "weight": 210,
            "batSide": {"code": "L", "description": "Left"},
            "pitchHand": {"code": "R", "description": "Right"},
            "primaryPosition": {"code": "DH", "description": "Designated Hitter"},
            "mlbDebutDate": "2018-03-29",
            "active": True,
        }
    ]
}


class TestPersonResponse:
    def test_parses_person(self):
        model = PersonResponse.model_validate(PERSON_PAYLOAD)
        person = model.person
        assert person is not None
        assert person.id == 660271
        assert person.full_name == "Shohei Ohtani"

    def test_convenience_properties(self):
        model = PersonResponse.model_validate(PERSON_PAYLOAD)
        person = model.person
        assert person.bats == "L"
        assert person.throws == "R"
        assert person.position_code == "DH"

    def test_empty_people_list(self):
        model = PersonResponse.model_validate({"people": []})
        assert model.person is None

    def test_extra_fields_ignored(self):
        payload = {"people": [{**PERSON_PAYLOAD["people"][0], "draftYear": 2013}]}
        model = PersonResponse.model_validate(payload)
        assert model.person.id == 660271


# ── Teams ──────────────────────────────────────────────────────────────────────

TEAMS_PAYLOAD = {
    "teams": [
        {
            "id": 119,
            "name": "Los Angeles Dodgers",
            "abbreviation": "LAD",
            "teamCode": "lan",
            "locationName": "Los Angeles",
            "firstYearOfPlay": "1884",
            "active": True,
            "league": {"id": 104, "name": "National League"},
            "division": {"id": 203, "name": "NL West"},
            "venue": {"id": 22, "name": "Dodger Stadium"},
            "sport": {"id": 1, "name": "Major League Baseball"},
        }
    ]
}


class TestTeamsResponse:
    def test_parses_team(self):
        model = TeamsResponse.model_validate(TEAMS_PAYLOAD)
        assert len(model.teams) == 1
        team = model.teams[0]
        assert team.id == 119
        assert team.abbreviation == "LAD"
        assert team.league.id == 104
        assert team.division.id == 203
        assert team.venue.id == 22

    def test_first_year_as_string(self):
        """API returns firstYearOfPlay as a string, not int."""
        model = TeamsResponse.model_validate(TEAMS_PAYLOAD)
        assert model.teams[0].first_year_of_play == "1884"

    def test_extra_fields_ignored(self):
        payload = {"teams": [{**TEAMS_PAYLOAD["teams"][0], "springLeague": {"id": 114}}]}
        model = TeamsResponse.model_validate(payload)
        assert model.teams[0].id == 119


# ── Roster ─────────────────────────────────────────────────────────────────────

ROSTER_PAYLOAD = {
    "roster": [
        {
            "jerseyNumber": "17",
            "person": {"id": 660271, "name": "Shohei Ohtani"},
            "position": {"code": "DH", "name": "Designated Hitter"},
            "status": {"code": "A", "description": "Active"},
        }
    ],
    "team": {"id": 119, "name": "Los Angeles Dodgers"},
    "rosterType": "active",
}


class TestRosterResponse:
    def test_parses_roster(self):
        model = RosterResponse.model_validate(ROSTER_PAYLOAD)
        assert len(model.roster) == 1
        entry = model.roster[0]
        assert entry.jersey_number == "17"
        assert entry.person.id == 660271
        assert entry.position.code == "DH"

    def test_team_ref(self):
        model = RosterResponse.model_validate(ROSTER_PAYLOAD)
        assert model.team.id == 119
