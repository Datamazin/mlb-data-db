"""
Pydantic models for /v1/schedule response.

Only fields consumed by the pipeline are modelled. All other API fields
are silently ignored via extra='ignore'.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Ref(BaseModel):
    """Generic id/name reference used across the API."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int | None = None
    name: str | None = None


class ScheduleGameStatus(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    detailed_state: str = Field("Unknown", alias="detailedState")
    abstract_game_state: str = Field("Unknown", alias="abstractGameState")


class ScheduleTeamEntry(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    team: _Ref
    score: int | None = None
    is_winner: bool | None = Field(None, alias="isWinner")


class ScheduleGameTeams(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    home: ScheduleTeamEntry
    away: ScheduleTeamEntry


class ScheduleGame(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    game_pk: int = Field(alias="gamePk")
    game_type: str = Field(alias="gameType")
    season: str
    game_date: str = Field(alias="gameDate")        # ISO datetime string e.g. "2024-07-04T17:10:00Z"
    official_date: str = Field(alias="officialDate") # YYYY-MM-DD local date
    status: ScheduleGameStatus
    teams: ScheduleGameTeams
    venue: _Ref | None = None
    double_header: str = Field("N", alias="doubleHeader")
    games_in_series: int | None = Field(None, alias="gamesInSeries")
    series_game_number: int | None = Field(None, alias="seriesGameNumber")
    series_description: str | None = Field(None, alias="seriesDescription")


class ScheduleDate(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    date: str  # YYYY-MM-DD
    games: list[ScheduleGame] = []


class ScheduleResponse(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    total_items: int = Field(0, alias="totalItems")
    total_games: int = Field(0, alias="totalGames")
    dates: list[ScheduleDate] = []

    def all_game_pks(self) -> list[int]:
        return [g.game_pk for d in self.dates for g in d.games]
