"""
Pydantic models for /v1/game/{gamePk}/feed/live response.

The live feed is the most complex endpoint in the API. We model:
  - gameData  → game header, datetime, status, teams, venue, weather, gameInfo
  - liveData.linescore → inning-by-inning runs/hits/errors
  - liveData.boxscore  → team-level batting/pitching aggregates + batting orders

Play-by-play (allPlays) is captured raw in the bronze Parquet but not modelled
here; it will be parsed in a later pipeline stage when at_bats/pitches tables
are populated.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


# ── Shared helpers ────────────────────────────────────────────────────────────

class _Ref(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    id: int | None = None
    name: str | None = None


# ── gameData sub-models ───────────────────────────────────────────────────────

class GameHeader(BaseModel):
    """gameData.game — top-level game metadata."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    pk: int
    type: str                          # R | S | F | D | L | W
    double_header: str = Field("N", alias="doubleHeader")
    game_number: int = Field(1, alias="gameNumber")
    season: str


class GameDatetime(BaseModel):
    """gameData.datetime — timing information."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    date_time: str | None = Field(None, alias="dateTime")    # UTC ISO-8601
    official_date: str | None = Field(None, alias="officialDate")  # YYYY-MM-DD
    day_night: str | None = Field(None, alias="dayNight")


class GameStatus(BaseModel):
    """gameData.status"""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    detailed_state: str = Field("Unknown", alias="detailedState")
    abstract_game_state: str = Field("Unknown", alias="abstractGameState")
    coded_game_state: str | None = Field(None, alias="codedGameState")


class GameTeamData(BaseModel):
    """gameData.teams.home / .away — team info embedded in the feed."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    name: str
    abbreviation: str | None = None
    league: _Ref | None = None
    division: _Ref | None = None
    venue: _Ref | None = None


class GameTeams(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    home: GameTeamData
    away: GameTeamData


class GameInfo(BaseModel):
    """gameData.gameInfo — attendance and duration."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    attendance: int | None = None
    game_duration_minutes: int | None = Field(None, alias="gameDurationMinutes")


class GameWeather(BaseModel):
    """gameData.weather — optional, not stored in silver but useful for future extensions."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    condition: str | None = None
    temp: str | None = None
    wind: str | None = None


class GameData(BaseModel):
    """gameData top-level object."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    game: GameHeader
    datetime: GameDatetime
    status: GameStatus
    teams: GameTeams
    venue: _Ref | None = None
    game_info: GameInfo | None = Field(None, alias="gameInfo")
    weather: GameWeather | None = None
    series_description: str | None = Field(None, alias="seriesDescription")
    series_game_number: int | None = Field(None, alias="seriesGameNumber")
    games_in_series: int | None = Field(None, alias="gamesInSeries")


# ── liveData.linescore sub-models ─────────────────────────────────────────────

class InningTeamLine(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    runs: int | None = None
    hits: int | None = None
    errors: int | None = None
    left_on_base: int | None = Field(None, alias="leftOnBase")


class InningLine(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    num: int
    home: InningTeamLine
    away: InningTeamLine


class LinescoreTotals(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    runs: int | None = None
    hits: int | None = None
    errors: int | None = None
    left_on_base: int | None = Field(None, alias="leftOnBase")


class LinescoreTeams(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    home: LinescoreTotals
    away: LinescoreTotals


class Linescore(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    current_inning: int | None = Field(None, alias="currentInning")
    innings: list[InningLine] = []
    teams: LinescoreTeams | None = None


# ── liveData.boxscore sub-models ──────────────────────────────────────────────

class BoxscoreTeamData(BaseModel):
    """Per-team section of the boxscore."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    team: _Ref
    batting_order: list[int] = Field([], alias="battingOrder")
    pitchers: list[int] = []
    batters: list[int] = []


class BoxscoreTeams(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    home: BoxscoreTeamData
    away: BoxscoreTeamData


class Boxscore(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    teams: BoxscoreTeams


# ── liveData top-level ────────────────────────────────────────────────────────

class LiveData(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    linescore: Linescore | None = None
    boxscore: Boxscore | None = None
    # plays (allPlays) intentionally omitted; captured as raw JSON in bronze


# ── Root response ─────────────────────────────────────────────────────────────

class GameFeedResponse(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    game_pk: int = Field(alias="gamePk")
    game_data: GameData = Field(alias="gameData")
    live_data: LiveData = Field(alias="liveData")

    # ── Convenience accessors ─────────────────────────────────────────────────

    @property
    def home_score(self) -> int | None:
        if self.live_data.linescore and self.live_data.linescore.teams:
            return self.live_data.linescore.teams.home.runs
        return None

    @property
    def away_score(self) -> int | None:
        if self.live_data.linescore and self.live_data.linescore.teams:
            return self.live_data.linescore.teams.away.runs
        return None

    @property
    def innings_played(self) -> int | None:
        if self.live_data.linescore:
            return self.live_data.linescore.current_inning
        return None
