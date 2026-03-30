"""
Pydantic models for /v1/teams and /v1/teams/{teamId}/roster responses.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Ref(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    id: int | None = None
    name: str | None = None
    code: str | None = None  # used by position refs e.g. {"code": "DH", "name": "..."}


class Team(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    name: str
    abbreviation: str | None = None
    team_code: str | None = Field(None, alias="teamCode")
    location_name: str | None = Field(None, alias="locationName")  # city
    first_year_of_play: str | None = Field(None, alias="firstYearOfPlay")
    active: bool = True
    league: _Ref | None = None
    division: _Ref | None = None
    venue: _Ref | None = None
    sport: _Ref | None = None


class TeamsResponse(BaseModel):
    """Root response from /v1/teams."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    teams: list[Team] = []


class RosterEntry(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    jersey_number: str | None = Field(None, alias="jerseyNumber")
    person: _Ref
    position: _Ref | None = None
    status: _Ref | None = None


class RosterResponse(BaseModel):
    """Root response from /v1/teams/{teamId}/roster."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    roster: list[RosterEntry] = []
    team: _Ref | None = None
    roster_type: str | None = Field(None, alias="rosterType")
