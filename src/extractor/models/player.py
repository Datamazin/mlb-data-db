"""
Pydantic models for /v1/people/{personId} response.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _CodeDesc(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    code: str | None = None
    description: str | None = None


class Person(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int
    full_name: str = Field(alias="fullName")
    first_name: str | None = Field(None, alias="firstName")
    last_name: str | None = Field(None, alias="lastName")
    birth_date: str | None = Field(None, alias="birthDate")        # YYYY-MM-DD
    birth_city: str | None = Field(None, alias="birthCity")
    birth_country: str | None = Field(None, alias="birthCountry")
    height: str | None = None                                       # e.g. "6' 2\""
    weight: int | None = None                                       # pounds
    bat_side: _CodeDesc | None = Field(None, alias="batSide")      # L | R | S
    pitch_hand: _CodeDesc | None = Field(None, alias="pitchHand")  # L | R
    primary_position: _CodeDesc | None = Field(None, alias="primaryPosition")
    mlb_debut_date: str | None = Field(None, alias="mlbDebutDate") # YYYY-MM-DD
    active: bool = True
    current_team_id: int | None = None  # populated separately if needed

    @property
    def bats(self) -> str | None:
        return self.bat_side.code if self.bat_side else None

    @property
    def throws(self) -> str | None:
        return self.pitch_hand.code if self.pitch_hand else None

    @property
    def position_code(self) -> str | None:
        return self.primary_position.code if self.primary_position else None


class PersonResponse(BaseModel):
    """Root response from /v1/people/{personId}."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    people: list[Person] = []

    @property
    def person(self) -> Person | None:
        return self.people[0] if self.people else None
