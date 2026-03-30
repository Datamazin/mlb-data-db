"""
API Exploration Script — validate Pydantic models against the live MLB Stats API.

Run this BEFORE building the historical back-fill to catch schema surprises early.
Each probe fetches a real endpoint, validates it against our Pydantic model, and
reports fields the API returned that we are currently ignoring (silent data loss).

Usage:
    python scripts/explore_api.py                         # uses defaults
    python scripts/explore_api.py --date 2024-09-01       # specific schedule date
    python scripts/explore_api.py --game-pk 745525        # specific game feed
    python scripts/explore_api.py --season 2023           # specific season

What to look for in the output:
  ✅  Model validated — key fields printed below
  ⚠   IGNORED KEYS — API returned fields our model doesn't capture
       (review these; they may be useful for future silver/gold columns)
  ❌  VALIDATION ERROR — model failed to parse; fix the Pydantic model before back-fill
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from extractor.client import MLBClient
from extractor.models import (
    GameFeedResponse,
    PersonResponse,
    ScheduleResponse,
    TeamsResponse,
)
from extractor.models.team import RosterResponse

# ── Well-known IDs for probing ─────────────────────────────────────────────────
PROBE_PLAYER_IDS = {
    "Shohei Ohtani (active, two-way)": 660271,
    "Mike Trout (active, OF)":         545361,
}
PROBE_SEASON = 2024
PROBE_TEAM_ID = 119  # Los Angeles Dodgers


# ── Utilities ──────────────────────────────────────────────────────────────────

PASS = "✅"
WARN = "⚠ "
FAIL = "❌"
SEP  = "─" * 70


def _flatten_keys(obj: Any, prefix: str = "") -> set[str]:
    """Recursively collect all dot-notation keys from a nested dict/list."""
    keys: set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            full = f"{prefix}.{k}" if prefix else k
            keys.add(full)
            keys |= _flatten_keys(v, full)
    elif isinstance(obj, list) and obj:
        keys |= _flatten_keys(obj[0], prefix)  # sample first element
    return keys


def _model_keys(model: Any, prefix: str = "") -> set[str]:
    """Collect dot-notation keys from a Pydantic model's dict output."""
    return _flatten_keys(model.model_dump(), prefix)


def _ignored_keys(raw: dict[str, Any], model: Any) -> list[str]:
    """
    Return top-level keys in the raw API response that our model did NOT capture.
    These are fields we are silently ignoring via extra='ignore'.
    """
    raw_top = set(raw.keys())
    model_aliases: set[str] = set()
    for field in model.model_fields.values():
        if field.alias:
            model_aliases.add(field.alias)
        model_aliases.add(field.title or "")
    # Simple check: any top-level key not represented in the model dict
    captured = set(model.model_dump().keys())
    # Map snake_case captured keys back — just show raw keys not in alias set
    # Since extra='ignore' we note any raw top-level key whose camelCase
    # isn't an alias in the model.
    all_aliases = {
        (f.alias or name)
        for name, f in model.model_fields.items()
    }
    return sorted(raw_top - all_aliases - {"copyright"})


def header(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def ok(msg: str) -> None:
    print(f"  {PASS}  {msg}")


def warn(msg: str) -> None:
    print(f"  {WARN}  {msg}")


def fail(msg: str) -> None:
    print(f"  {FAIL}  {msg}")


def kv(label: str, value: Any) -> None:
    print(f"       {label:<28} {value}")


# ── Probes ─────────────────────────────────────────────────────────────────────

async def probe_schedule(client: MLBClient, target_date: date) -> list[int]:
    """Probe /v1/schedule and return gamePks found."""
    header(f"PROBE: /v1/schedule  ({target_date})")
    params = {
        "sportId": 1,
        "season": target_date.year,
        "startDate": target_date.strftime("%Y-%m-%d"),
        "endDate": target_date.strftime("%Y-%m-%d"),
        "gameType": "R,F,D,L,W",
    }
    try:
        raw = await client.get("/v1/schedule", params=params)
        model = ScheduleResponse.model_validate(raw)
        ok(f"ScheduleResponse validated — {model.total_games} game(s)")
        kv("totalItems",  model.total_items)
        kv("dates found", len(model.dates))

        game_pks = model.all_game_pks()
        kv("gamePks", game_pks[:5])

        if model.dates:
            first_game = model.dates[0].games[0] if model.dates[0].games else None
            if first_game:
                kv("sample game_pk",     first_game.game_pk)
                kv("game_type",          first_game.game_type)
                kv("status",             first_game.status.detailed_state)
                kv("home team id",       first_game.teams.home.team.id)
                kv("away team id",       first_game.teams.away.team.id)
                kv("venue id",           first_game.venue.id if first_game.venue else "None")

        ignored = _ignored_keys(raw, model)
        if ignored:
            warn(f"IGNORED top-level keys: {ignored}")

        return game_pks

    except Exception as exc:
        fail(f"ScheduleResponse FAILED: {exc}")
        return []


async def probe_game_feed(client: MLBClient, game_pk: int) -> None:
    """Probe /v1/game/{gamePk}/feed/live."""
    header(f"PROBE: /v1/game/{game_pk}/feed/live")
    try:
        raw = await client.get(f"/v1.1/game/{game_pk}/feed/live")
        model = GameFeedResponse.model_validate(raw)
        ok(f"GameFeedResponse validated — gamePk={model.game_pk}")

        gd = model.game_data
        kv("game_type",        gd.game.type)
        kv("season",           gd.game.season)
        kv("official_date",    gd.datetime.official_date)
        kv("status",           gd.status.detailed_state)
        kv("home_team",        f"{gd.teams.home.name} (id={gd.teams.home.id})")
        kv("away_team",        f"{gd.teams.away.name} (id={gd.teams.away.id})")
        kv("venue",            f"{gd.venue.name} (id={gd.venue.id})" if gd.venue else "None")
        kv("home_score",       model.home_score)
        kv("away_score",       model.away_score)
        kv("innings_played",   model.innings_played)

        gi = gd.game_info
        kv("attendance",       gi.attendance if gi else "None")
        kv("duration_min",     gi.game_duration_minutes if gi else "None")

        ls = model.live_data.linescore
        if ls:
            kv("linescore innings", len(ls.innings))

        bs = model.live_data.boxscore
        if bs:
            kv("batting_order (home)", bs.teams.home.batting_order[:5])

        # Check for gameData fields we ignore
        raw_gd = raw.get("gameData", {})
        captured_gd_keys = {"game", "datetime", "status", "teams", "venue",
                            "gameInfo", "weather", "seriesDescription",
                            "seriesGameNumber", "gamesInSeries"}
        extra_gd = sorted(set(raw_gd.keys()) - captured_gd_keys - {"copyright", "players"})
        if extra_gd:
            warn(f"gameData keys not modelled: {extra_gd}")

        raw_ld = raw.get("liveData", {})
        extra_ld = sorted(set(raw_ld.keys()) - {"linescore", "boxscore", "plays", "decisions"})
        if extra_ld:
            warn(f"liveData keys not modelled: {extra_ld}")

        # Note that plays/allPlays is intentionally raw
        plays = raw_ld.get("plays", {})
        all_plays = plays.get("allPlays", [])
        ok(f"allPlays (raw, not modelled): {len(all_plays)} plays captured in raw_json")

    except Exception as exc:
        fail(f"GameFeedResponse FAILED: {exc}")
        import traceback
        traceback.print_exc()


async def probe_player(client: MLBClient, name: str, player_id: int) -> None:
    """Probe /v1/people/{personId}."""
    header(f"PROBE: /v1/people/{player_id}  ({name})")
    try:
        raw = await client.get(f"/v1/people/{player_id}")
        model = PersonResponse.model_validate(raw)
        person = model.person
        if not person:
            warn("No person returned")
            return

        ok(f"PersonResponse validated — {person.full_name}")
        kv("player_id",     person.id)
        kv("birth_date",    person.birth_date)
        kv("birth_country", person.birth_country)
        kv("height/weight", f"{person.height} / {person.weight} lbs")
        kv("bats/throws",   f"{person.bats} / {person.throws}")
        kv("position",      person.position_code)
        kv("mlb_debut",     person.mlb_debut_date)
        kv("active",        person.active)

        raw_person = raw.get("people", [{}])[0]
        expected = {"id", "fullName", "firstName", "lastName", "birthDate",
                    "birthCity", "birthCountry", "height", "weight",
                    "batSide", "pitchHand", "primaryPosition", "mlbDebutDate", "active"}
        extra = sorted(set(raw_person.keys()) - expected)
        if extra:
            warn(f"Person keys not modelled: {extra}")

    except Exception as exc:
        fail(f"PersonResponse FAILED: {exc}")


async def probe_teams(client: MLBClient, season_year: int) -> None:
    """Probe /v1/teams."""
    header(f"PROBE: /v1/teams  (season={season_year})")
    try:
        raw = await client.get("/v1/teams", params={"sportId": 1, "season": season_year})
        model = TeamsResponse.model_validate(raw)
        ok(f"TeamsResponse validated — {len(model.teams)} team(s)")

        for team in model.teams[:3]:
            kv(f"id={team.id}", f"{team.name} ({team.abbreviation}) "
                                f"league={team.league.id if team.league else '?'} "
                                f"div={team.division.id if team.division else '?'}")

        raw_team = raw.get("teams", [{}])[0]
        expected = {"id", "name", "abbreviation", "teamCode", "locationName",
                    "firstYearOfPlay", "active", "league", "division", "venue", "sport"}
        extra = sorted(set(raw_team.keys()) - expected)
        if extra:
            warn(f"Team keys not modelled: {extra}")

    except Exception as exc:
        fail(f"TeamsResponse FAILED: {exc}")


async def probe_roster(client: MLBClient, team_id: int, season_year: int) -> None:
    """Probe /v1/teams/{teamId}/roster."""
    header(f"PROBE: /v1/teams/{team_id}/roster  (season={season_year})")
    try:
        raw = await client.get(
            f"/v1/teams/{team_id}/roster",
            params={"rosterType": "active", "season": season_year},
        )
        model = RosterResponse.model_validate(raw)
        ok(f"RosterResponse validated — {len(model.roster)} player(s)")
        for entry in model.roster[:3]:
            kv(f"#{entry.jersey_number}", f"id={entry.person.id}  {entry.person.name}  "
                                           f"pos={entry.position.code if entry.position else '?'}")

    except Exception as exc:
        fail(f"RosterResponse FAILED: {exc}")


# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(game_pks: list[int], season: int) -> None:
    header("SUMMARY")
    print("""
  Review any ⚠  IGNORED KEYS lines above.
  These are API fields our models currently drop via extra='ignore'.
  Decide for each:
    → Add to the Pydantic model + Arrow schema if needed for silver/gold
    → Leave ignored if truly not needed downstream

  Review any ❌ VALIDATION ERROR lines.
  These must be fixed in the Pydantic models before running the back-fill.

  If all probes showed ✅, the models are consistent with the live API and
  it is safe to proceed to M3 (historical back-fill).
""")
    if game_pks:
        print(f"  Sample gamePks for further manual inspection:")
        for pk in game_pks[:5]:
            print(f"    https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live")


# ── Entry point ────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    target_date = args.date or (date.today() - timedelta(days=1))
    season = args.season or target_date.year

    print(f"\nMLB Stats API — Model Validation Probes")
    print(f"Target date : {target_date}")
    print(f"Season      : {season}")

    async with MLBClient() as client:
        # 1 — Schedule (bootstraps gamePks for the game feed probe)
        game_pks = await probe_schedule(client, target_date)

        # 2 — Game feed (use first gamePk from schedule, or --game-pk override)
        probe_pk = args.game_pk or (game_pks[0] if game_pks else None)
        if probe_pk:
            await probe_game_feed(client, probe_pk)
        else:
            warn("No gamePk available for game feed probe — try --date on a day with games")

        # 3 — Players
        for name, pid in PROBE_PLAYER_IDS.items():
            await probe_player(client, name, pid)

        # 4 — Teams
        await probe_teams(client, season)

        # 5 — Roster
        await probe_roster(client, PROBE_TEAM_ID, season)

    print_summary(game_pks, season)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Pydantic models against live MLB API")
    parser.add_argument(
        "--date", type=date.fromisoformat, default=None,
        metavar="YYYY-MM-DD",
        help="Schedule date to probe (default: yesterday)",
    )
    parser.add_argument(
        "--game-pk", type=int, default=None,
        help="Specific gamePk for game feed probe",
    )
    parser.add_argument(
        "--season", type=int, default=None,
        help="Season year for teams/roster probes (default: derived from --date)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
