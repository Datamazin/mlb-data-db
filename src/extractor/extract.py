"""
High-level extraction functions.

Each function corresponds to one logical extraction job:
  - extract_schedule()   → /v1/schedule for a date range
  - extract_game_feeds() → /v1/game/{gamePk}/feed/live for a list of gamePks
  - extract_teams()      → /v1/teams for a season
  - extract_players()    → /v1/sports/1/players for a season (full universe)

Concurrency is controlled by a semaphore (default 8) so we never exceed the
token-bucket limit even when called from asyncio.TaskGroup.

Typical call sequence for a nightly incremental run:
    1. extract_schedule(date, date)  → gamePks for the day
    2. extract_game_feeds(gamePks)   → full game data → bronze/games/
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from typing import Any

import structlog

from .client import MLBClient
from .models import GameFeedResponse, PersonResponse, ScheduleResponse, TeamsResponse
from .writer import (
    BronzeWriter,
    game_feed_to_record,
    player_to_record,
    team_to_record,
)

log = structlog.get_logger(__name__)

# Max concurrent API requests — stays within the 8 req/s token bucket
_DEFAULT_CONCURRENCY = 8


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


# ── Schedule ──────────────────────────────────────────────────────────────────

async def extract_schedule(
    client: MLBClient,
    writer: BronzeWriter,
    start_date: date,
    end_date: date,
    season_year: int,
    game_types: str = "R,F,D,L,W,S",
) -> list[int]:
    """
    Fetch the MLB schedule for a date range and write to bronze.

    Returns a list of gamePks found.
    """
    params: dict[str, Any] = {
        "sportId": 1,
        "season": season_year,
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "gameType": game_types,
    }
    source_url = f"/v1/schedule?{_params_str(params)}"

    log.info("extract_schedule_start", start=str(start_date), end=str(end_date))
    raw = await client.get("/v1/schedule", params=params)
    schedule = ScheduleResponse.model_validate(raw)

    all_pks: list[int] = []
    for sched_date in schedule.dates:
        parsed_date = date.fromisoformat(sched_date.date)
        records = []
        for game in sched_date.games:
            all_pks.append(game.game_pk)
            records.append({
                "game_pk":               game.game_pk,
                "season_year":           season_year,
                "game_date":             game.official_date,
                "game_datetime":         game.game_date,
                "game_type":             game.game_type,
                "status_detailed_state": game.status.detailed_state,
                "home_team_id":          game.teams.home.team.id,
                "away_team_id":          game.teams.away.team.id,
                "home_score":            game.teams.home.score,
                "away_score":            game.teams.away.score,
                "venue_id":              game.venue.id if game.venue else None,
                "double_header":         game.double_header,
                "series_description":    game.series_description,
                "series_game_num":       game.series_game_number,
                "extracted_at":          _utc_now(),
                "source_url":            source_url,
            })
        if records:
            writer.write_schedule(records, for_date=parsed_date)

    log.info("extract_schedule_done", game_pks=len(all_pks))
    return all_pks


# ── Game feeds ────────────────────────────────────────────────────────────────

async def extract_game_feeds(
    client: MLBClient,
    writer: BronzeWriter,
    game_pks: list[int],
    concurrency: int = _DEFAULT_CONCURRENCY,
) -> list[int]:
    """
    Fetch /v1/game/{gamePk}/feed/live for each gamePk and write to bronze.

    Returns gamePks that were successfully extracted.
    """
    sem = asyncio.Semaphore(concurrency)
    results: list[int] = []
    errors: list[tuple[int, Exception]] = []

    async def _fetch_one(game_pk: int) -> tuple[int, dict[str, Any] | None]:
        path = f"/v1.1/game/{game_pk}/feed/live"
        async with sem:
            try:
                raw = await client.get(path)
                return game_pk, raw
            except Exception as exc:
                log.warning("extract_game_feed_error", game_pk=game_pk, error=str(exc))
                errors.append((game_pk, exc))
                return game_pk, None

    # Fan out all requests concurrently (semaphore limits active count)
    tasks = [asyncio.create_task(_fetch_one(pk)) for pk in game_pks]
    responses: list[tuple[int, dict[str, Any] | None]] = await asyncio.gather(*tasks)

    # Group completed games by date for partitioned Parquet writes
    by_date: dict[date, list[dict[str, Any]]] = {}
    for game_pk, raw in responses:
        if raw is None:
            continue
        try:
            feed = GameFeedResponse.model_validate(raw)
            source_url = f"/v1.1/game/{game_pk}/feed/live"
            record = game_feed_to_record(feed, raw, source_url)
            game_date = date.fromisoformat(record["game_date"]) if record["game_date"] else date.today()
            by_date.setdefault(game_date, []).append(record)
            results.append(game_pk)
        except Exception as exc:
            log.error("extract_game_feed_parse_error", game_pk=game_pk, error=str(exc))

    for game_date, records in by_date.items():
        writer.write_games(records, for_date=game_date)

    log.info(
        "extract_game_feeds_done",
        requested=len(game_pks),
        succeeded=len(results),
        failed=len(errors),
    )
    return results


# ── Teams ─────────────────────────────────────────────────────────────────────

async def extract_teams(
    client: MLBClient,
    writer: BronzeWriter,
    season_year: int,
) -> list[int]:
    """
    Fetch all MLB teams for a season and write to bronze.

    Returns team IDs extracted.
    """
    params: dict[str, Any] = {"sportId": 1, "season": season_year}
    source_url = f"/v1/teams?{_params_str(params)}"

    raw = await client.get("/v1/teams", params=params)
    teams_resp = TeamsResponse.model_validate(raw)

    raw_teams: list[dict[str, Any]] = raw.get("teams", [])
    records = [
        team_to_record(team, season_year, raw_teams[i], source_url)
        for i, team in enumerate(teams_resp.teams)
    ]
    writer.write_teams(records, season_year=season_year)

    log.info("extract_teams_done", season_year=season_year, count=len(records))
    return [t.id for t in teams_resp.teams]


# ── Players ───────────────────────────────────────────────────────────────────

async def extract_players(
    client: MLBClient,
    writer: BronzeWriter,
    season_year: int,
    concurrency: int = _DEFAULT_CONCURRENCY,
) -> list[int]:
    """
    Fetch full player universe for a season via /v1/sports/1/players,
    then hydrate each player with biographical detail.

    Returns player IDs extracted.
    """
    # Step 1 — get the full player ID list for the season
    params: dict[str, Any] = {"season": season_year}
    raw_roster = await client.get("/v1/sports/1/players", params=params)
    player_ids = [p["id"] for p in raw_roster.get("people", [])]
    log.info("extract_players_universe", season_year=season_year, count=len(player_ids))

    # Step 2 — hydrate each player individually
    sem = asyncio.Semaphore(concurrency)
    records: list[dict[str, Any]] = []

    async def _fetch_player(pid: int) -> None:
        path = f"/v1/people/{pid}"
        async with sem:
            try:
                raw = await client.get(path)
                resp = PersonResponse.model_validate(raw)
                person = resp.person
                if person:
                    raw_person = raw.get("people", [{}])[0]
                    records.append(player_to_record(person, raw_person, path))
            except Exception as exc:
                log.warning("extract_player_error", player_id=pid, error=str(exc))

    await asyncio.gather(*[asyncio.create_task(_fetch_player(pid)) for pid in player_ids])

    writer.write_players(records, season_year=season_year)
    log.info("extract_players_done", season_year=season_year, written=len(records))
    return [r["player_id"] for r in records]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _params_str(params: dict[str, Any]) -> str:
    return "&".join(f"{k}={v}" for k, v in params.items())
