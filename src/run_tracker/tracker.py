"""
Pipeline run tracker.

Provides two guarantees:
  1. Idempotency — before extracting an entity, check meta.entity_checksums.
     If the hash matches, skip re-extraction.
  2. Observability — every pipeline run is recorded in meta.pipeline_runs
     with status, timing, and record counts.

All state is persisted in the mlb.duckdb file so it survives process restarts
and is queryable alongside the pipeline data.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import date, datetime, timezone
from typing import Any

import duckdb
import structlog

log = structlog.get_logger(__name__)

PIPELINE_VERSION = "0.1.0"


class RunTracker:
    """
    Wraps a DuckDB connection and exposes run-state operations.

    Usage:
        tracker = RunTracker(conn)
        run_id = tracker.start_run("backfill", season_year=2024)
        try:
            ...
            tracker.complete_run(run_id, records_extracted=n, records_loaded=n)
        except Exception as exc:
            tracker.fail_run(run_id, str(exc))
            raise
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    # ── Run lifecycle ─────────────────────────────────────────────────────────

    def start_run(
        self,
        job_name: str,
        season_year: int | None = None,
        target_date: date | None = None,
    ) -> str:
        """Insert a 'running' row in meta.pipeline_runs. Returns the run_id."""
        run_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO meta.pipeline_runs
                (run_id, job_name, status, started_at, season_year, target_date, pipeline_version)
            VALUES (?, ?, 'running', ?, ?, ?, ?)
            """,
            [run_id, job_name, _utc_now(), season_year, target_date, PIPELINE_VERSION],
        )
        log.info("run_started", run_id=run_id, job=job_name, season=season_year)
        return run_id

    def complete_run(
        self,
        run_id: str,
        records_extracted: int = 0,
        records_loaded: int = 0,
    ) -> None:
        """Mark a run as succeeded."""
        self._conn.execute(
            """
            UPDATE meta.pipeline_runs
               SET status            = 'success',
                   completed_at      = ?,
                   records_extracted = ?,
                   records_loaded    = ?
             WHERE run_id = ?
            """,
            [_utc_now(), records_extracted, records_loaded, run_id],
        )
        log.info(
            "run_completed",
            run_id=run_id,
            extracted=records_extracted,
            loaded=records_loaded,
        )

    def fail_run(self, run_id: str, error_message: str) -> None:
        """Mark a run as failed."""
        self._conn.execute(
            """
            UPDATE meta.pipeline_runs
               SET status        = 'failed',
                   completed_at  = ?,
                   error_message = ?
             WHERE run_id = ?
            """,
            [_utc_now(), error_message[:2000], run_id],
        )
        log.error("run_failed", run_id=run_id, error=error_message[:200])

    # ── Idempotency ───────────────────────────────────────────────────────────

    def is_extracted(self, entity_type: str, entity_key: str) -> bool:
        """Return True if this entity has already been extracted."""
        row = self._conn.execute(
            """
            SELECT 1 FROM meta.entity_checksums
             WHERE entity_type = ? AND entity_key = ?
            """,
            [entity_type, entity_key],
        ).fetchone()
        return row is not None

    def filter_unextracted(
        self, entity_type: str, entity_keys: list[str]
    ) -> list[str]:
        """
        Return only the entity_keys that have NOT yet been extracted.
        Efficient bulk check — one query instead of N.
        """
        if not entity_keys:
            return []
        placeholders = ", ".join("?" for _ in entity_keys)
        already_done = {
            row[0]
            for row in self._conn.execute(
                f"""
                SELECT entity_key FROM meta.entity_checksums
                 WHERE entity_type = ?
                   AND entity_key IN ({placeholders})
                """,
                [entity_type, *entity_keys],
            ).fetchall()
        }
        return [k for k in entity_keys if k not in already_done]

    def record_checksum(
        self,
        entity_type: str,
        entity_key: str,
        raw_json: str,
        source_url: str,
        correction_source: str | None = None,
    ) -> None:
        """
        Record (or update) the checksum for an extracted entity.

        Uses INSERT OR REPLACE so a retroactive stat correction with a new
        raw_json will update the existing row and set correction_source.
        """
        response_hash = hashlib.sha256(raw_json.encode()).hexdigest()
        self._conn.execute(
            """
            INSERT OR REPLACE INTO meta.entity_checksums
                (entity_type, entity_key, response_hash, source_url,
                 extracted_at, transform_version, correction_source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                entity_type,
                entity_key,
                response_hash,
                source_url,
                _utc_now(),
                PIPELINE_VERSION,
                correction_source,
            ],
        )

    def record_checksums_bulk(
        self,
        entity_type: str,
        entries: list[dict[str, Any]],
    ) -> None:
        """
        Bulk-insert checksums for a list of extracted entities.

        Each entry must have: entity_key, raw_json, source_url.
        Optional: correction_source.
        """
        if not entries:
            return
        rows = [
            (
                entity_type,
                e["entity_key"],
                hashlib.sha256(e["raw_json"].encode()).hexdigest(),
                e["source_url"],
                _utc_now(),
                PIPELINE_VERSION,
                e.get("correction_source"),
            )
            for e in entries
        ]
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO meta.entity_checksums
                (entity_type, entity_key, response_hash, source_url,
                 extracted_at, transform_version, correction_source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        log.debug("checksums_recorded", entity_type=entity_type, count=len(rows))

    # ── Inspection ────────────────────────────────────────────────────────────

    def extraction_count(self, entity_type: str) -> int:
        """Return total number of extracted entities of a given type."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM meta.entity_checksums WHERE entity_type = ?",
            [entity_type],
        ).fetchone()
        return row[0] if row else 0

    def last_successful_run(self, job_name: str) -> dict[str, Any] | None:
        """Return the most recent successful run for a job, or None."""
        row = self._conn.execute(
            """
            SELECT run_id, started_at, completed_at, records_extracted, records_loaded
              FROM meta.pipeline_runs
             WHERE job_name = ? AND status = 'success'
             ORDER BY completed_at DESC
             LIMIT 1
            """,
            [job_name],
        ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "started_at": row[1],
            "completed_at": row[2],
            "records_extracted": row[3],
            "records_loaded": row[4],
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
