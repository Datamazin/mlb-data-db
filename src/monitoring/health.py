"""
M9 — Pipeline health checker.

Queries meta.pipeline_runs and the gold DuckDB artifact to produce a
HealthReport that club data engineers and on-call staff can consume.

Checks performed:
  1. SLA compliance  — each scheduled job must have a successful run within
                        its max_age_hours window (see sla.py).
  2. Failed runs     — any job currently in 'failed' status is surfaced.
  3. Data freshness  — the latest game_date in silver.games must be recent
                        during the active season.
  4. Artifact health — gold mlb.duckdb must exist and be within expected
                        size bounds.
  5. Row-count sanity— core gold tables must be non-empty when data has been
                        loaded (warns, does not fail).

Usage:
    uv run python -m src.monitoring.health
    uv run python -m src.monitoring.health --db data/gold/mlb.duckdb
    uv run python -m src.monitoring.health --json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import structlog

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from monitoring.sla import (
    GAME_FRESHNESS_HOURS,
    GOLD_DB_MAX_BYTES,
    GOLD_DB_MIN_BYTES,
    SLA_REGISTRY,
    SLASpec,
)

log = structlog.get_logger(__name__)

DB_PATH = Path("data/gold/mlb.duckdb")


# ── Result types ───────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name: str
    ok: bool
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class HealthReport:
    generated_at: datetime
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return all(c.ok for c in self.checks)

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.ok]

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at.isoformat(),
            "healthy": self.healthy,
            "checks": [
                {
                    "name": c.name,
                    "ok": c.ok,
                    "message": c.message,
                    "details": c.details,
                }
                for c in self.checks
            ],
        }


# ── Health checker ─────────────────────────────────────────────────────────────

class HealthChecker:
    """
    Runs all health checks against a DuckDB connection and returns a HealthReport.

    Parameters
    ----------
    conn:
        Open DuckDB connection to mlb.duckdb.
    db_path:
        Path to the DuckDB file on disk (for artifact size check).
        Pass None to skip the artifact check (e.g. in tests using :memory:).
    now:
        Override the current UTC time (useful for deterministic tests).
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        db_path: Path | None = DB_PATH,
        now: datetime | None = None,
    ) -> None:
        self._conn = conn
        self._db_path = db_path
        self._now = now or datetime.now(timezone.utc)

    # ── Individual checks ──────────────────────────────────────────────────────

    def check_sla(self, spec: SLASpec) -> CheckResult:
        """
        Verify that a successful run exists within spec.max_age_hours of now.
        """
        cutoff = self._now - timedelta(hours=spec.max_age_hours)
        row = self._conn.execute(
            """
            SELECT run_id, completed_at, records_loaded
              FROM meta.pipeline_runs
             WHERE job_name = ?
               AND status   = 'success'
               AND completed_at >= ?
             ORDER BY completed_at DESC
             LIMIT 1
            """,
            [spec.job_name, cutoff],
        ).fetchone()

        if row is None:
            # Check if there's any run at all (to distinguish "never run" from "stale")
            any_row = self._conn.execute(
                "SELECT completed_at FROM meta.pipeline_runs "
                "WHERE job_name = ? AND status = 'success' "
                "ORDER BY completed_at DESC LIMIT 1",
                [spec.job_name],
            ).fetchone()
            if any_row is None:
                msg = f"No successful run found for '{spec.job_name}'"
                details: dict = {"last_success": None}
            else:
                age_h = (self._now - any_row[0]).total_seconds() / 3600
                msg = (
                    f"SLA breached: last success {age_h:.1f}h ago "
                    f"(max {spec.max_age_hours}h)"
                )
                details = {
                    "last_success": any_row[0].isoformat(),
                    "age_hours": round(age_h, 1),
                    "max_age_hours": spec.max_age_hours,
                }
            return CheckResult(
                name=f"sla:{spec.job_name}",
                ok=False,
                message=msg,
                details=details,
            )

        age_h = (self._now - row[1]).total_seconds() / 3600
        return CheckResult(
            name=f"sla:{spec.job_name}",
            ok=True,
            message=f"OK — last success {age_h:.1f}h ago",
            details={
                "run_id": row[0],
                "last_success": row[1].isoformat(),
                "age_hours": round(age_h, 1),
                "records_loaded": row[2],
            },
        )

    def check_failed_runs(self) -> CheckResult:
        """Surface any jobs currently stuck in 'failed' status."""
        rows = self._conn.execute(
            """
            SELECT job_name, run_id, started_at, error_message
              FROM meta.pipeline_runs
             WHERE status = 'failed'
             ORDER BY started_at DESC
             LIMIT 10
            """
        ).fetchall()

        if not rows:
            return CheckResult(
                name="failed_runs",
                ok=True,
                message="No failed runs",
            )

        failed = [
            {
                "job_name": r[0],
                "run_id": r[1],
                "started_at": r[2].isoformat() if r[2] else None,
                "error": (r[3] or "")[:200],
            }
            for r in rows
        ]
        return CheckResult(
            name="failed_runs",
            ok=False,
            message=f"{len(rows)} failed run(s) detected",
            details={"failed": failed},
        )

    def check_data_freshness(self) -> CheckResult:
        """
        Verify the latest game_date in silver.games is within GAME_FRESHNESS_HOURS.

        During the off-season (December–February) this check is skipped (returns OK)
        since no games are expected.
        """
        month = self._now.month
        off_season = month in (12, 1, 2)

        row = self._conn.execute(
            "SELECT MAX(game_date) FROM silver.games WHERE status = 'Final'"
        ).fetchone()
        latest_date = row[0] if row else None

        if latest_date is None:
            if off_season:
                return CheckResult(
                    name="data_freshness",
                    ok=True,
                    message="Off-season: no games expected",
                    details={"latest_game_date": None},
                )
            return CheckResult(
                name="data_freshness",
                ok=False,
                message="No Final games found in silver.games",
                details={"latest_game_date": None},
            )

        # Convert date → datetime for age calculation
        latest_dt = datetime(
            latest_date.year, latest_date.month, latest_date.day,
            tzinfo=timezone.utc,
        )
        age_h = (self._now - latest_dt).total_seconds() / 3600

        if off_season or age_h <= GAME_FRESHNESS_HOURS:
            return CheckResult(
                name="data_freshness",
                ok=True,
                message=f"Latest game {age_h:.0f}h ago ({latest_date})",
                details={"latest_game_date": str(latest_date), "age_hours": round(age_h, 1)},
            )

        return CheckResult(
            name="data_freshness",
            ok=False,
            message=(
                f"Stale data: latest game {age_h:.0f}h ago "
                f"(max {GAME_FRESHNESS_HOURS}h) — {latest_date}"
            ),
            details={"latest_game_date": str(latest_date), "age_hours": round(age_h, 1)},
        )

    def check_artifact(self) -> CheckResult:
        """
        Confirm gold mlb.duckdb exists on disk and is within expected size bounds.
        Skipped when db_path is None (in-memory mode).
        """
        if self._db_path is None:
            return CheckResult(
                name="artifact",
                ok=True,
                message="Artifact check skipped (in-memory DB)",
            )

        if not self._db_path.exists():
            return CheckResult(
                name="artifact",
                ok=False,
                message=f"Gold DB not found: {self._db_path}",
                details={"path": str(self._db_path)},
            )

        size = self._db_path.stat().st_size
        size_mb = round(size / 1_048_576, 1)

        if size < GOLD_DB_MIN_BYTES:
            return CheckResult(
                name="artifact",
                ok=False,
                message=f"Gold DB suspiciously small: {size_mb} MB",
                details={"path": str(self._db_path), "size_bytes": size},
            )

        if size > GOLD_DB_MAX_BYTES:
            return CheckResult(
                name="artifact",
                ok=False,
                message=f"Gold DB exceeds size limit: {size_mb} MB",
                details={"path": str(self._db_path), "size_bytes": size},
            )

        return CheckResult(
            name="artifact",
            ok=True,
            message=f"Gold DB healthy: {size_mb} MB",
            details={"path": str(self._db_path), "size_mb": size_mb},
        )

    def check_row_counts(self) -> CheckResult:
        """
        Warn if core gold views are empty when silver data exists.
        Returns ok=True with a warning message (not a hard failure) so that
        a freshly migrated DB without data loaded doesn't raise a false alarm.
        """
        silver_games = self._conn.execute(
            "SELECT COUNT(*) FROM silver.games"
        ).fetchone()[0]

        if silver_games == 0:
            return CheckResult(
                name="row_counts",
                ok=True,
                message="No silver data loaded yet — row count check skipped",
                details={"silver_games": 0},
            )

        counts = {}
        for table in ["gold.standings_snap", "gold.league_averages"]:
            row = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = row[0] if row else 0

        for view in ["gold.dim_player", "gold.dim_team", "gold.fact_game"]:
            try:
                row = self._conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()
                counts[view] = row[0] if row else 0
            except Exception:
                counts[view] = -1  # view may not exist yet

        empty = [t for t, n in counts.items() if n == 0]

        if empty:
            return CheckResult(
                name="row_counts",
                ok=True,  # warning only
                message=f"Warning: {len(empty)} gold table(s) empty despite silver data",
                details={"empty_tables": empty, "counts": counts},
            )

        return CheckResult(
            name="row_counts",
            ok=True,
            message="All core gold tables populated",
            details={"counts": counts},
        )

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(self) -> HealthReport:
        """Run all checks and return a HealthReport."""
        report = HealthReport(generated_at=self._now)

        for spec in SLA_REGISTRY:
            report.checks.append(self.check_sla(spec))

        report.checks.append(self.check_failed_runs())
        report.checks.append(self.check_data_freshness())
        report.checks.append(self.check_artifact())
        report.checks.append(self.check_row_counts())

        status = "healthy" if report.healthy else "unhealthy"
        log.info(
            "health_check_complete",
            status=status,
            total=len(report.checks),
            failures=len(report.failures),
        )

        for failure in report.failures:
            log.warning("health_check_failure", check=failure.name, message=failure.message)

        return report


# ── RunTracker-integrated entry point ─────────────────────────────────────────

def run_health_check(
    db_path: Path = DB_PATH,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    now: datetime | None = None,
) -> HealthReport:
    """
    Run the full health check suite with RunTracker observability.

    Records the outcome in meta.pipeline_runs. Opens its own DuckDB
    connection if duckdb_conn is not provided.
    """
    from run_tracker.tracker import RunTracker

    own_conn = duckdb_conn is None
    conn = duckdb_conn or duckdb.connect(str(db_path))

    tracker = RunTracker(conn)
    run_id = tracker.start_run("health_check")

    try:
        checker = HealthChecker(conn, db_path=None if duckdb_conn else db_path, now=now)
        report = checker.run()

        if report.healthy:
            tracker.complete_run(run_id, records_loaded=len(report.checks))
        else:
            failure_summary = "; ".join(f.message for f in report.failures)
            tracker.fail_run(run_id, failure_summary[:2000])

        return report

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        raise
    finally:
        if own_conn:
            conn.close()


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MLB pipeline health checker — verify SLAs, data freshness, artifact."
    )
    parser.add_argument(
        "--db", type=Path, default=DB_PATH,
        help=f"Path to gold DuckDB file (default: {DB_PATH})",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output health report as JSON",
    )
    return parser.parse_args()


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO)

    args = _parse_args()
    report = run_health_check(db_path=args.db)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(f"\nHealth check: {'HEALTHY' if report.healthy else 'UNHEALTHY'}")
        print(f"Generated at: {report.generated_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        for check in report.checks:
            icon = "OK " if check.ok else "FAIL"
            print(f"  [{icon}] {check.name}: {check.message}")
        print()

    if not report.healthy:
        sys.exit(1)


if __name__ == "__main__":
    main()
