"""
SLA definitions for the MLB data pipeline.

Each job has a documented SLA: the maximum wall-clock time after the scheduled
trigger by which it must complete successfully. If a job's last successful run
is older than (last_trigger + sla_window), the SLA is considered breached.

SLA table (all times ET):
  Job                    Schedule        Must complete by   Max age
  ────────────────────── ─────────────── ────────────────── ────────
  nightly_incremental    02:00 (Mar-Nov) 04:00 ET           26 h
  roster_sync            06:00 daily     08:00 ET           26 h
  standings_snapshot     03:00 (Apr-Oct) 05:00 ET           26 h
  distribution           04:30 daily     06:30 ET           26 h
  health_check           07:00 daily     07:30 ET           26 h

The 26-hour max age gives a 2-hour grace window beyond the next day's trigger,
accommodating transient failures that recover on first retry.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SLASpec:
    """
    Service Level Agreement specification for a single pipeline job.

    Attributes
    ----------
    job_name:
        Matches the value stored in meta.pipeline_runs.job_name.
    max_age_hours:
        A successful run must exist within this many hours of now.
        If no run within this window exists, the SLA is breached.
    description:
        Human-readable description shown in health reports and alerts.
    """
    job_name: str
    max_age_hours: int
    description: str


# Canonical SLA registry — consumed by HealthChecker.
SLA_REGISTRY: list[SLASpec] = [
    SLASpec(
        job_name="nightly_incremental",
        max_age_hours=26,
        description="Prior-day game extraction, silver transform, gold aggregation",
    ),
    SLASpec(
        job_name="roster_sync",
        max_age_hours=26,
        description="40-man roster and player biography refresh",
    ),
    SLASpec(
        job_name="standings_snapshot",
        max_age_hours=26,
        description="Daily standings snapshot (win/loss/GB/streak)",
    ),
    SLASpec(
        job_name="distribution",
        max_age_hours=26,
        description="Gold DuckDB upload to Azure Blob + per-club SAS provisioning",
    ),
]

# Data-freshness SLA: the most recently loaded game date must be within this
# many hours of now during the active season (March–November).
GAME_FRESHNESS_HOURS = 30

# Gold artifact size bounds (bytes).  Alerts fire outside these bounds.
GOLD_DB_MIN_BYTES = 1_000          # must exist and be non-trivial
GOLD_DB_MAX_BYTES = 10_000_000_000  # 10 GB upper guard
