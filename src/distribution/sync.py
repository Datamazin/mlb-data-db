"""
M7 — Distribution: Azure Blob sync + per-club SAS token provisioning.

Responsibilities:
  1. Upload the gold mlb.duckdb artifact to a shared "master" container so it
     is available as a source for per-club copies.
  2. Copy (server-side) the artifact into each club's own container
     (named  <AZURE_STORAGE_CONTAINER_PREFIX><club_slug>).
  3. Generate a time-limited SAS token scoped to each club's container so
     club data scientists can download the file without needing Azure credentials.

Architecture:
  - Uses azure-identity DefaultAzureCredential for authentication (Managed
    Identity in production, az login / env vars in local dev).
  - SAS tokens are generated via a user-delegation key (no storage account key
    required), keeping credentials out of the codebase.
  - Each club upload is idempotent: the same blob name is overwritten each run.

Usage:
    uv run python -m src.distribution.sync
    uv run python -m src.distribution.sync --clubs lad nyy bos
    uv run python -m src.distribution.sync --dry-run
    uv run python -m src.distribution.sync --sas-expiry-hours 48
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobSasPermissions,
    BlobServiceClient,
    generate_blob_sas,
)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import duckdb

from run_tracker.tracker import RunTracker

log = structlog.get_logger(__name__)

# ── Defaults ───────────────────────────────────────────────────────────────────

GOLD_DB_PATH = Path("data/gold/mlb.duckdb")
BLOB_NAME = "mlb.duckdb"
MASTER_CONTAINER = "mlb-master"
DEFAULT_SAS_EXPIRY_HOURS = 24

# Canonical 3-letter slugs for all 30 MLB clubs.
ALL_CLUBS: list[str] = [
    "ari", "atl", "bal", "bos", "chc",
    "cws", "cin", "cle", "col", "det",
    "hou", "kan", "laa", "lad", "mia",
    "mil", "min", "nym", "nyy", "oak",
    "phi", "pit", "sdp", "sea", "sfg",
    "stl", "tbr", "tex", "tor", "wsh",
]


# ── Result dataclasses ─────────────────────────────────────────────────────────

@dataclass
class ClubSyncResult:
    club: str
    container: str
    sas_url: str | None = None
    success: bool = False
    error: str | None = None


@dataclass
class SyncResult:
    master_uploaded: bool = False
    clubs_synced: int = 0
    clubs_failed: int = 0
    club_results: list[ClubSyncResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.master_uploaded and self.clubs_failed == 0


# ── Core sync class ────────────────────────────────────────────────────────────

class DistributionSync:
    """
    Uploads mlb.duckdb to Azure Blob Storage and provisions per-club SAS URLs.

    Parameters
    ----------
    account_name:
        Azure Storage account name (from AZURE_STORAGE_ACCOUNT_NAME env var
        or passed directly).
    container_prefix:
        Prefix for per-club containers, e.g. "mlb-club-" → "mlb-club-lad".
        Defaults to AZURE_STORAGE_CONTAINER_PREFIX env var or "mlb-club-".
    db_path:
        Local path to the gold mlb.duckdb file.
    sas_expiry_hours:
        Lifetime of generated SAS tokens in hours (default: 24).
    """

    def __init__(
        self,
        account_name: str,
        container_prefix: str = "mlb-club-",
        db_path: Path = GOLD_DB_PATH,
        sas_expiry_hours: int = DEFAULT_SAS_EXPIRY_HOURS,
    ) -> None:
        self._account_name = account_name
        self._container_prefix = container_prefix
        self._db_path = db_path
        self._sas_expiry_hours = sas_expiry_hours

        self._credential = DefaultAzureCredential()
        self._account_url = f"https://{account_name}.blob.core.windows.net"
        self._service_client = BlobServiceClient(
            account_url=self._account_url,
            credential=self._credential,
        )

    # ── Master upload ──────────────────────────────────────────────────────────

    def upload_master(self, dry_run: bool = False) -> None:
        """
        Upload mlb.duckdb to the shared master container.

        Creates the container if it doesn't exist. Overwrites any existing blob
        with the same name (idempotent).
        """
        if not self._db_path.exists():
            raise FileNotFoundError(f"Gold DB not found: {self._db_path}")

        size_mb = self._db_path.stat().st_size / 1_048_576
        log.info(
            "distribution_upload_master_start",
            container=MASTER_CONTAINER,
            blob=BLOB_NAME,
            size_mb=round(size_mb, 1),
            dry_run=dry_run,
        )

        if dry_run:
            log.info("distribution_upload_master_dry_run", skipped=True)
            return

        container_client = self._service_client.get_container_client(MASTER_CONTAINER)
        if not container_client.exists():
            container_client.create_container()
            log.info("distribution_master_container_created", container=MASTER_CONTAINER)

        blob_client = container_client.get_blob_client(BLOB_NAME)
        with self._db_path.open("rb") as fh:
            blob_client.upload_blob(fh, overwrite=True)

        log.info(
            "distribution_upload_master_done",
            container=MASTER_CONTAINER,
            blob=BLOB_NAME,
            size_mb=round(size_mb, 1),
        )

    # ── Per-club copy + SAS ────────────────────────────────────────────────────

    def _container_name(self, club: str) -> str:
        return f"{self._container_prefix}{club}"

    def _ensure_container(self, club: str) -> None:
        """Create the club's container if it doesn't already exist."""
        container_client = self._service_client.get_container_client(
            self._container_name(club)
        )
        if not container_client.exists():
            container_client.create_container()
            log.info("distribution_club_container_created", club=club,
                     container=self._container_name(club))

    def _copy_blob(self, club: str) -> None:
        """
        Server-side copy from master container into the club's container.

        Uses start_copy_from_url which is instantaneous for blobs within the
        same storage account (server-side copy, no egress charges).
        """
        source_url = (
            f"{self._account_url}/{MASTER_CONTAINER}/{BLOB_NAME}"
        )
        dest_client = self._service_client.get_blob_client(
            container=self._container_name(club),
            blob=BLOB_NAME,
        )
        dest_client.start_copy_from_url(source_url)
        log.info("distribution_club_blob_copied", club=club)

    def _generate_sas_url(self, club: str) -> str:
        """
        Generate a user-delegation SAS URL scoped to the club's blob.

        The SAS grants read-only access (r) and expires in sas_expiry_hours.
        Uses a user-delegation key (no storage account key required).
        """
        expiry = datetime.now(UTC) + timedelta(hours=self._sas_expiry_hours)
        start = datetime.now(UTC) - timedelta(minutes=5)  # clock-skew buffer

        # Obtain a user-delegation key valid for the SAS window
        key = self._service_client.get_user_delegation_key(
            key_start_time=start,
            key_expiry_time=expiry,
        )

        sas_token = generate_blob_sas(
            account_name=self._account_name,
            container_name=self._container_name(club),
            blob_name=BLOB_NAME,
            user_delegation_key=key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry,
            start=start,
        )

        return (
            f"{self._account_url}/{self._container_name(club)}"
            f"/{BLOB_NAME}?{sas_token}"
        )

    def sync_club(self, club: str, dry_run: bool = False) -> ClubSyncResult:
        """
        Ensure club's container has the latest mlb.duckdb and return a SAS URL.

        Steps:
          1. Create the club container if it doesn't exist.
          2. Server-side copy from the master blob.
          3. Generate a read-only SAS URL valid for sas_expiry_hours.

        Returns a ClubSyncResult; never raises — errors are captured in the result.
        """
        container = self._container_name(club)
        result = ClubSyncResult(club=club, container=container)

        if dry_run:
            log.info("distribution_club_dry_run", club=club, container=container)
            result.success = True
            result.sas_url = f"https://dry-run-placeholder/{container}/{BLOB_NAME}?sas=..."
            return result

        try:
            self._ensure_container(club)
            self._copy_blob(club)
            result.sas_url = self._generate_sas_url(club)
            result.success = True
            log.info(
                "distribution_club_sync_done",
                club=club,
                container=container,
                sas_expiry_hours=self._sas_expiry_hours,
            )
        except Exception as exc:
            result.error = str(exc)
            log.error("distribution_club_sync_failed", club=club, error=str(exc))

        return result

    # ── Orchestrator ───────────────────────────────────────────────────────────

    def run(
        self,
        clubs: list[str] | None = None,
        dry_run: bool = False,
    ) -> SyncResult:
        """
        Full distribution run: upload master blob, then sync each club.

        Args:
            clubs:    Optional list of club slugs to sync (default: ALL_CLUBS).
            dry_run:  Log actions without uploading or generating SAS tokens.

        Returns:
            SyncResult with per-club outcomes and aggregate counts.
        """
        target_clubs = clubs if clubs is not None else ALL_CLUBS
        result = SyncResult()

        # 1 — Upload master
        try:
            self.upload_master(dry_run=dry_run)
            result.master_uploaded = True
        except Exception as exc:
            msg = f"Master upload failed: {exc}"
            log.error("distribution_master_upload_failed", error=str(exc))
            result.errors.append(msg)
            # Cannot copy to clubs without a master blob — abort early
            return result

        # 2 — Per-club copy + SAS
        for club in target_clubs:
            club_result = self.sync_club(club, dry_run=dry_run)
            result.club_results.append(club_result)
            if club_result.success:
                result.clubs_synced += 1
            else:
                result.clubs_failed += 1
                if club_result.error:
                    result.errors.append(f"{club}: {club_result.error}")

        log.info(
            "distribution_run_complete",
            master_uploaded=result.master_uploaded,
            clubs_synced=result.clubs_synced,
            clubs_failed=result.clubs_failed,
            dry_run=dry_run,
        )
        return result


# ── Convenience factory ────────────────────────────────────────────────────────

def build_sync_from_env(
    db_path: Path = GOLD_DB_PATH,
    sas_expiry_hours: int = DEFAULT_SAS_EXPIRY_HOURS,
) -> DistributionSync:
    """
    Build a DistributionSync using environment variables.

    Required env var:
        AZURE_STORAGE_ACCOUNT_NAME

    Optional env var:
        AZURE_STORAGE_CONTAINER_PREFIX  (default: "mlb-club-")
    """
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
    if not account_name:
        raise OSError(
            "AZURE_STORAGE_ACCOUNT_NAME environment variable is not set. "
            "Copy .env.example to .env and fill in your storage account name."
        )
    container_prefix = os.environ.get("AZURE_STORAGE_CONTAINER_PREFIX", "mlb-club-")
    return DistributionSync(
        account_name=account_name,
        container_prefix=container_prefix,
        db_path=db_path,
        sas_expiry_hours=sas_expiry_hours,
    )


# ── RunTracker-integrated entry point ─────────────────────────────────────────

def run_distribution(
    clubs: list[str] | None = None,
    db_path: Path = GOLD_DB_PATH,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    sas_expiry_hours: int = DEFAULT_SAS_EXPIRY_HOURS,
    dry_run: bool = False,
) -> SyncResult:
    """
    Run the full distribution job with RunTracker observability.

    Opens its own DuckDB connection if duckdb_conn is not provided.
    """
    own_conn = duckdb_conn is None
    conn = duckdb_conn or duckdb.connect(str(db_path))
    tracker = RunTracker(conn)
    run_id = tracker.start_run("distribution")

    try:
        syncer = build_sync_from_env(db_path=db_path, sas_expiry_hours=sas_expiry_hours)
        result = syncer.run(clubs=clubs, dry_run=dry_run)

        if result.success:
            tracker.complete_run(run_id, records_loaded=result.clubs_synced)
        else:
            tracker.fail_run(run_id, "; ".join(result.errors))

        return result

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        raise
    finally:
        if own_conn:
            conn.close()


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Distribute gold mlb.duckdb to per-club Azure Blob containers."
    )
    parser.add_argument(
        "--clubs", nargs="+", default=None, metavar="CLUB",
        help="Club slugs to sync (default: all 30 clubs). E.g. --clubs lad nyy bos",
    )
    parser.add_argument(
        "--db", type=Path, default=GOLD_DB_PATH,
        help=f"Path to gold DuckDB file (default: {GOLD_DB_PATH})",
    )
    parser.add_argument(
        "--sas-expiry-hours", type=int, default=DEFAULT_SAS_EXPIRY_HOURS,
        dest="sas_expiry_hours",
        help=f"SAS token lifetime in hours (default: {DEFAULT_SAS_EXPIRY_HOURS})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log actions without uploading to Azure or generating real SAS tokens",
    )
    return parser.parse_args()


def main() -> None:
    import logging

    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    args = _parse_args()

    result = run_distribution(
        clubs=args.clubs,
        db_path=args.db,
        sas_expiry_hours=args.sas_expiry_hours,
        dry_run=args.dry_run,
    )

    if result.success:
        log.info(
            "distribution_complete",
            clubs_synced=result.clubs_synced,
            dry_run=args.dry_run,
        )
    else:
        log.error("distribution_finished_with_errors", errors=result.errors)
        sys.exit(1)


if __name__ == "__main__":
    main()
