"""
Integration tests for M7 — Distribution sync.

All tests run against real (in-memory or file-backed) DuckDB instances.
Azure SDK calls are patched at the boundary so no network or Azure credentials
are required. The tests verify:

  - DistributionSync.run() produces correct SyncResult structures
  - Per-club copy and SAS URL generation paths are exercised
  - Master upload failures abort the run and report errors
  - Individual club failures are captured without failing other clubs
  - run_distribution() records outcomes in meta.pipeline_runs via RunTracker
  - The distribution scheduler job is wired into build_scheduler()
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from distribution.sync import (
    ALL_CLUBS,
    BLOB_NAME,
    DEFAULT_SAS_EXPIRY_HOURS,
    MASTER_CONTAINER,
    DistributionSync,
    SyncResult,
    build_sync_from_env,
    run_distribution,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_syncer(
    tmp_path: Path,
    account_name: str = "testaccount",
    container_prefix: str = "mlb-club-",
) -> tuple[DistributionSync, MagicMock]:
    """
    Build a DistributionSync whose BlobServiceClient is fully mocked.

    Returns (syncer, mock_service_client).
    """
    syncer = DistributionSync.__new__(DistributionSync)
    syncer._account_name = account_name
    syncer._container_prefix = container_prefix
    syncer._db_path = tmp_path / "mlb.duckdb"
    syncer._sas_expiry_hours = DEFAULT_SAS_EXPIRY_HOURS
    syncer._account_url = f"https://{account_name}.blob.core.windows.net"

    mock_service = MagicMock()
    syncer._service_client = mock_service
    syncer._credential = MagicMock()

    # Default: containers exist, blob upload/copy succeed
    mock_container = MagicMock()
    mock_container.exists.return_value = True
    mock_service.get_container_client.return_value = mock_container

    mock_blob = MagicMock()
    mock_service.get_blob_client.return_value = mock_blob

    # user_delegation_key returns a MagicMock (value doesn't matter for unit tests)
    mock_service.get_user_delegation_key.return_value = MagicMock()

    return syncer, mock_service


def _write_fake_db(path: Path) -> None:
    """Write a minimal placeholder file so upload_master finds it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"duckdb_placeholder")


# ── DistributionSync.upload_master ────────────────────────────────────────────

class TestUploadMaster:
    def test_raises_if_db_missing(self, tmp_path):
        syncer, _ = _make_syncer(tmp_path)
        with pytest.raises(FileNotFoundError, match="Gold DB not found"):
            syncer.upload_master()

    def test_uploads_blob(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        _write_fake_db(syncer._db_path)

        syncer.upload_master()

        mock_service.get_container_client.assert_called_with(MASTER_CONTAINER)
        blob_client = mock_service.get_container_client.return_value.get_blob_client.return_value
        blob_client.upload_blob.assert_called_once()

    def test_creates_container_if_missing(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        _write_fake_db(syncer._db_path)
        mock_service.get_container_client.return_value.exists.return_value = False

        syncer.upload_master()

        mock_service.get_container_client.return_value.create_container.assert_called_once()

    def test_dry_run_skips_upload(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        _write_fake_db(syncer._db_path)

        syncer.upload_master(dry_run=True)

        mock_service.get_container_client.assert_not_called()


# ── DistributionSync.sync_club ────────────────────────────────────────────────

class TestSyncClub:
    def test_success_returns_sas_url(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        # Patch generate_blob_sas to return a predictable token
        with patch("distribution.sync.generate_blob_sas", return_value="sv=2024&sig=abc"):
            result = syncer.sync_club("lad")

        assert result.success is True
        assert result.club == "lad"
        assert result.container == "mlb-club-lad"
        assert "mlb-club-lad" in result.sas_url
        assert BLOB_NAME in result.sas_url
        assert result.error is None

    def test_creates_club_container_if_missing(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        mock_service.get_container_client.return_value.exists.return_value = False

        with patch("distribution.sync.generate_blob_sas", return_value="token"):
            syncer.sync_club("nyy")

        mock_service.get_container_client.return_value.create_container.assert_called_once()

    def test_blob_copy_called(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)

        with patch("distribution.sync.generate_blob_sas", return_value="token"):
            syncer.sync_club("bos")

        dest_blob = mock_service.get_blob_client.return_value
        dest_blob.start_copy_from_url.assert_called_once()
        # Source URL should reference the master container
        source_url = dest_blob.start_copy_from_url.call_args[0][0]
        assert MASTER_CONTAINER in source_url
        assert BLOB_NAME in source_url

    def test_error_is_captured_not_raised(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        mock_service.get_blob_client.return_value.start_copy_from_url.side_effect = (
            RuntimeError("network error")
        )

        result = syncer.sync_club("sfg")

        assert result.success is False
        assert "network error" in result.error

    def test_dry_run_returns_placeholder_sas(self, tmp_path):
        syncer, _ = _make_syncer(tmp_path)

        result = syncer.sync_club("lad", dry_run=True)

        assert result.success is True
        assert "dry-run-placeholder" in result.sas_url


# ── DistributionSync.run ──────────────────────────────────────────────────────

class TestDistributionSyncRun:
    def test_all_clubs_synced(self, tmp_path):
        syncer, _ = _make_syncer(tmp_path)
        _write_fake_db(syncer._db_path)

        with patch("distribution.sync.generate_blob_sas", return_value="tok"):
            result = syncer.run(clubs=["lad", "nyy", "bos"])

        assert result.master_uploaded is True
        assert result.clubs_synced == 3
        assert result.clubs_failed == 0
        assert result.success is True
        assert len(result.club_results) == 3

    def test_master_failure_aborts_clubs(self, tmp_path):
        syncer, _ = _make_syncer(tmp_path)
        # db file does not exist → upload_master raises FileNotFoundError

        result = syncer.run(clubs=["lad"])

        assert result.master_uploaded is False
        assert result.clubs_synced == 0
        assert len(result.club_results) == 0
        assert result.success is False
        assert any("Master upload failed" in e for e in result.errors)

    def test_partial_club_failures_recorded(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        _write_fake_db(syncer._db_path)

        call_count = {"n": 0}

        def flaky_copy(url):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("timeout")

        mock_service.get_blob_client.return_value.start_copy_from_url.side_effect = flaky_copy

        with patch("distribution.sync.generate_blob_sas", return_value="tok"):
            result = syncer.run(clubs=["lad", "nyy", "bos"])

        assert result.clubs_synced == 2
        assert result.clubs_failed == 1
        assert result.success is False

    def test_dry_run_does_not_call_azure(self, tmp_path):
        syncer, mock_service = _make_syncer(tmp_path)
        _write_fake_db(syncer._db_path)

        result = syncer.run(clubs=["lad", "nyy"], dry_run=True)

        assert result.master_uploaded is True
        assert result.clubs_synced == 2
        # No real Azure calls
        mock_service.get_container_client.assert_not_called()
        mock_service.get_blob_client.assert_not_called()

    def test_all_30_clubs_in_default_list(self, tmp_path):
        assert len(ALL_CLUBS) == 30
        assert "lad" in ALL_CLUBS
        assert "nyy" in ALL_CLUBS


# ── build_sync_from_env ────────────────────────────────────────────────────────

class TestBuildSyncFromEnv:
    def test_raises_without_account_name(self, monkeypatch):
        monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_NAME", raising=False)
        with pytest.raises(EnvironmentError, match="AZURE_STORAGE_ACCOUNT_NAME"):
            build_sync_from_env()

    def test_builds_with_env_vars(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "myaccount")
        monkeypatch.setenv("AZURE_STORAGE_CONTAINER_PREFIX", "test-club-")
        with patch("distribution.sync.DefaultAzureCredential"), \
             patch("distribution.sync.BlobServiceClient"):
            syncer = build_sync_from_env(db_path=tmp_path / "mlb.duckdb")
        assert syncer._account_name == "myaccount"
        assert syncer._container_prefix == "test-club-"

    def test_default_container_prefix(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "myaccount")
        monkeypatch.delenv("AZURE_STORAGE_CONTAINER_PREFIX", raising=False)
        with patch("distribution.sync.DefaultAzureCredential"), \
             patch("distribution.sync.BlobServiceClient"):
            syncer = build_sync_from_env(db_path=tmp_path / "mlb.duckdb")
        assert syncer._container_prefix == "mlb-club-"


# ── run_distribution (RunTracker integration) ─────────────────────────────────

class TestRunDistribution:
    def test_success_recorded_in_pipeline_runs(self, db_file_path, tmp_path, monkeypatch):
        db_path, conn = db_file_path

        fake_db = tmp_path / "mlb.duckdb"
        _write_fake_db(fake_db)

        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "testaccount")

        successful_result = SyncResult(
            master_uploaded=True,
            clubs_synced=30,
            clubs_failed=0,
        )

        with patch("distribution.sync.build_sync_from_env") as mock_factory:
            mock_syncer = MagicMock()
            mock_syncer.run.return_value = successful_result
            mock_factory.return_value = mock_syncer

            result = run_distribution(
                db_path=fake_db,
                duckdb_conn=conn,
            )

        assert result.success is True
        row = conn.execute(
            "SELECT status, records_loaded FROM meta.pipeline_runs "
            "WHERE job_name = 'distribution'"
        ).fetchone()
        assert row is not None
        assert row[0] == "success"
        assert row[1] == 30

    def test_failure_recorded_in_pipeline_runs(self, db_file_path, tmp_path, monkeypatch):
        db_path, conn = db_file_path

        fake_db = tmp_path / "mlb.duckdb"
        _write_fake_db(fake_db)

        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "testaccount")

        failed_result = SyncResult(
            master_uploaded=False,
            errors=["Master upload failed: connection refused"],
        )

        with patch("distribution.sync.build_sync_from_env") as mock_factory:
            mock_syncer = MagicMock()
            mock_syncer.run.return_value = failed_result
            mock_factory.return_value = mock_syncer

            result = run_distribution(
                db_path=fake_db,
                duckdb_conn=conn,
            )

        assert result.success is False
        row = conn.execute(
            "SELECT status FROM meta.pipeline_runs WHERE job_name = 'distribution'"
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"

    def test_missing_env_raises_and_records_failure(self, db_file_path, tmp_path, monkeypatch):
        db_path, conn = db_file_path
        fake_db = tmp_path / "mlb.duckdb"
        _write_fake_db(fake_db)
        monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_NAME", raising=False)

        with pytest.raises(EnvironmentError):
            run_distribution(db_path=fake_db, duckdb_conn=conn)

        row = conn.execute(
            "SELECT status FROM meta.pipeline_runs WHERE job_name = 'distribution'"
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"


# ── Scheduler wiring ──────────────────────────────────────────────────────────

class TestDistributionSchedulerWiring:
    def _pending_map(self, scheduler):
        return {job.id: job.trigger for job, *_ in scheduler._pending_jobs}

    def test_distribution_job_registered(self):
        from scheduler.jobs import build_scheduler
        scheduler = build_scheduler()
        pending = self._pending_map(scheduler)
        assert "distribution" in pending

    def test_distribution_fires_at_4_30(self):
        from apscheduler.triggers.cron import CronTrigger
        from scheduler.jobs import build_scheduler
        scheduler = build_scheduler()
        trigger = self._pending_map(scheduler)["distribution"]
        assert isinstance(trigger, CronTrigger)
        hour_field = next(f for f in trigger.fields if f.name == "hour")
        minute_field = next(f for f in trigger.fields if f.name == "minute")
        assert str(hour_field) == "4"
        assert str(minute_field) == "30"
