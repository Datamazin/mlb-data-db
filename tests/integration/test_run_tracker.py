"""
Integration tests for RunTracker.

Uses a real in-memory DuckDB with the full schema applied — no mocks.
"""

from __future__ import annotations

import time

import duckdb
import pytest

from run_tracker.tracker import RunTracker


class TestRunLifecycle:
    def test_start_run_creates_row(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        run_id = tracker.start_run("test_job", season_year=2024)
        row = db.execute(
            "SELECT job_name, status, season_year FROM meta.pipeline_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()
        assert row == ("test_job", "running", 2024)

    def test_complete_run_updates_status(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        run_id = tracker.start_run("test_job")
        tracker.complete_run(run_id, records_extracted=100, records_loaded=95)
        row = db.execute(
            "SELECT status, records_extracted, records_loaded FROM meta.pipeline_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()
        assert row == ("success", 100, 95)

    def test_fail_run_updates_status(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        run_id = tracker.start_run("test_job")
        tracker.fail_run(run_id, "connection timeout")
        row = db.execute(
            "SELECT status, error_message FROM meta.pipeline_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()
        assert row[0] == "failed"
        assert "connection timeout" in row[1]

    def test_multiple_runs_independent(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        run1 = tracker.start_run("job_a", season_year=2022)
        run2 = tracker.start_run("job_b", season_year=2023)
        tracker.complete_run(run1, records_extracted=50)
        tracker.fail_run(run2, "error")

        statuses = {
            row[0]: row[1]
            for row in db.execute(
                "SELECT run_id, status FROM meta.pipeline_runs"
            ).fetchall()
        }
        assert statuses[run1] == "success"
        assert statuses[run2] == "failed"

    def test_last_successful_run_returns_latest(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        run1 = tracker.start_run("backfill")
        tracker.complete_run(run1, records_extracted=10)
        time.sleep(0.01)  # ensure distinct timestamps
        run2 = tracker.start_run("backfill")
        tracker.complete_run(run2, records_extracted=20)

        result = tracker.last_successful_run("backfill")
        assert result is not None
        assert result["run_id"] == run2
        assert result["records_extracted"] == 20

    def test_last_successful_run_none_when_no_runs(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        assert tracker.last_successful_run("nonexistent_job") is None

    def test_last_successful_run_ignores_failures(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        run1 = tracker.start_run("backfill")
        tracker.complete_run(run1, records_extracted=10)
        run2 = tracker.start_run("backfill")
        tracker.fail_run(run2, "error")

        result = tracker.last_successful_run("backfill")
        assert result["run_id"] == run1


class TestIdempotency:
    def test_is_extracted_false_before_recording(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        assert tracker.is_extracted("game_feed", "745525") is False

    def test_is_extracted_true_after_recording(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        tracker.record_checksum(
            "game_feed", "745525", '{"gamePk":745525}', "/v1/game/745525/feed/live"
        )
        assert tracker.is_extracted("game_feed", "745525") is True

    def test_is_extracted_scoped_to_entity_type(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        tracker.record_checksum("player", "660271", '{"id":660271}', "/v1/people/660271")
        assert tracker.is_extracted("game_feed", "660271") is False
        assert tracker.is_extracted("player", "660271") is True

    def test_record_checksum_upsert(self, db: duckdb.DuckDBPyConnection):
        """Re-recording with new JSON updates the hash (stat correction flow)."""
        tracker = RunTracker(db)
        tracker.record_checksum("game_feed", "745525", '{"v":1}', "/v1/game/745525/feed/live")
        tracker.record_checksum(
            "game_feed", "745525", '{"v":2}', "/v1/game/745525/feed/live",
            correction_source="mlb_official_correction_2024-10-01",
        )
        row = db.execute(
            "SELECT response_hash, correction_source FROM meta.entity_checksums "
            "WHERE entity_type='game_feed' AND entity_key='745525'"
        ).fetchone()
        import hashlib
        expected_hash = hashlib.sha256(b'{"v":2}').hexdigest()
        assert row[0] == expected_hash
        assert row[1] == "mlb_official_correction_2024-10-01"

    def test_filter_unextracted_returns_only_new(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        tracker.record_checksum("game_feed", "100", '{}', "/v1/game/100/feed/live")
        tracker.record_checksum("game_feed", "200", '{}', "/v1/game/200/feed/live")

        result = tracker.filter_unextracted("game_feed", ["100", "200", "300", "400"])
        assert set(result) == {"300", "400"}

    def test_filter_unextracted_empty_input(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        assert tracker.filter_unextracted("game_feed", []) == []

    def test_filter_unextracted_all_new(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        result = tracker.filter_unextracted("game_feed", ["1", "2", "3"])
        assert result == ["1", "2", "3"]

    def test_filter_unextracted_all_done(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        for pk in ["1", "2", "3"]:
            tracker.record_checksum("game_feed", pk, f'{{"pk":{pk}}}', f"/v1/game/{pk}/feed/live")
        assert tracker.filter_unextracted("game_feed", ["1", "2", "3"]) == []

    def test_record_checksums_bulk(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        entries = [
            {"entity_key": str(pk), "raw_json": f'{{"pk":{pk}}}', "source_url": f"/v1/game/{pk}/feed/live"}
            for pk in [100, 101, 102]
        ]
        tracker.record_checksums_bulk("game_feed", entries)
        assert tracker.extraction_count("game_feed") == 3

    def test_extraction_count(self, db: duckdb.DuckDBPyConnection):
        tracker = RunTracker(db)
        assert tracker.extraction_count("game_feed") == 0
        tracker.record_checksum("game_feed", "1", '{}', "/v1/game/1/feed/live")
        tracker.record_checksum("game_feed", "2", '{}', "/v1/game/2/feed/live")
        assert tracker.extraction_count("game_feed") == 2
        assert tracker.extraction_count("player") == 0


class TestBackfillHelpers:
    """Test the month-splitting utility used by the back-fill job."""

    def test_month_ranges_single_month(self):
        from extractor.backfill import _month_ranges
        from datetime import date
        ranges = _month_ranges(date(2024, 7, 1), date(2024, 7, 31))
        assert len(ranges) == 1
        assert ranges[0] == (date(2024, 7, 1), date(2024, 7, 31))

    def test_month_ranges_partial_start(self):
        from extractor.backfill import _month_ranges
        from datetime import date
        ranges = _month_ranges(date(2024, 3, 20), date(2024, 4, 30))
        assert ranges[0] == (date(2024, 3, 20), date(2024, 3, 31))
        assert ranges[1] == (date(2024, 4, 1), date(2024, 4, 30))

    def test_month_ranges_partial_end(self):
        from extractor.backfill import _month_ranges
        from datetime import date
        ranges = _month_ranges(date(2024, 9, 1), date(2024, 10, 15))
        assert ranges[-1] == (date(2024, 10, 1), date(2024, 10, 15))

    def test_month_ranges_year_boundary(self):
        from extractor.backfill import _month_ranges
        from datetime import date
        ranges = _month_ranges(date(2024, 11, 1), date(2025, 1, 31))
        months = [(r[0].year, r[0].month) for r in ranges]
        assert (2024, 11) in months
        assert (2024, 12) in months
        assert (2025, 1) in months

    def test_season_ranges_cover_all_seasons(self):
        from extractor.backfill import SEASON_RANGES, DEFAULT_SEASONS
        for season in DEFAULT_SEASONS:
            assert season in SEASON_RANGES
            start, end = SEASON_RANGES[season]
            assert start < end
            assert start.year == season
