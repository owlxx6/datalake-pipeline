"""Tests de l'AuditLogger : création, écriture, lecture des JobLog."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from datalake_lib.log.audit import AuditLogger
from datalake_lib.log.models import JobLog, JobMetrics


@pytest.fixture()
def audit(tmp_path: Path) -> AuditLogger:
    return AuditLogger(output_path=str(tmp_path))


# ── start ─────────────────────────────────────────────────────────────────


class TestStart:
    def test_start_creates_log_with_started_status(self, audit: AuditLogger) -> None:
        log = audit.start(
            job_name="JOB_X",
            table_name="silver_x",
            layer="silver",
            domain="credit",
        )
        assert log.status == "started"
        assert log.finished_at is None
        assert log.job_id != ""
        assert log.job_name == "JOB_X"

    def test_start_does_not_write_file(self, audit: AuditLogger, tmp_path: Path) -> None:
        audit.start("JOB_X", "silver_x", "silver", "credit")
        # Aucun fichier ne doit avoir été créé.
        assert list(tmp_path.rglob("*.json")) == []

    def test_start_with_dag_run(self, audit: AuditLogger) -> None:
        log = audit.start(
            "JOB_X",
            "silver_x",
            "silver",
            "credit",
            dag_id="dag_silver_credit",
            run_id="manual__2026-04-25",
        )
        assert log.dag_id == "dag_silver_credit"
        assert log.run_id == "manual__2026-04-25"


# ── success / fail / partial ──────────────────────────────────────────────


class TestSuccessFailPartial:
    def test_success_updates_status_and_writes(
        self, audit: AuditLogger, tmp_path: Path
    ) -> None:
        log = audit.start("JOB_X", "silver_x", "silver", "credit")
        time.sleep(0.01)
        metrics = JobMetrics(records_read=100, records_written=99, dq_score=99.0)

        finalized = audit.success(log, metrics)

        assert finalized.status == "success"
        assert finalized.finished_at is not None
        assert finalized.metrics.records_written == 99
        assert finalized.metrics.duration_seconds > 0

        # Fichier écrit
        target = tmp_path / "silver" / "silver_x" / f"{finalized.job_id}.json"
        assert target.exists()

    def test_fail_updates_status_and_error(
        self, audit: AuditLogger, tmp_path: Path
    ) -> None:
        log = audit.start("JOB_X", "silver_x", "silver", "credit")
        finalized = audit.fail(log, error="Boom: connection refused")

        assert finalized.status == "failed"
        assert finalized.error_message == "Boom: connection refused"
        assert finalized.finished_at is not None

        target = tmp_path / "silver" / "silver_x" / f"{finalized.job_id}.json"
        assert target.exists()

    def test_fail_without_metrics_uses_empty(
        self, audit: AuditLogger
    ) -> None:
        log = audit.start("JOB_X", "silver_x", "silver", "credit")
        finalized = audit.fail(log, error="oops")
        assert finalized.metrics.records_read == 0

    def test_partial_status(self, audit: AuditLogger) -> None:
        log = audit.start("JOB_X", "silver_x", "silver", "credit")
        metrics = JobMetrics(records_read=100, records_written=80, records_rejected=20)
        finalized = audit.partial(log, metrics, error="20% rejets DQ")

        assert finalized.status == "partial"
        assert finalized.error_message == "20% rejets DQ"


# ── _write : structure des fichiers ───────────────────────────────────────


class TestWriteStructure:
    def test_write_creates_json_file_at_expected_path(
        self, audit: AuditLogger, tmp_path: Path
    ) -> None:
        log = audit.start("JOB_X", "silver_credit_amort", "silver", "credit")
        finalized = audit.success(log, JobMetrics())

        expected = (
            tmp_path
            / "silver"
            / "silver_credit_amort"
            / f"{finalized.job_id}.json"
        )
        assert expected.exists()

        # Contenu JSON valide
        data = json.loads(expected.read_text(encoding="utf-8"))
        assert data["job_name"] == "JOB_X"
        assert data["status"] == "success"

    def test_write_creates_layer_subdirs(
        self, audit: AuditLogger, tmp_path: Path
    ) -> None:
        for layer in ("bronze", "silver", "gold"):
            log = audit.start(f"JOB_{layer.upper()}", f"{layer}_x", layer, "credit")
            audit.success(log, JobMetrics())

        assert (tmp_path / "bronze").is_dir()
        assert (tmp_path / "silver").is_dir()
        assert (tmp_path / "gold").is_dir()


# ── read_logs ─────────────────────────────────────────────────────────────


class TestReadLogs:
    def test_read_logs_returns_recent_first(
        self, audit: AuditLogger
    ) -> None:
        # 3 jobs successifs
        for i in range(3):
            log = audit.start(f"JOB_{i}", "silver_x", "silver", "credit")
            audit.success(log, JobMetrics(records_read=i))
            time.sleep(0.01)

        logs = audit.read_logs(table_name="silver_x", layer="silver")

        assert len(logs) == 3
        # Plus récent en premier
        assert logs[0].metrics.records_read == 2
        assert logs[2].metrics.records_read == 0

    def test_read_logs_respects_limit(self, audit: AuditLogger) -> None:
        for i in range(5):
            log = audit.start(f"JOB_{i}", "silver_x", "silver", "credit")
            audit.success(log, JobMetrics())

        logs = audit.read_logs(table_name="silver_x", layer="silver", limit=2)
        assert len(logs) == 2

    def test_read_logs_filter_by_table(self, audit: AuditLogger) -> None:
        log_a = audit.start("JOB_A", "silver_a", "silver", "credit")
        audit.success(log_a, JobMetrics())

        log_b = audit.start("JOB_B", "silver_b", "silver", "credit")
        audit.success(log_b, JobMetrics())

        logs = audit.read_logs(table_name="silver_a")
        assert len(logs) == 1
        assert logs[0].table_name == "silver_a"

    def test_read_logs_empty_dir_returns_empty(self, tmp_path: Path) -> None:
        audit = AuditLogger(output_path=str(tmp_path / "missing"))
        assert audit.read_logs() == []
