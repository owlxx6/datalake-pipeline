"""Tests des modèles JobLog / JobMetrics."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from datalake_lib.log.models import JobLog, JobMetrics


def _started_at(offset_seconds: int = 0) -> datetime:
    return datetime(2026, 4, 25, 6, 0, 0, tzinfo=timezone.utc) + timedelta(
        seconds=offset_seconds
    )


class TestJobLogBasics:
    def test_job_log_auto_uuid(self) -> None:
        """job_id est auto-généré si non passé."""
        log = JobLog(
            job_name="JOB_X",
            table_name="silver_x",
            layer="silver",
            domain="credit",
            status="started",
            started_at=_started_at(),
        )
        assert log.job_id != ""
        assert len(log.job_id) == 36  # UUID4 standard

    def test_job_log_uuid_unique_across_instances(self) -> None:
        log_a = JobLog(
            job_name="A",
            table_name="silver_a",
            layer="silver",
            domain="credit",
            status="started",
            started_at=_started_at(),
        )
        log_b = JobLog(
            job_name="B",
            table_name="silver_b",
            layer="silver",
            domain="credit",
            status="started",
            started_at=_started_at(),
        )
        assert log_a.job_id != log_b.job_id

    def test_job_log_explicit_empty_id_is_replaced(self) -> None:
        log = JobLog(
            job_id="",
            job_name="X",
            table_name="silver_x",
            layer="silver",
            domain="credit",
            status="started",
            started_at=_started_at(),
        )
        assert log.job_id != ""

    def test_job_log_finished_required_when_not_started(self) -> None:
        with pytest.raises(ValidationError, match="finished_at obligatoire"):
            JobLog(
                job_name="X",
                table_name="silver_x",
                layer="silver",
                domain="credit",
                status="success",
                started_at=_started_at(),
            )

    def test_job_log_started_without_finished_ok(self) -> None:
        log = JobLog(
            job_name="X",
            table_name="silver_x",
            layer="silver",
            domain="credit",
            status="started",
            started_at=_started_at(),
        )
        assert log.finished_at is None
        assert log.metrics.duration_seconds == 0.0

    def test_job_log_duration_auto_calculated(self) -> None:
        start = _started_at()
        end = start + timedelta(seconds=42.5)
        log = JobLog(
            job_name="X",
            table_name="silver_x",
            layer="silver",
            domain="credit",
            status="success",
            started_at=start,
            finished_at=end,
        )
        assert log.metrics.duration_seconds == pytest.approx(42.5)

    def test_job_log_invalid_layer_raises(self) -> None:
        with pytest.raises(ValidationError):
            JobLog(
                job_name="X",
                table_name="silver_x",
                layer="silvr",  # typo
                domain="credit",
                status="started",
                started_at=_started_at(),
            )


class TestJobMetrics:
    def test_dq_score_bounds_lower(self) -> None:
        with pytest.raises(ValidationError):
            JobMetrics(dq_score=-1.0)

    def test_dq_score_bounds_upper(self) -> None:
        with pytest.raises(ValidationError):
            JobMetrics(dq_score=100.5)

    def test_dq_score_in_range(self) -> None:
        m = JobMetrics(dq_score=99.5)
        assert m.dq_score == 99.5

    def test_default_dicts_are_independent(self) -> None:
        """Pas de partage de dict entre instances (Field default_factory)."""
        a = JobMetrics()
        b = JobMetrics()
        a.partition_values["date_arrete"] = "2026-04-25"
        assert b.partition_values == {}


class TestJobLogValidComplete:
    def test_job_log_valid_complete(self) -> None:
        start = _started_at()
        end = start + timedelta(seconds=154.2)
        log = JobLog(
            job_name="JOB_SILVER_CREDIT_AMORT",
            table_name="silver_credit_amort",
            layer="silver",
            domain="credit",
            status="success",
            started_at=start,
            finished_at=end,
            metrics=JobMetrics(
                records_read=10000,
                records_written=9950,
                records_rejected=50,
                dq_score=99.5,
                input_path="/data/bronze/credit_amort/",
                output_table="silver_credit_amort",
                partition_values={"date_arrete": "2026-04-25"},
            ),
            dag_id="dag_silver_credit",
            run_id="manual__2026-04-25T06:00:00",
        )
        assert log.status == "success"
        assert log.metrics.duration_seconds == pytest.approx(154.2)
        assert log.metrics.partition_values == {"date_arrete": "2026-04-25"}
