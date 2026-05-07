"""Tests des formatters console."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from datalake_lib.log.formatters import format_job_log, format_job_summary
from datalake_lib.log.models import JobLog, JobMetrics


def _make_log(
    job_name: str = "JOB_SILVER_CREDIT_AMORT",
    table_name: str = "silver_credit_amort",
    status: str = "success",
    records_read: int = 10_000,
    records_written: int = 9_950,
    records_rejected: int = 50,
    dq_score: float = 99.5,
    duration: float = 154.2,
    input_path: str = "/data/bronze/credit_amort/",
    output_table: str = "silver_credit_amort",
    partitions: dict | None = None,
) -> JobLog:
    start = datetime(2026, 4, 25, 6, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=duration)
    return JobLog(
        job_name=job_name,
        table_name=table_name,
        layer="silver",
        domain="credit",
        status=status,
        started_at=start,
        finished_at=end if status != "started" else None,
        metrics=JobMetrics(
            records_read=records_read,
            records_written=records_written,
            records_rejected=records_rejected,
            dq_score=dq_score,
            input_path=input_path,
            output_table=output_table,
            partition_values=partitions or {},
        ),
    )


class TestFormatJobLog:
    def test_format_job_log_contains_key_fields(self) -> None:
        log = _make_log(partitions={"date_arrete": "2026-04-25"})
        out = format_job_log(log)

        assert "JOB_SILVER_CREDIT_AMORT" in out
        assert "silver_credit_amort" in out
        assert "silver" in out
        assert "credit" in out
        assert "✅" in out
        assert "SUCCESS" in out
        assert "10,000" in out
        assert "9,950" in out
        assert "99.5" in out
        assert "date_arrete=2026-04-25" in out

    def test_format_job_log_failed_includes_error(self) -> None:
        log = JobLog(
            job_name="JOB_X",
            table_name="silver_x",
            layer="silver",
            domain="credit",
            status="failed",
            started_at=datetime(2026, 4, 25, 6, 0, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 4, 25, 6, 0, 5, tzinfo=timezone.utc),
            error_message="Connection refused",
        )
        out = format_job_log(log)
        assert "❌" in out
        assert "FAILED" in out
        assert "Connection refused" in out

    def test_format_job_log_started_no_finished(self) -> None:
        log = JobLog(
            job_name="JOB_X",
            table_name="silver_x",
            layer="silver",
            domain="credit",
            status="started",
            started_at=datetime(2026, 4, 25, 6, 0, 0, tzinfo=timezone.utc),
        )
        out = format_job_log(log)
        assert "STARTED" in out
        assert "Finished" not in out
        assert "Duration" not in out


class TestFormatJobSummary:
    def test_format_job_summary_tabular(self) -> None:
        logs = [
            _make_log(job_name="JOB_SILVER_CREDIT_AMORT"),
            _make_log(
                job_name="JOB_SILVER_IMPAYES_CDL",
                records_read=5200,
                records_written=5200,
                records_rejected=0,
                dq_score=100.0,
                duration=42.1,
            ),
            _make_log(
                job_name="JOB_SILVER_DEPASSEMENT",
                status="failed",
                records_read=0,
                records_written=0,
                records_rejected=0,
                dq_score=0.0,
                duration=2.3,
            ),
        ]
        out = format_job_summary(logs)

        # Bordures Unicode
        assert "┌" in out
        assert "┐" in out
        assert "└" in out
        assert "┘" in out
        assert "├" in out
        assert "┤" in out
        # Headers
        assert "Job" in out
        assert "Status" in out
        assert "Read" in out
        assert "Written" in out
        assert "Duration" in out
        # Données
        assert "JOB_SILVER_CREDIT_AMORT" in out
        assert "JOB_SILVER_IMPAYES_CDL" in out
        assert "JOB_SILVER_DEPASSEMENT" in out
        assert "10,000" in out
        assert "5,200" in out
        assert "✅" in out
        assert "❌" in out
        assert "OK" in out
        assert "FAIL" in out
        assert "154.2s" in out

    def test_format_job_summary_empty(self) -> None:
        out = format_job_summary([])
        assert "aucun job" in out
