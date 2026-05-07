"""Tests du MetricsCollector."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from datalake_lib.log.metrics import MetricsCollector


class TestMetricsCollectorBasics:
    def test_collector_build_calculates_duration(self) -> None:
        collector = MetricsCollector()
        time.sleep(0.05)  # 50 ms suffisent pour mesurer une durée non nulle
        metrics = collector.build()
        assert metrics.duration_seconds >= 0.05
        assert metrics.duration_seconds < 1.0  # garde-fou

    def test_collector_setters(self) -> None:
        collector = MetricsCollector()
        collector.set_records_read(100)
        collector.set_records_written(95)
        collector.set_records_rejected(5)
        collector.set_records_skipped(0)
        collector.set_dq_score(95.0)
        collector.set_input_path("/data/bronze/x/")
        collector.set_output_table("silver_x")

        metrics = collector.build()
        assert metrics.records_read == 100
        assert metrics.records_written == 95
        assert metrics.records_rejected == 5
        assert metrics.dq_score == 95.0
        assert metrics.input_path == "/data/bronze/x/"
        assert metrics.output_table == "silver_x"

    def test_collector_add_partition(self) -> None:
        collector = MetricsCollector()
        collector.add_partition("date_arrete", "2026-04-25")
        collector.add_partition("region", "MA")

        metrics = collector.build()
        assert metrics.partition_values == {
            "date_arrete": "2026-04-25",
            "region": "MA",
        }

    def test_collector_add_custom_metric(self) -> None:
        collector = MetricsCollector()
        collector.add_custom_metric("p99_latency_ms", 152.4)
        collector.add_custom_metric("spark_executors", 4)

        metrics = collector.build()
        assert metrics.custom_metrics == {
            "p99_latency_ms": 152.4,
            "spark_executors": 4,
        }


# ── Intégration avec quality/DQReport ─────────────────────────────────────


def _fake_dq_result(rule_id: str, severity: str, records_failed: int) -> SimpleNamespace:
    """Mock léger d'un DQResult — évite l'import de pyspark via quality."""
    return SimpleNamespace(
        rule_id=rule_id,
        severity=severity,
        records_failed=records_failed,
        records_checked=10000,
    )


def _fake_dq_report(results: list[SimpleNamespace]) -> SimpleNamespace:
    return SimpleNamespace(results=results)


class TestFromDQReport:
    def test_blocking_rejects_only(self) -> None:
        report = _fake_dq_report(
            [
                _fake_dq_result("R1", "blocking", 50),
                _fake_dq_result("R2", "warning", 200),  # ignoré
                _fake_dq_result("R3", "blocking", 10),
            ]
        )
        collector = MetricsCollector()
        collector.from_dq_report(report)

        metrics = collector.build()
        assert metrics.records_rejected == 60  # 50 + 10, pas 260
        # 60 / 10000 = 0.6% → score 99.4
        assert metrics.dq_score == pytest.approx(99.4)

    def test_no_results_gives_score_100(self) -> None:
        report = _fake_dq_report([])
        collector = MetricsCollector()
        collector.from_dq_report(report)

        metrics = collector.build()
        assert metrics.records_rejected == 0
        assert metrics.dq_score == 100.0

    def test_zero_records_checked_gives_score_100(self) -> None:
        result = SimpleNamespace(
            rule_id="R1",
            severity="blocking",
            records_failed=0,
            records_checked=0,
        )
        report = _fake_dq_report([result])

        collector = MetricsCollector()
        collector.from_dq_report(report)

        metrics = collector.build()
        assert metrics.dq_score == 100.0
