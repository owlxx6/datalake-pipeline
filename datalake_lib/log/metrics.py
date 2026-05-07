"""Collecteur de métriques d'un job — chronomètre + builder de JobMetrics."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from .models import JobMetrics

if TYPE_CHECKING:
    # Import lazy : permet d'utiliser `log` sans dépendre de `quality`.
    from datalake_lib.quality import DQReport


class MetricsCollector:
    """Collecte les métriques d'un job pendant son exécution.

    Pattern d'usage :

        collector = MetricsCollector()
        collector.set_records_read(10000)
        collector.set_input_path("/data/bronze/credit_amort/")
        # ... pendant l'exécution ...
        collector.set_records_written(9950)
        metrics = collector.build()

    Le chronomètre démarre à l'instanciation et se fige au `build()`.
    """

    def __init__(self) -> None:
        self._start_time: float = time.monotonic()
        self._metrics: JobMetrics = JobMetrics()

    # ── Setters ───────────────────────────────────────────────────────────

    def set_records_read(self, count: int) -> None:
        self._metrics.records_read = count

    def set_records_written(self, count: int) -> None:
        self._metrics.records_written = count

    def set_records_rejected(self, count: int) -> None:
        self._metrics.records_rejected = count

    def set_records_skipped(self, count: int) -> None:
        self._metrics.records_skipped = count

    def set_dq_score(self, score: float) -> None:
        self._metrics.dq_score = score

    def set_input_path(self, path: str) -> None:
        self._metrics.input_path = path

    def set_output_table(self, table: str) -> None:
        self._metrics.output_table = table

    def add_partition(self, key: str, value: str) -> None:
        self._metrics.partition_values[key] = value

    def add_custom_metric(self, key: str, value: float) -> None:
        self._metrics.custom_metrics[key] = value

    # ── Intégration avec le module quality ────────────────────────────────

    def from_dq_report(self, report: DQReport) -> None:
        """Remplit `records_rejected` et `dq_score` depuis un DQReport.

        Règle :
        - records_rejected = somme des `records_failed` des règles **bloquantes**.
        - dq_score = 100 × (1 - records_rejected / records_checked)
          basé sur le `records_checked` du premier résultat (toutes les règles
          opèrent sur le même DataFrame, donc valeur uniforme).

        Les règles `warning` ne diminuent pas le score.
        """
        blocking_rejected = sum(
            r.records_failed
            for r in report.results
            if r.severity == "blocking"
        )
        self._metrics.records_rejected = blocking_rejected

        total = report.results[0].records_checked if report.results else 0
        if total > 0:
            self._metrics.dq_score = 100.0 * (1 - blocking_rejected / total)
        else:
            self._metrics.dq_score = 100.0

    # ── Finalisation ──────────────────────────────────────────────────────

    def build(self) -> JobMetrics:
        """Construit les `JobMetrics` finaux avec la durée écoulée."""
        elapsed = time.monotonic() - self._start_time
        self._metrics.duration_seconds = elapsed
        return self._metrics
