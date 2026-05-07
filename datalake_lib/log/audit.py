"""AuditLogger — orchestre la création et l'écriture des JobLog."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .models import JobLog, JobMetrics, Layer

logger = logging.getLogger(__name__)


class AuditLogger:
    """Écrit les traces d'audit des jobs au format JSON.

    Layout disque :
        {output_path}/{layer}/{table_name}/{job_id}.json

    Note POC : pas de partitionnement par date — adéquat pour le périmètre
    Crédit-Garantie. Ajout possible en prod.
    """

    def __init__(self, output_path: str = "/data/audit/jobs") -> None:
        self.output_path = Path(output_path)

    # ── Cycle de vie d'un job ─────────────────────────────────────────────

    def start(
        self,
        job_name: str,
        table_name: str,
        layer: Layer,
        domain: str,
        dag_id: str = "",
        run_id: str = "",
    ) -> JobLog:
        """Crée un JobLog en statut `started`. Pas d'écriture disque.

        Le JobLog retourné doit être passé à `success` / `fail` / `partial`
        pour finaliser et persister.
        """
        return JobLog(
            job_name=job_name,
            table_name=table_name,
            layer=layer,
            domain=domain,
            status="started",
            started_at=datetime.now(timezone.utc),
            dag_id=dag_id,
            run_id=run_id,
        )

    def success(self, job_log: JobLog, metrics: JobMetrics) -> JobLog:
        """Finalise un job en succès et écrit le JSON."""
        finalized = self._finalize(
            job_log,
            status="success",
            metrics=metrics,
            error_message="",
        )
        self._write(finalized)
        return finalized

    def fail(
        self,
        job_log: JobLog,
        error: str,
        metrics: JobMetrics | None = None,
    ) -> JobLog:
        """Finalise un job en échec et écrit le JSON."""
        finalized = self._finalize(
            job_log,
            status="failed",
            metrics=metrics or JobMetrics(),
            error_message=error,
        )
        self._write(finalized)
        return finalized

    def partial(
        self,
        job_log: JobLog,
        metrics: JobMetrics,
        error: str = "",
    ) -> JobLog:
        """Finalise un job en succès partiel (ex : DQ avec rejets non bloquants)."""
        finalized = self._finalize(
            job_log,
            status="partial",
            metrics=metrics,
            error_message=error,
        )
        self._write(finalized)
        return finalized

    # ── I/O ───────────────────────────────────────────────────────────────

    def _write(self, job_log: JobLog) -> None:
        """Sérialise un JobLog dans `{output_path}/{layer}/{table_name}/{job_id}.json`."""
        target_dir = self.output_path / job_log.layer / job_log.table_name
        target_dir.mkdir(parents=True, exist_ok=True)

        target_file = target_dir / f"{job_log.job_id}.json"
        target_file.write_text(
            job_log.model_dump_json(indent=2),
            encoding="utf-8",
        )
        logger.info("JobLog écrit dans '%s'", target_file)

    def read_logs(
        self,
        table_name: str = "",
        layer: str = "",
        limit: int = 10,
    ) -> list[JobLog]:
        """Lit les `limit` logs les plus récents (tri par `started_at` DESC).

        Args:
            table_name: Filtre sur le nom de table (vide = toutes).
            layer: Filtre sur la couche bronze/silver/gold (vide = toutes).
            limit: Nombre maximum de logs à retourner.

        Returns:
            Liste de JobLog ordonnés du plus récent au plus ancien.
        """
        if not self.output_path.exists():
            return []

        # Construction du glob selon les filtres.
        layer_glob = layer if layer else "*"
        table_glob = table_name if table_name else "*"
        pattern = f"{layer_glob}/{table_glob}/*.json"

        logs: list[JobLog] = []
        for json_path in self.output_path.glob(pattern):
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
                logs.append(JobLog(**data))
            except (json.JSONDecodeError, ValueError) as exc:
                logger.warning(
                    "Log corrompu ignoré : '%s' (%s)", json_path, exc
                )

        logs.sort(key=lambda log: log.started_at, reverse=True)
        return logs[:limit]

    # ── Helper interne ────────────────────────────────────────────────────

    @staticmethod
    def _finalize(
        job_log: JobLog,
        *,
        status: str,
        metrics: JobMetrics,
        error_message: str,
    ) -> JobLog:
        """Reconstruit un JobLog avec finished_at + status + metrics.

        On passe par `model_dump` + `model_validate` pour re-déclencher les
        validators (notamment `compute_duration` qui met à jour la durée).
        """
        data = job_log.model_dump()
        data.update(
            {
                "status": status,
                "finished_at": datetime.now(timezone.utc),
                "metrics": metrics.model_dump(),
                "error_message": error_message,
            }
        )
        return JobLog.model_validate(data)
