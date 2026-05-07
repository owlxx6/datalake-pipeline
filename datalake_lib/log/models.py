"""Modèles pydantic v2 pour l'audit des jobs du pipeline."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

# Statut d'exécution d'un job.
JobStatus = Literal["started", "success", "failed", "partial"]
Layer = Literal["bronze", "silver", "gold"]


# ── Métriques d'un job ────────────────────────────────────────────────────


class JobMetrics(BaseModel):
    """Métriques collectées pendant l'exécution d'un job."""

    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0
    records_skipped: int = 0
    dq_score: float = Field(default=0.0, ge=0.0, le=100.0)
    duration_seconds: float = 0.0
    input_path: str = ""
    output_table: str = ""
    partition_values: dict[str, str] = Field(default_factory=dict)
    custom_metrics: dict[str, float] = Field(default_factory=dict)


# ── Log d'un job ──────────────────────────────────────────────────────────


class JobLog(BaseModel):
    """Trace d'audit d'une exécution de job (Bronze, Silver ou Gold)."""

    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    job_name: str
    table_name: str
    layer: Layer
    domain: str
    status: JobStatus
    started_at: datetime
    finished_at: datetime | None = None
    error_message: str = ""
    metrics: JobMetrics = Field(default_factory=JobMetrics)
    dag_id: str = ""
    run_id: str = ""

    @model_validator(mode="after")
    def fill_empty_job_id(self) -> JobLog:
        """Auto-génère un UUID si `job_id` est explicitement passé vide."""
        if not self.job_id:
            self.job_id = str(uuid.uuid4())
        return self

    @model_validator(mode="after")
    def check_finished_when_terminal(self) -> JobLog:
        """Un job non-`started` doit avoir un `finished_at`."""
        if self.status != "started" and self.finished_at is None:
            raise ValueError(
                f"finished_at obligatoire quand status='{self.status}' "
                f"(seul status='started' autorise finished_at=None)"
            )
        return self

    @model_validator(mode="after")
    def compute_duration(self) -> JobLog:
        """Met à jour `metrics.duration_seconds` quand `finished_at` est connu."""
        if self.finished_at is not None:
            delta = (self.finished_at - self.started_at).total_seconds()
            # Toujours recalculer pour rester cohérent si finished_at change.
            self.metrics.duration_seconds = max(0.0, delta)
        return self
