"""Modèles pydantic v2 pour les règles de qualité (DQ)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

# Types de règles DQ supportés.
RuleType = Literal[
    "not_null",
    "unique",
    "referential",
    "regex",
    "range",
    "in_set",
    "custom_sql",
]

Severity = Literal["blocking", "warning"]
RuleStatus = Literal["passed", "failed", "warning"]
OverallStatus = Literal["passed", "failed"]


# ── Règle individuelle ────────────────────────────────────────────────────


class DQRule(BaseModel):
    """Une règle de qualité déclarative.

    La forme exacte des champs requis dépend de `rule_type`. Le validateur
    `check_rule_shape` garantit la cohérence après initialisation.
    """

    rule_id: str
    rule_type: RuleType
    columns: list[str] = Field(default_factory=list)
    column: str = ""
    reference_table: str = ""
    reference_column: str = ""
    pattern: str = ""
    min_value: float | None = None
    max_value: float | None = None
    allowed_values: list[str] = Field(default_factory=list)
    sql_expr: str = ""
    severity: Severity = "blocking"
    description: str = ""

    @model_validator(mode="after")
    def check_rule_shape(self) -> DQRule:
        """Vérifie que les champs requis sont remplis selon `rule_type`."""
        rt = self.rule_type

        if rt == "not_null":
            if not self.columns:
                raise ValueError("rule_type=not_null requiert 'columns' non vide")

        elif rt == "unique":
            if not self.columns:
                raise ValueError("rule_type=unique requiert 'columns' non vide")

        elif rt == "referential":
            if not self.column:
                raise ValueError("rule_type=referential requiert 'column'")
            if not self.reference_table:
                raise ValueError("rule_type=referential requiert 'reference_table'")
            if not self.reference_column:
                raise ValueError("rule_type=referential requiert 'reference_column'")

        elif rt == "regex":
            if not self.column:
                raise ValueError("rule_type=regex requiert 'column'")
            if not self.pattern:
                raise ValueError("rule_type=regex requiert 'pattern'")

        elif rt == "range":
            if not self.column:
                raise ValueError("rule_type=range requiert 'column'")
            if self.min_value is None and self.max_value is None:
                raise ValueError(
                    "rule_type=range requiert au moins 'min_value' ou 'max_value'"
                )

        elif rt == "in_set":
            if not self.columns:
                raise ValueError("rule_type=in_set requiert 'columns' non vide")
            if not self.allowed_values:
                raise ValueError("rule_type=in_set requiert 'allowed_values' non vide")

        elif rt == "custom_sql":
            if not self.sql_expr:
                raise ValueError("rule_type=custom_sql requiert 'sql_expr'")

        return self


# ── Configuration complète d'une table ────────────────────────────────────


class DQConfig(BaseModel):
    """Configuration DQ d'une table — ensemble de règles à appliquer.

    `primary_key` permet au runner Spark de calculer `df_valid` via
    LEFT ANTI JOIN (plus performant que `exceptAll`). Si vide, le runner
    bascule sur un fallback basé sur la ligne complète.
    """

    table_name: str
    domain: str
    version: str = "1.0"
    primary_key: list[str] = Field(default_factory=list)
    rules: list[DQRule]

    @model_validator(mode="after")
    def check_rules_not_empty(self) -> DQConfig:
        if not self.rules:
            raise ValueError("DQConfig doit contenir au moins une règle")
        return self


# ── Résultat d'une règle ──────────────────────────────────────────────────


class DQResult(BaseModel):
    """Résultat d'exécution d'une règle DQ sur un DataFrame."""

    rule_id: str
    rule_type: RuleType
    status: RuleStatus
    records_checked: int
    records_failed: int
    failure_rate: float
    severity: Severity
    details: str = ""


# ── Rapport global ────────────────────────────────────────────────────────


class DQReport(BaseModel):
    """Rapport d'exécution complet d'une DQConfig sur une table."""

    table_name: str
    execution_date: datetime
    total_rules: int
    passed: int
    failed: int
    warnings: int
    results: list[DQResult]
    overall_status: OverallStatus

    @model_validator(mode="after")
    def check_overall_status_coherence(self) -> DQReport:
        """Le statut global doit refléter l'existence d'au moins un échec
        bloquant dans les résultats."""
        has_blocking_failure = any(
            r.status == "failed" and r.severity == "blocking"
            for r in self.results
        )
        expected = "failed" if has_blocking_failure else "passed"

        if self.overall_status != expected:
            raise ValueError(
                f"overall_status incohérent : attendu '{expected}', "
                f"reçu '{self.overall_status}' "
                f"(blocking_failure={has_blocking_failure})"
            )
        return self
