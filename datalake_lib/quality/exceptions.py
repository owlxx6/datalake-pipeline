"""Exceptions métier du module quality."""

from __future__ import annotations


class DQConfigError(ValueError):
    """Erreur de chargement ou de validation d'un fichier DQ YAML.

    Attributes:
        path: Chemin du fichier fautif (ou None si plusieurs fichiers).
        errors: Liste de dicts décrivant chaque erreur.
    """

    def __init__(
        self,
        message: str,
        *,
        path: str | None = None,
        errors: list[dict] | None = None,
    ) -> None:
        self.path = path
        self.errors = errors or []
        super().__init__(message)


class DQExecutionError(RuntimeError):
    """Erreur d'exécution d'une règle DQ par le runner Spark.

    Levée typiquement quand une règle référence une table inexistante,
    une colonne absente du DataFrame, ou un SQL invalide.
    """

    def __init__(self, message: str, *, rule_id: str | None = None) -> None:
        self.rule_id = rule_id
        super().__init__(message)


class DataQualityError(RuntimeError):
    """Erreur métier de qualité — taux de rejets bloquants au-dessus du seuil.

    Levée par les jobs lorsqu'un seuil métier (ex: 10% de rejets bloquants)
    est dépassé. À distinguer de `DQExecutionError` qui signale un problème
    technique d'exécution.
    """

    def __init__(
        self,
        message: str,
        *,
        failure_rate: float | None = None,
        threshold: float | None = None,
    ) -> None:
        self.failure_rate = failure_rate
        self.threshold = threshold
        super().__init__(message)
