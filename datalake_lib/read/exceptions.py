"""Exceptions métier du module read."""

from __future__ import annotations


class ReadError(RuntimeError):
    """Erreur générique de lecture (table introuvable, accès refusé, etc.).

    Attributes:
        source: Chemin ou nom de table qui a échoué.
    """

    def __init__(self, message: str, *, source: str | None = None) -> None:
        self.source = source
        super().__init__(message)


class PathResolutionError(ValueError):
    """Erreur lors de la résolution d'un path_pattern (placeholder manquant, format invalide)."""

    def __init__(self, message: str, *, pattern: str | None = None) -> None:
        self.pattern = pattern
        super().__init__(message)


class SchemaInferenceError(RuntimeError):
    """Erreur lors de l'inférence ou du cast d'un schéma."""

    def __init__(self, message: str, *, column: str | None = None) -> None:
        self.column = column
        super().__init__(message)
