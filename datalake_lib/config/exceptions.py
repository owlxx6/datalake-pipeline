"""Exceptions métier pour le chargement et la validation des configs YAML."""

from __future__ import annotations


class ConfigNotFoundError(FileNotFoundError):
    """Fichier de configuration YAML introuvable."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Fichier de configuration introuvable : '{path}'")


class ConfigValidationError(ValueError):
    """Erreur(s) de validation dans un ou plusieurs fichiers de configuration.

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
