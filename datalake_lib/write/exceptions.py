"""Exceptions métier du module write."""

from __future__ import annotations


class WriteError(RuntimeError):
    """Erreur générique d'écriture (échec Spark, mode invalide, etc.).

    Attributes:
        target: Table ou path qui a échoué.
    """

    def __init__(self, message: str, *, target: str | None = None) -> None:
        self.target = target
        super().__init__(message)


class MergeKeyError(ValueError):
    """Erreur sur les clés de merge en mode incremental.

    Levée quand `merge_keys` est vide ou contient une colonne absente
    du DataFrame source.
    """

    def __init__(self, message: str, *, missing_keys: list[str] | None = None) -> None:
        self.missing_keys = missing_keys or []
        super().__init__(message)


class PartitionError(ValueError):
    """Erreur liée au partitionnement (colonne absente, valeur invalide)."""

    def __init__(self, message: str, *, partition_col: str | None = None) -> None:
        self.partition_col = partition_col
        super().__init__(message)
