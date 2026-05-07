"""Modèles pydantic v2 du module write."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

WriteMode = Literal["full", "incremental"]
TargetLayer = Literal["silver", "gold"]


# ── Options d'écriture ────────────────────────────────────────────────────


class WriteOptions(BaseModel):
    """Options à passer à `write()` pour ajuster l'écriture."""

    mode: WriteMode = "full"
    """`full` = INSERT OVERWRITE PARTITION (recharge la partition cible).
    `incremental` = MERGE INTO (Silver uniquement, nécessite merge_keys)."""

    merge_keys: list[str] = Field(default_factory=list)
    """Clés de merge — obligatoires en mode incremental."""

    partition_by: list[str] = Field(default_factory=lambda: ["date_arrete"])
    """Colonnes de partitionnement (défaut Bâle III : `date_arrete`)."""

    coalesce: int = 1
    """Nombre de fichiers en sortie (POC=1, prod=auto)."""

    overwrite_schema: bool = False
    """Autoriser un changement de schéma à l'écriture."""

    dry_run: bool = False
    """Si True, log le plan d'écriture mais n'écrit rien.
    Le WriteResult retourné a `rows_written=0`."""


# ── Résultat d'écriture ───────────────────────────────────────────────────


class WriteResult(BaseModel):
    """Métadonnées renvoyées par `write()`."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    table: str
    layer: TargetLayer
    mode: str
    rows_written: int
    partitions_written: int
    write_duration_seconds: float
    target_path: str
