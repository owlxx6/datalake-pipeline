"""Modèles pydantic v2 du module read.

`JobConfig` est un modèle léger de **configuration runtime** pour un job
de lecture — distinct de `TableConfig` (qui décrit la structure complète
Bronze/Silver/Gold dans dags/configs/*.yaml).
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Layer = Literal["bronze", "silver", "gold"]


# ── Source landing (CSV) ──────────────────────────────────────────────────


class SourceConfig(BaseModel):
    """Configuration de la zone landing (CSV sur HDFS) pour LandingReader."""

    path_pattern: str
    """Path HDFS du CSV. Peut contenir le placeholder `{date_arrete}`
    qui sera remplacé par la date passée dans ReadOptions au format YYYYMMDD."""

    delimiter: str = ";"
    has_header: bool = True
    encoding: str = "UTF-8"


# ── JobConfig — config runtime d'un job ───────────────────────────────────


class JobConfig(BaseModel):
    """Configuration d'un job de lecture.

    Utilisée par BronzeReader et LandingReader pour résoudre :
    - le nom de la table Hive (Bronze) → `bronze_{domain}_{table}`
    - le path landing (CSV) → `source.path_pattern`
    """

    domain: str
    table: str
    layer: Layer = "bronze"
    source_system: str = ""
    source: SourceConfig | None = None


# ── Options de lecture ────────────────────────────────────────────────────


class ReadOptions(BaseModel):
    """Options à passer à `read()` pour ajuster la lecture."""

    filter_date: date | None = None
    """Filtre sur la partition `date_arrete` (Bronze) ou substitue
    `{date_arrete}` dans le path_pattern (Landing)."""

    columns: list[str] = Field(default_factory=list)
    """Projection — vide = toutes les colonnes."""

    limit: int | None = None
    """Limite — pour dev/test uniquement, **pas de prod**."""

    coalesce: int | None = None
    """Réduit le nombre de partitions Spark après lecture."""

    encoding: str = ""
    """Override de l'encodage (laisse vide pour utiliser la valeur de SourceConfig)."""


# ── Résultat de lecture ───────────────────────────────────────────────────


class ReadResult(BaseModel):
    """Métadonnées renvoyées par `read_with_result()`.

    Le DataFrame Spark lui-même est renvoyé séparément (ce modèle ne le
    contient pas — pydantic ne sérialise pas un objet Spark).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    table: str
    row_count: int
    columns: list[str]
    partition_date: date | None = None
    source_path: str
    read_duration_seconds: float
