"""Modèles pydantic v2 pour la validation des configs YAML du pipeline médaillon."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

VALID_DOMAINS: set[str] = {
    "credit",
    "garantie",
    "tiers",
    "idce",
    "incident",
    "pivot",
    "produit",
    "ref",
    "personnel",
    "org",
}

CRON_RE = re.compile(
    r"^(\S+\s+){4}\S+$"  # 5 champs séparés par des espaces
)


# ── Sous-modèles ───────────────────────────────────────────────────────────


class Metadata(BaseModel):
    """Métadonnées de traçabilité du fichier YAML généré."""

    generated_from: str
    sheet_name: str
    generated_at: datetime


class BronzeConfig(BaseModel):
    """Configuration de la couche Bronze (Hive externe sur CSV/HDFS)."""

    ingestion_tool: Literal["nifi"]
    landing_path: str
    table: str
    storage: Literal["hive"]
    format: Literal["csv"]
    delimiter: str = ";"
    partition_by: str = "date_arrete"
    mode: Literal["append", "overwrite"]

    @field_validator("landing_path")
    @classmethod
    def landing_path_prefix(cls, v: str) -> str:
        if not v.startswith("/data/bronze/"):
            raise ValueError(
                f"landing_path doit commencer par '/data/bronze/', reçu : '{v}'"
            )
        return v

    @field_validator("table")
    @classmethod
    def table_prefix(cls, v: str) -> str:
        if not v.startswith("bronze_"):
            raise ValueError(
                f"le nom de table Bronze doit commencer par 'bronze_', reçu : '{v}'"
            )
        return v


class SchemaField(BaseModel):
    """Définition d'un champ dans le schéma Silver."""

    name: str
    type: str
    nullable: bool
    description: str = ""

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("le nom du champ ne peut pas être vide")
        return v

    @field_validator("type")
    @classmethod
    def type_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("le type du champ ne peut pas être vide")
        return v


class QualityRule(BaseModel):
    """Règle de qualité déclarative.

    Deux formes acceptées :
    - règles `not_null`, `unique`, `regex`, `range`, `in_set` : champ `columns` (liste)
    - règle `referential` : champs `column` + `reference_table` + `reference_column`
    """

    rule: Literal["not_null", "unique", "referential", "regex", "range", "in_set"]
    columns: list[str] = Field(default_factory=list)
    column: str = ""
    reference_table: str = ""
    reference_column: str = ""

    @model_validator(mode="after")
    def check_rule_shape(self) -> QualityRule:
        if self.rule == "referential":
            if not self.column:
                raise ValueError("rule=referential requiert le champ 'column'")
            if not self.reference_table:
                raise ValueError("rule=referential requiert le champ 'reference_table'")
            if not self.reference_column:
                raise ValueError("rule=referential requiert le champ 'reference_column'")
        else:
            if not self.columns:
                raise ValueError(
                    f"rule={self.rule} requiert une liste 'columns' non vide"
                )
        return self


class SilverConfig(BaseModel):
    """Configuration de la couche Silver (Iceberg, données typées et validées)."""

    table: str
    storage: Literal["iceberg"]
    primary_key: list[str] = Field(default_factory=list)
    write_mode: Literal["append", "merge", "scd2"]
    partition_by: str = "date_arrete"
    schema_: list[SchemaField] = Field(alias="schema", min_length=1)
    quality_rules: list[QualityRule] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    @field_validator("table")
    @classmethod
    def table_prefix(cls, v: str) -> str:
        if not v.startswith("silver_"):
            raise ValueError(
                f"le nom de table Silver doit commencer par 'silver_', reçu : '{v}'"
            )
        return v

    @model_validator(mode="after")
    def warn_empty_pk(self) -> SilverConfig:
        """Émet un warning si la PK est vide (pas une erreur bloquante)."""
        if not self.primary_key:
            logger.warning(
                "Table '%s' : aucune clé primaire définie — "
                "le merge/SCD2 ne sera pas possible sans PK.",
                self.table,
            )
        return self


class GoldConfig(BaseModel):
    """Configuration de la couche Gold (modèle en étoile)."""

    feeds: list = Field(default_factory=list)


# ── Modèle racine ──────────────────────────────────────────────────────────


class TableConfig(BaseModel):
    """Configuration complète d'une table du pipeline médaillon.

    Chaque fichier YAML dans dags/configs/ est validé contre ce modèle.
    """

    table_name: str
    domain: str
    description: str = ""
    metadata: Metadata
    bronze: BronzeConfig
    silver: SilverConfig
    gold: GoldConfig
    schedule: str
    owner: str
    frequency: str = ""
    storage_type: str = ""

    @field_validator("table_name")
    @classmethod
    def table_name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("table_name ne peut pas être vide")
        return v

    @field_validator("domain")
    @classmethod
    def domain_valid(cls, v: str) -> str:
        if v not in VALID_DOMAINS:
            raise ValueError(
                f"domaine '{v}' non reconnu. "
                f"Valeurs acceptées : {', '.join(sorted(VALID_DOMAINS))}"
            )
        return v

    @field_validator("schedule")
    @classmethod
    def schedule_cron(cls, v: str) -> str:
        clean = v.split("#")[0].strip()
        if not CRON_RE.match(clean):
            raise ValueError(
                f"'{v}' n'est pas un cron valide (5 champs attendus, ex: '0 6 * * *')"
            )
        return v

    @field_validator("owner")
    @classmethod
    def owner_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("owner ne peut pas être vide")
        return v

    @model_validator(mode="after")
    def check_table_name_coherence(self) -> TableConfig:
        """Vérifie la cohérence de nommage entre table_name, bronze.table et silver.table."""
        expected_bronze = f"bronze_{self.table_name.lower()}"
        if self.bronze.table != expected_bronze:
            raise ValueError(
                f"Incohérence de nommage : table_name='{self.table_name}' "
                f"mais bronze.table='{self.bronze.table}' "
                f"(attendu : '{expected_bronze}')"
            )

        expected_silver = f"silver_{self.table_name.lower()}"
        if self.silver.table != expected_silver:
            raise ValueError(
                f"Incohérence de nommage : table_name='{self.table_name}' "
                f"mais silver.table='{self.silver.table}' "
                f"(attendu : '{expected_silver}')"
            )

        return self
