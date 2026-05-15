"""config_loader.py — Pydantic models + YAML loader pour le Bronze Engine groupé."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel


# ── Globals ───────────────────────────────────────────────────────────────────

class SeverityWeights(BaseModel):
    CRITICAL: int = 1
    HIGH: int = 2
    MEDIUM: int = 3
    LOW: int = 4


class Globals(BaseModel):
    layer_abbrev: str
    severity_weights: SeverityWeights = SeverityWeights()


# ── Project ───────────────────────────────────────────────────────────────────

class Project(BaseModel):
    layer: str
    domain: str
    environment: str = "DEV"
    execution_id_template: str


# ── Spark ─────────────────────────────────────────────────────────────────────

class SparkConfigEntry(BaseModel):
    key: str
    value: str


class SparkConfig(BaseModel):
    configs: list[SparkConfigEntry] = []


# ── Output ────────────────────────────────────────────────────────────────────

class OutputTable(BaseModel):
    name_template: str
    format: str = "iceberg"
    mode: str = "append"
    partition_by: list[str] = []
    enabled: bool = True


class OutputConfig(BaseModel):
    database: str
    quality_results: OutputTable
    logs_rejected: OutputTable
    log_pipeline: OutputTable


# ── Performance / Retry / Execution ──────────────────────────────────────────

class RepartitionConfig(BaseModel):
    enabled: bool = False
    num_partitions: int = 8


class PerformanceConfig(BaseModel):
    cache_source_data: bool = True
    repartition: RepartitionConfig = RepartitionConfig()


class RetryConfig(BaseModel):
    max_retries: int = 3
    backoff_seconds: int = 2
    retryable_exceptions: list[str] = []


class DateSituationConfig(BaseModel):
    source: str = "today"
    format: str = "yyyy-MM-dd"
    column_name: Optional[str] = None
    fixed_date: Optional[str] = None


class ExecutionConfig(BaseModel):
    dry_run: bool = False
    date_situation: DateSituationConfig = DateSituationConfig()


# ── Table-level models ────────────────────────────────────────────────────────

class LoadingConfig(BaseModel):
    mode: str = "FULL"
    incremental_key: Optional[str] = None
    watermark_delay: Optional[str] = None


class TableSourceConfig(BaseModel):
    path_template: str
    format: str = "parquet"
    loading: LoadingConfig = LoadingConfig()


class SchemaField(BaseModel):
    name: str
    spark_type: str
    nullable: bool = True
    description: str = ""


class SchemaConfig(BaseModel):
    on_cast_error: str = "reject"
    fields: list[SchemaField]


class WriteConfig(BaseModel):
    format: str = "parquet"
    path_template: str
    mode: str = "overwrite"
    partition_by: list[str] = ["date_arrete"]
    compression: str = "snappy"
    coalesce: int = 4


class SilverWriteConfig(BaseModel):
    """Écriture des lignes propres (DQ passé) dans une table Hive Silver."""
    database: str
    table: str
    mode: str = "overwrite"
    partition_by: list[str] = ["date_arrete"]


class DQCheck(BaseModel):
    type: str
    severity: str = "MEDIUM"
    alert_threshold: float = 0.0
    control_type: str = "BASIC"
    allowed_values: Optional[list[str]] = None
    pattern: Optional[str] = None
    max_length: Optional[int] = None
    min_date: Optional[str] = None
    max_date_expr: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None


class QualityRule(BaseModel):
    field: str
    source_column: str
    data_type: str
    description: str = ""
    use_case: str = ""
    filter_condition: Optional[str] = None
    checks: list[DQCheck]


class ConsistencyRule(BaseModel):
    name: str
    description: str = ""
    use_case: str = ""
    condition: str
    required_fields: list[str]
    severity: str = "MEDIUM"
    alert_threshold: float = 0.0
    control_type: str = "ADVANCED"


class TableEntry(BaseModel):
    """Une table dans le YAML groupé."""
    name: str
    domain: str
    primary_key: list[str]
    is_partitioned: bool = False
    source: TableSourceConfig
    schema: SchemaConfig
    write: WriteConfig                          # Bronze HDFS — données brutes (Parquet)
    silver_write: Optional[SilverWriteConfig] = None  # Silver Hive — données clean (post-DQ)
    quality_rules: list[QualityRule] = []
    consistency_rules: list[ConsistencyRule] = []


# ── Top-level BronzeConfig ────────────────────────────────────────────────────

class BronzeConfig(BaseModel):
    globals: Globals
    project: Project
    spark: SparkConfig = SparkConfig()
    output: OutputConfig
    performance: PerformanceConfig = PerformanceConfig()
    retry: RetryConfig = RetryConfig()
    execution: ExecutionConfig = ExecutionConfig()
    tables: list[TableEntry]

    def resolve(self, template: str, table_name: str = "", **extra) -> str:
        """Résout les placeholders d'un template string."""
        vars_ = {
            "layer_abbrev": self.globals.layer_abbrev,
            "domain": self.project.domain.upper(),
            "table_name": table_name,
            "timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
            **extra,
        }
        return template.format(**vars_)

    def get_table(self, name: str) -> Optional[TableEntry]:
        """Retourne une table par son nom (insensible à la casse)."""
        for t in self.tables:
            if t.name.upper() == name.upper():
                return t
        return None


# ── Loader ────────────────────────────────────────────────────────────────────

def load_config(yaml_path: str) -> BronzeConfig:
    """Charge et valide le fichier YAML Bronze groupé."""
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Config introuvable : {yaml_path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return BronzeConfig(**raw)
