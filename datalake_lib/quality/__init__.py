"""Module Quality — moteur de règles DQ déclaratives.

Architecture modulaire :
- `models/`  : structures pydantic (DQRule, DQConfig, DQResult, DQReport)
- `parser/`  : lecture YAML → objets typés
- `engine/`  : exécution PySpark sur DataFrame
- `utils/`   : gestion des rejets et logging
- `dq_configs/` : règles métier au format YAML

Usage typique :
    >>> from datalake_lib.quality import parse_dq_config, SparkDQRunner
    >>> config = parse_dq_config("dq_configs/credit_amort_dq.yaml")
    >>> runner = SparkDQRunner(spark)
    >>> df_valid, df_rejected, report = runner.run(df, config)
"""

from __future__ import annotations

from .engine.spark_dq_runner import SparkDQRunner
from .exceptions import DataQualityError, DQConfigError, DQExecutionError
from .models.rule_models import DQConfig, DQReport, DQResult, DQRule
from .parser.yaml_parser import parse_all_dq_configs, parse_dq_config
from .utils.metrics_logger import format_console_report, log_dq_report
from .utils.reject_handler import count_rejects_by_rule, write_rejects

__all__ = [
    "DataQualityError",
    "DQConfig",
    "DQConfigError",
    "DQExecutionError",
    "DQReport",
    "DQResult",
    "DQRule",
    "SparkDQRunner",
    "count_rejects_by_rule",
    "format_console_report",
    "log_dq_report",
    "parse_all_dq_configs",
    "parse_dq_config",
    "write_rejects",
]
