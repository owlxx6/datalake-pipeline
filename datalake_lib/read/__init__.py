"""Module Read — lecteurs de la couche Bronze (Hive table) et landing zone (CSV).

Architecture :
- `models.py`     : JobConfig, ReadOptions, ReadResult, SourceConfig
- `bronze.py`     : BronzeReader (lecture Hive table)
- `landing.py`    : LandingReader (lecture CSV depuis HDFS)
- `utils.py`      : add_audit_cols, cast_columns, infer_schema
- `exceptions.py` : ReadError, PathResolutionError, SchemaInferenceError

Usage typique :

    from datalake_lib.read import BronzeReader, JobConfig, ReadOptions
    from datetime import date

    config = JobConfig(domain="credit", table="credit_amort",
                       source_system="evolan")
    reader = BronzeReader(spark, config)
    df = reader.read(ReadOptions(filter_date=date(2026, 4, 25)))
"""

from __future__ import annotations

from .bronze import BronzeReader
from .exceptions import PathResolutionError, ReadError, SchemaInferenceError
from .landing import LandingReader
from .models import JobConfig, ReadOptions, ReadResult, SourceConfig
from .utils import add_audit_cols, cast_columns, infer_schema

__all__ = [
    "BronzeReader",
    "JobConfig",
    "LandingReader",
    "PathResolutionError",
    "ReadError",
    "ReadOptions",
    "ReadResult",
    "SchemaInferenceError",
    "SourceConfig",
    "add_audit_cols",
    "cast_columns",
    "infer_schema",
]
