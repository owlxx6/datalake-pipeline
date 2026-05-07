"""Module Write — écriture Silver/Gold (Hive/Iceberg).

Architecture :
- `models.py`     : WriteOptions, WriteResult
- `silver.py`     : SilverWriter (full + incremental MERGE)
- `gold.py`       : GoldWriter (full uniquement)
- `utils.py`      : build_merge_sql, validate_merge_keys, add_technical_cols
- `exceptions.py` : WriteError, MergeKeyError, PartitionError

Usage typique :

    from datalake_lib.write import SilverWriter, WriteOptions
    from datalake_lib.read import JobConfig

    config = JobConfig(domain="credit", table="credit_amort")
    writer = SilverWriter(spark, config)

    # Mode full (recharge la partition date_arrete)
    result = writer.write(df, WriteOptions(mode="full"))

    # Mode incremental (MERGE INTO sur PK)
    result = writer.write(df, WriteOptions(
        mode="incremental",
        merge_keys=["IDF_NUM_DOSS"],
    ))
"""

from __future__ import annotations

from .exceptions import MergeKeyError, PartitionError, WriteError
from .gold import GoldWriter
from .models import WriteOptions, WriteResult
from .silver import SilverWriter
from .utils import (
    add_technical_cols,
    build_merge_sql,
    count_partitions_written,
    validate_merge_keys,
)

__all__ = [
    "GoldWriter",
    "MergeKeyError",
    "PartitionError",
    "SilverWriter",
    "WriteError",
    "WriteOptions",
    "WriteResult",
    "add_technical_cols",
    "build_merge_sql",
    "count_partitions_written",
    "validate_merge_keys",
]
