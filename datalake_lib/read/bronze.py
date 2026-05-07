"""BronzeReader — lecture des tables Hive Bronze."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from .exceptions import ReadError
from .models import JobConfig, ReadOptions, ReadResult
from .utils import add_audit_cols

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class BronzeReader:
    """Lecture d'une table Hive de la couche Bronze.

    Le nom de la table est construit selon la convention :
        bronze_{domain}_{table}

    Filtres supportés via `ReadOptions` :
    - `filter_date`   : pruning sur la partition `date_arrete`
    - `columns`       : projection
    - `limit`         : pour dev/test (warning si appliqué)
    - `coalesce`      : repartitionnement après lecture
    """

    def __init__(self, spark: SparkSession, config: JobConfig) -> None:
        self.spark = spark
        self.config = config

    # ── API publique ──────────────────────────────────────────────────────

    def read(self, options: ReadOptions | None = None) -> DataFrame:
        """Lit la table Hive Bronze selon la config + options."""
        opts = options or ReadOptions()
        table_name = self._build_hive_table_name()

        try:
            df = self.spark.read.table(table_name)
        except Exception as exc:
            raise ReadError(
                f"Table Hive introuvable : '{table_name}' ({exc})",
                source=table_name,
            ) from exc

        df = self._apply_filters(df, opts)
        df = add_audit_cols(df, self.config.source_system, self.config.layer)

        if opts.coalesce is not None:
            df = df.coalesce(opts.coalesce)

        return df

    def read_with_result(
        self,
        options: ReadOptions | None = None,
    ) -> tuple[DataFrame, ReadResult]:
        """Lit la table et retourne aussi un ReadResult avec métriques."""
        opts = options or ReadOptions()
        table_name = self._build_hive_table_name()

        start = time.monotonic()
        df = self.read(opts)
        row_count = df.count()
        duration = time.monotonic() - start

        result = ReadResult(
            table=table_name,
            row_count=row_count,
            columns=list(df.columns),
            partition_date=opts.filter_date,
            source_path=table_name,
            read_duration_seconds=duration,
        )
        return df, result

    # ── Helpers ───────────────────────────────────────────────────────────

    def _build_hive_table_name(self) -> str:
        """Retourne `bronze_{domain}_{table}`."""
        return f"bronze_{self.config.domain}_{self.config.table}"

    def _apply_filters(
        self,
        df: DataFrame,
        options: ReadOptions,
    ) -> DataFrame:
        """Applique date_arrete, projection, limit (dans cet ordre)."""
        from pyspark.sql import functions as F

        if options.filter_date is not None:
            df = df.filter(F.col("date_arrete") == F.lit(options.filter_date))

        if options.columns:
            keep = [c for c in options.columns if c in df.columns]
            df = df.select(*keep)

        if options.limit is not None:
            logger.warning(
                "BronzeReader.read : limit=%d appliqué — réservé dev/test",
                options.limit,
            )
            df = df.limit(options.limit)

        return df
