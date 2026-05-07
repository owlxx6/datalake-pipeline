"""GoldWriter — écriture vers la couche Gold (DataMart agrégés)."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from datalake_lib.read.models import JobConfig

from .exceptions import WriteError
from .models import WriteOptions, WriteResult
from .utils import add_technical_cols, count_partitions_written

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class GoldWriter:
    """Écrit un DataFrame dans une table Gold (modèle en étoile, agrégats).

    Politique :
    - **Mode `full` uniquement**. Le mode `incremental` lève une `WriteError`
      car les tables Gold sont des agrégats recalculés à chaque exécution.
    - Coalesce avant écriture pour limiter le nombre de fichiers (POC : 1).
    - Colonnes techniques ajoutées : `_gold_ts`, `_job_name`.
    """

    def __init__(self, spark: SparkSession, config: JobConfig) -> None:
        self.spark = spark
        self.config = config
        self.job_name = f"JOB_GOLD_{config.table.upper()}"

    # ── API publique ──────────────────────────────────────────────────────

    def write(
        self,
        df: DataFrame,
        options: WriteOptions | None = None,
    ) -> WriteResult:
        """Écrit `df` dans la table Gold.

        Raises:
            WriteError: Si `options.mode == "incremental"`.
        """
        opts = options or WriteOptions()
        table = self._build_table_name()

        if opts.mode == "incremental":
            raise WriteError(
                "Gold layer ne supporte pas le mode incremental. Utiliser full.",
                target=table,
            )

        df_to_write = add_technical_cols(df, layer="gold", job_name=self.job_name)

        if opts.dry_run:
            logger.info(
                "dry_run=True → plan Gold : %s mode=full rows=%d",
                table,
                df_to_write.count(),
            )
            return WriteResult(
                table=table,
                layer="gold",
                mode=opts.mode,
                rows_written=0,
                partitions_written=count_partitions_written(
                    df_to_write, opts.partition_by
                ),
                write_duration_seconds=0.0,
                target_path=table,
            )

        if opts.coalesce > 0:
            df_to_write = df_to_write.coalesce(opts.coalesce)

        start = time.monotonic()
        rows_written = self._write_full(df_to_write, opts, table)
        duration = time.monotonic() - start

        return WriteResult(
            table=table,
            layer="gold",
            mode=opts.mode,
            rows_written=rows_written,
            partitions_written=count_partitions_written(df_to_write, opts.partition_by),
            write_duration_seconds=duration,
            target_path=table,
        )

    # ── Mode unique ───────────────────────────────────────────────────────

    def _write_full(
        self,
        df: DataFrame,
        options: WriteOptions,
        table: str,
    ) -> int:
        """Écrit la table Gold en overwrite total (recalcul complet)."""
        rows = df.count()

        writer = df.write.mode("overwrite")
        if options.partition_by:
            existing_parts = [c for c in options.partition_by if c in df.columns]
            if existing_parts:
                writer = writer.partitionBy(*existing_parts)
        writer.saveAsTable(table)

        logger.info(
            "Gold write_full : table=%s rows=%d partitions=%s",
            table,
            rows,
            options.partition_by,
        )
        return rows

    # ── Helpers ───────────────────────────────────────────────────────────

    def _build_table_name(self) -> str:
        """Retourne `gold_{domain}_{table}`."""
        return f"gold_{self.config.domain}_{self.config.table}"
