"""SilverWriter — écriture vers la couche Silver (Hive/Iceberg)."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from datalake_lib.read.models import JobConfig

from .exceptions import WriteError
from .models import WriteOptions, WriteResult
from .utils import (
    add_technical_cols,
    build_merge_sql,
    count_partitions_written,
    validate_merge_keys,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_TEMP_VIEW_NAME = "__silver_write_src"


class SilverWriter:
    """Écrit un DataFrame dans une table Hive/Iceberg de la couche Silver.

    Modes supportés :
    - `full`        : INSERT OVERWRITE PARTITION (recharge la partition cible
                       uniquement, pas de DROP TABLE).
    - `incremental` : MERGE INTO via SQL Spark (Iceberg requis).

    Le job_name est dérivé du config.table : `JOB_SILVER_{TABLE}`.
    """

    def __init__(self, spark: SparkSession, config: JobConfig) -> None:
        self.spark = spark
        self.config = config
        self.job_name = f"JOB_SILVER_{config.table.upper()}"

    # ── API publique ──────────────────────────────────────────────────────

    def write(
        self,
        df: DataFrame,
        options: WriteOptions | None = None,
    ) -> WriteResult:
        """Écrit `df` dans la table Silver selon `options.mode`."""
        opts = options or WriteOptions()
        table = self._build_table_name()

        # Ajouter colonnes techniques avant écriture.
        df_to_write = add_technical_cols(df, layer="silver", job_name=self.job_name)

        if opts.dry_run:
            logger.info(
                "dry_run=True → plan : %s mode=%s rows=%d",
                table,
                opts.mode,
                df_to_write.count(),
            )
            return WriteResult(
                table=table,
                layer="silver",
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
        if opts.mode == "full":
            rows_written = self._write_full(df_to_write, opts, table)
        elif opts.mode == "incremental":
            validate_merge_keys(df_to_write, opts.merge_keys)
            rows_written = self._write_incremental(df_to_write, opts, table)
        else:
            raise WriteError(
                f"Mode d'écriture inconnu : '{opts.mode}'", target=table
            )
        duration = time.monotonic() - start

        return WriteResult(
            table=table,
            layer="silver",
            mode=opts.mode,
            rows_written=rows_written,
            partitions_written=count_partitions_written(df_to_write, opts.partition_by),
            write_duration_seconds=duration,
            target_path=table,
        )

    # ── Modes ─────────────────────────────────────────────────────────────

    def _write_full(
        self,
        df: DataFrame,
        options: WriteOptions,
        table: str,
    ) -> int:
        """INSERT OVERWRITE PARTITION (dynamic).

        Si la table n'existe pas → `saveAsTable(partitionBy=...)`.
        Si elle existe → `insertInto(overwrite=True)` avec
        `partitionOverwriteMode=dynamic`.
        """
        rows = df.count()
        catalog = self.spark.catalog

        if catalog.tableExists(table):
            self.spark.conf.set(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )
            df.write.mode("overwrite").insertInto(table, overwrite=True)
            logger.info(
                "Silver write_full (insertInto dynamic) : table=%s rows=%d",
                table,
                rows,
            )
        else:
            writer = df.write.mode("overwrite")
            if options.partition_by:
                existing_parts = [
                    c for c in options.partition_by if c in df.columns
                ]
                if existing_parts:
                    writer = writer.partitionBy(*existing_parts)
            writer.saveAsTable(table)
            logger.info(
                "Silver write_full (saveAsTable) : table=%s rows=%d", table, rows
            )

        return rows

    def _write_incremental(
        self,
        df: DataFrame,
        options: WriteOptions,
        table: str,
    ) -> int:
        """MERGE INTO via SQL Spark.

        Si la table cible n'existe pas, on bascule sur `saveAsTable`
        (pas de cible à merger).
        """
        rows = df.count()
        catalog = self.spark.catalog

        if not catalog.tableExists(table):
            logger.info(
                "Table %s absente → fallback sur saveAsTable pour le 1er chargement",
                table,
            )
            writer = df.write.mode("overwrite")
            existing_parts = [c for c in options.partition_by if c in df.columns]
            if existing_parts:
                writer = writer.partitionBy(*existing_parts)
            writer.saveAsTable(table)
            return rows

        df.createOrReplaceTempView(_TEMP_VIEW_NAME)
        sql = build_merge_sql(
            target=table,
            source_alias=_TEMP_VIEW_NAME,
            merge_keys=options.merge_keys,
            all_columns=list(df.columns),
        )
        logger.debug("MERGE SQL :\n%s", sql)
        self.spark.sql(sql)

        return rows

    # ── Helpers ───────────────────────────────────────────────────────────

    def _build_table_name(self) -> str:
        """Retourne `silver_{domain}_{table}`."""
        return f"silver_{self.config.domain}_{self.config.table}"
