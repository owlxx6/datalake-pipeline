"""LandingReader — lecture des CSV depuis HDFS landing zone."""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

from .exceptions import PathResolutionError, ReadError
from .models import JobConfig, ReadOptions
from .utils import add_audit_cols

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# Placeholder reconnu dans `source.path_pattern`.
_DATE_PLACEHOLDER = "{date_arrete}"


class LandingReader:
    """Lecture de fichiers CSV depuis la zone landing HDFS.

    Le path est résolu depuis `config.source.path_pattern` :
    - si le pattern contient `{date_arrete}` et que `options.filter_date`
      est fourni, on substitue la date au format `YYYYMMDD`.
    - sinon le pattern est utilisé tel quel.

    Encodage : `config.source.encoding` (défaut "UTF-8") ; surchargeable
    via `options.encoding`. La méthode `_detect_encoding` est un stub à
    enrichir en prod (lecture HDFS + chardet).
    """

    def __init__(self, spark: SparkSession, config: JobConfig) -> None:
        if config.source is None:
            raise ReadError(
                "JobConfig.source est requis pour LandingReader",
                source=config.table,
            )
        self.spark = spark
        self.config = config

    # ── API publique ──────────────────────────────────────────────────────

    def read(self, options: ReadOptions | None = None) -> DataFrame:
        """Lit les CSV de la landing zone selon la config.

        Args:
            options: Options de lecture (defaults si None).

        Returns:
            DataFrame Spark avec colonne d'audit `_source_file_name`
            ajoutée via `input_file_name()`.
        """
        from pyspark.sql import functions as F

        opts = options or ReadOptions()
        source = self.config.source
        assert source is not None  # garanti par __init__

        path = self._resolve_path(source.path_pattern, opts.filter_date)
        encoding = opts.encoding or source.encoding or self._detect_encoding(path)

        try:
            df = (
                self.spark.read
                .option("header", str(source.has_header).lower())
                .option("delimiter", source.delimiter)
                .option("encoding", encoding)
                .option("inferSchema", "true")
                .csv(path)
            )
        except Exception as exc:
            raise ReadError(
                f"Échec lecture CSV '{path}' : {exc}",
                source=path,
            ) from exc

        # Colonne d'audit : nom du fichier source.
        df = df.withColumn("_source_file_name", F.input_file_name())

        # Audit cols communes (timestamp, source_system, layer).
        df = add_audit_cols(df, self.config.source_system, self.config.layer)

        # Projection.
        if opts.columns:
            keep = [c for c in opts.columns if c in df.columns]
            df = df.select(*keep)

        # Limite (avec warning).
        if opts.limit is not None:
            logger.warning(
                "LandingReader.read : limit=%d appliqué — réservé dev/test",
                opts.limit,
            )
            df = df.limit(opts.limit)

        # Coalesce.
        if opts.coalesce is not None:
            df = df.coalesce(opts.coalesce)

        return df

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _resolve_path(pattern: str, filter_date: date | None) -> str:
        """Substitue `{date_arrete}` par la date au format `YYYYMMDD`.

        Si le pattern ne contient pas de placeholder, retourné tel quel.
        Si le pattern contient `{date_arrete}` mais que `filter_date` est
        None, lève `PathResolutionError`.
        """
        if _DATE_PLACEHOLDER not in pattern:
            return pattern

        if filter_date is None:
            raise PathResolutionError(
                f"Le path_pattern contient '{_DATE_PLACEHOLDER}' mais aucun "
                f"filter_date n'est fourni dans ReadOptions",
                pattern=pattern,
            )

        return pattern.replace(_DATE_PLACEHOLDER, filter_date.strftime("%Y%m%d"))

    @staticmethod
    def _detect_encoding(path: str) -> str:
        """Stub POC — retourne toujours 'UTF-8'.

        En prod, à enrichir : lire les premiers Ko via `hdfs dfs -cat`
        + `chardet.detect()` pour gérer les fichiers ISO-8859-1 émis
        par certains systèmes legacy.
        """
        logger.debug(
            "_detect_encoding stub : retourne 'UTF-8' pour '%s'", path
        )
        return "UTF-8"
