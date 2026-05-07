"""Tests de BronzeReader — nécessite une SparkSession.

Pour tester le path Hive sans installer un metastore externe, on
utilise le catalog Spark local : on crée des tables temporaires via
`saveAsTable` dans le warehouse `/tmp/spark-warehouse`.
"""

from __future__ import annotations

from datetime import date

import pytest

pytest.importorskip("pyspark")

from datalake_lib.read import BronzeReader, JobConfig, ReadError, ReadOptions


# ── _build_hive_table_name (méthode pure, pas besoin de Spark) ────────────


class TestBuildHiveTableName:
    def test_concatenation(self) -> None:
        cfg = JobConfig(domain="credit", table="credit_encours")
        # Inutile d'instancier Spark — on appelle la méthode via une
        # instance dummy qui n'utilise pas spark dans __init__.
        reader = BronzeReader.__new__(BronzeReader)
        reader.config = cfg
        assert reader._build_hive_table_name() == "bronze_credit_credit_encours"

    def test_concatenation_garweb(self) -> None:
        cfg = JobConfig(domain="garweb", table="garantie_contrat")
        reader = BronzeReader.__new__(BronzeReader)
        reader.config = cfg
        assert reader._build_hive_table_name() == "bronze_garweb_garantie_contrat"


# ── read_with_result + filtres (Spark requis) ─────────────────────────────


@pytest.fixture(scope="module")
def credit_table(spark):
    """Crée une table Hive temporaire `bronze_credit_credit_encours`.

    Lifecycle : créée une fois par module, supprimée à la fin.
    """
    from pyspark.sql import functions as F

    table = "bronze_credit_credit_encours"

    rows = [
        ("CTR-0001", 100000.0, 75000.0, "2025-01-01"),
        ("CTR-0002", 250000.0, 180000.0, "2025-01-01"),
        ("CTR-0003", 50000.0, 0.0, "2025-01-02"),
        ("CTR-0004", 1500000.0, 1490000.0, "2025-01-02"),
    ]
    df = spark.createDataFrame(
        rows,
        ["NUM_CONTRAT", "MONTANT_ACCORDE", "MONTANT_RESTANT", "date_arrete"],
    ).withColumn("date_arrete", F.to_date("date_arrete"))

    spark.sql(f"DROP TABLE IF EXISTS {table}")
    df.write.mode("overwrite").saveAsTable(table)
    yield table
    spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture()
def credit_reader(spark) -> BronzeReader:
    cfg = JobConfig(
        domain="credit",
        table="credit_encours",
        layer="bronze",
        source_system="evolan",
    )
    return BronzeReader(spark, cfg)


class TestRead:
    def test_read_with_result_returns_metrics(
        self, credit_reader: BronzeReader, credit_table: str
    ) -> None:
        df, result = credit_reader.read_with_result()

        assert result.table == credit_table
        assert result.row_count == df.count()
        assert "NUM_CONTRAT" in result.columns
        assert result.read_duration_seconds >= 0.0

    def test_read_filter_date_prunes_partition(
        self, credit_reader: BronzeReader, credit_table: str
    ) -> None:
        df = credit_reader.read(ReadOptions(filter_date=date(2025, 1, 1)))
        assert df.count() == 2

        df_other = credit_reader.read(ReadOptions(filter_date=date(2025, 1, 2)))
        assert df_other.count() == 2

    def test_read_columns_projection(
        self, credit_reader: BronzeReader, credit_table: str
    ) -> None:
        df = credit_reader.read(ReadOptions(columns=["NUM_CONTRAT"]))
        # 1 colonne projetée + 3 colonnes d'audit ajoutées par add_audit_cols.
        assert "NUM_CONTRAT" in df.columns
        assert "MONTANT_ACCORDE" not in df.columns

    def test_read_includes_audit_cols(
        self, credit_reader: BronzeReader, credit_table: str
    ) -> None:
        df = credit_reader.read()
        for col in ["_ingestion_ts", "_source_system", "_layer"]:
            assert col in df.columns

    def test_read_limit(
        self, credit_reader: BronzeReader, credit_table: str
    ) -> None:
        df = credit_reader.read(ReadOptions(limit=1))
        assert df.count() == 1


class TestReadErrors:
    def test_read_unknown_table_raises(self, spark) -> None:
        cfg = JobConfig(domain="ghost", table="missing")
        reader = BronzeReader(spark, cfg)
        with pytest.raises(ReadError, match="introuvable"):
            reader.read()
