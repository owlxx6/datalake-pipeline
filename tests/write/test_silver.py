"""Tests de SilverWriter — Spark requis."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from datalake_lib.read import JobConfig
from datalake_lib.write import MergeKeyError, SilverWriter, WriteOptions


# ── _build_table_name (sans Spark) ────────────────────────────────────────


class TestBuildTableName:
    def test_concatenation(self) -> None:
        cfg = JobConfig(domain="credit", table="credit_encours")
        writer = SilverWriter.__new__(SilverWriter)
        writer.config = cfg
        assert writer._build_table_name() == "silver_credit_credit_encours"


# ── Fixtures Spark ────────────────────────────────────────────────────────


@pytest.fixture()
def silver_writer(spark):
    cfg = JobConfig(
        domain="credit",
        table="credit_encours",
        layer="silver",
        source_system="evolan",
    )
    return SilverWriter(spark, cfg)


@pytest.fixture()
def sample_df(spark):
    return spark.createDataFrame(
        [
            ("CTR-0001", 100000.0, "2025-01-01"),
            ("CTR-0002", 250000.0, "2025-01-01"),
            ("CTR-0003", 50000.0, "2025-01-02"),
        ],
        ["NUM_CONTRAT", "MONTANT", "date_arrete"],
    )


@pytest.fixture(autouse=True)
def cleanup_table(spark):
    """Supprime la table de test avant et après chaque test."""
    table = "silver_credit_credit_encours"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    yield
    spark.sql(f"DROP TABLE IF EXISTS {table}")


# ── write() mode=full ─────────────────────────────────────────────────────


class TestWriteFull:
    def test_write_full_creates_table(
        self, silver_writer, sample_df, spark
    ) -> None:
        result = silver_writer.write(sample_df, WriteOptions(mode="full"))

        assert result.layer == "silver"
        assert result.mode == "full"
        assert result.rows_written == 3
        assert result.table == "silver_credit_credit_encours"
        assert spark.catalog.tableExists("silver_credit_credit_encours")

    def test_write_full_row_count_matches(
        self, silver_writer, sample_df, spark
    ) -> None:
        silver_writer.write(sample_df, WriteOptions(mode="full"))
        loaded = spark.read.table("silver_credit_credit_encours")
        assert loaded.count() == 3

    def test_write_full_partitions_count(
        self, silver_writer, sample_df
    ) -> None:
        result = silver_writer.write(sample_df, WriteOptions(mode="full"))
        # 2 dates distinctes dans le sample.
        assert result.partitions_written == 2

    def test_write_adds_technical_cols(
        self, silver_writer, sample_df, spark
    ) -> None:
        silver_writer.write(sample_df, WriteOptions(mode="full"))
        loaded = spark.read.table("silver_credit_credit_encours")
        assert "_updated_ts" in loaded.columns
        assert "_job_name" in loaded.columns
        assert (
            loaded.select("_job_name").first()["_job_name"]
            == "JOB_SILVER_CREDIT_ENCOURS"
        )


# ── write() dry_run ───────────────────────────────────────────────────────


class TestDryRun:
    def test_dry_run_does_not_create_table(
        self, silver_writer, sample_df, spark
    ) -> None:
        result = silver_writer.write(
            sample_df, WriteOptions(mode="full", dry_run=True)
        )
        assert result.rows_written == 0
        assert not spark.catalog.tableExists("silver_credit_credit_encours")


# ── write() mode=incremental ──────────────────────────────────────────────


class TestWriteIncremental:
    def test_incremental_without_merge_keys_raises(
        self, silver_writer, sample_df
    ) -> None:
        with pytest.raises(MergeKeyError, match="vide"):
            silver_writer.write(
                sample_df, WriteOptions(mode="incremental", merge_keys=[])
            )

    def test_incremental_first_load_uses_saveAsTable(
        self, silver_writer, sample_df, spark
    ) -> None:
        """Si la table n'existe pas, le mode incremental tombe sur saveAsTable."""
        result = silver_writer.write(
            sample_df,
            WriteOptions(mode="incremental", merge_keys=["NUM_CONTRAT"]),
        )
        assert result.rows_written == 3
        assert spark.catalog.tableExists("silver_credit_credit_encours")

    def test_incremental_merges_existing_rows(
        self, silver_writer, sample_df, spark
    ) -> None:
        """MERGE INTO nécessite Iceberg/Delta — skip sinon."""
        # 1er chargement.
        silver_writer.write(sample_df, WriteOptions(mode="full"))

        # Données mises à jour : MONTANT du CTR-0001 changé, nouveau CTR-0099.
        updated_df = spark.createDataFrame(
            [
                ("CTR-0001", 999999.0, "2025-01-01"),
                ("CTR-0099", 50000.0, "2025-01-03"),
            ],
            ["NUM_CONTRAT", "MONTANT", "date_arrete"],
        )

        try:
            silver_writer.write(
                updated_df,
                WriteOptions(mode="incremental", merge_keys=["NUM_CONTRAT"]),
            )
        except Exception as exc:
            msg = str(exc).lower()
            if any(kw in msg for kw in ["merge", "iceberg", "v2 table", "not supported"]):
                pytest.skip(f"MERGE non supporté sans Iceberg : {exc}")
            raise

        loaded = spark.read.table("silver_credit_credit_encours")
        rows = {r["NUM_CONTRAT"]: r["MONTANT"] for r in loaded.collect()}
        assert rows["CTR-0001"] == 999999.0  # mis à jour
        assert rows["CTR-0002"] == 250000.0  # inchangé
        assert rows["CTR-0099"] == 50000.0   # inséré
