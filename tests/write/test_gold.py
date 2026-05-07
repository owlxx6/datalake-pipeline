"""Tests de GoldWriter — Spark requis."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from datalake_lib.read import JobConfig
from datalake_lib.write import GoldWriter, WriteError, WriteOptions


# ── _build_table_name (sans Spark) ────────────────────────────────────────


class TestBuildTableName:
    def test_concatenation(self) -> None:
        cfg = JobConfig(domain="credit", table="kpi_encours")
        writer = GoldWriter.__new__(GoldWriter)
        writer.config = cfg
        assert writer._build_table_name() == "gold_credit_kpi_encours"


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture()
def gold_writer(spark):
    cfg = JobConfig(
        domain="credit",
        table="kpi_encours",
        layer="gold",
        source_system="dwh",
    )
    return GoldWriter(spark, cfg)


@pytest.fixture()
def kpi_df(spark):
    return spark.createDataFrame(
        [
            ("credit", 1500000.0, "2025-01-01"),
            ("garantie", 800000.0, "2025-01-01"),
        ],
        ["domain", "encours_total", "date_arrete"],
    )


@pytest.fixture(autouse=True)
def cleanup_table(spark):
    table = "gold_credit_kpi_encours"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    yield
    spark.sql(f"DROP TABLE IF EXISTS {table}")


# ── write() mode=full ─────────────────────────────────────────────────────


class TestWriteFull:
    def test_write_full_creates_table(self, gold_writer, kpi_df, spark) -> None:
        result = gold_writer.write(kpi_df, WriteOptions(mode="full"))

        assert result.layer == "gold"
        assert result.mode == "full"
        assert result.rows_written == 2
        assert result.table == "gold_credit_kpi_encours"
        assert spark.catalog.tableExists("gold_credit_kpi_encours")

    def test_write_adds_gold_technical_cols(
        self, gold_writer, kpi_df, spark
    ) -> None:
        gold_writer.write(kpi_df, WriteOptions(mode="full"))
        loaded = spark.read.table("gold_credit_kpi_encours")
        assert "_gold_ts" in loaded.columns
        assert "_job_name" in loaded.columns
        assert (
            loaded.select("_job_name").first()["_job_name"]
            == "JOB_GOLD_KPI_ENCOURS"
        )


# ── write() mode=incremental → erreur ─────────────────────────────────────


class TestIncrementalForbidden:
    def test_incremental_raises_write_error(self, gold_writer, kpi_df) -> None:
        with pytest.raises(WriteError, match="ne supporte pas le mode incremental"):
            gold_writer.write(
                kpi_df,
                WriteOptions(mode="incremental", merge_keys=["domain"]),
            )

    def test_incremental_does_not_create_table(
        self, gold_writer, kpi_df, spark
    ) -> None:
        with pytest.raises(WriteError):
            gold_writer.write(kpi_df, WriteOptions(mode="incremental"))
        # La table n'a pas été créée.
        assert not spark.catalog.tableExists("gold_credit_kpi_encours")


# ── dry_run ───────────────────────────────────────────────────────────────


class TestDryRun:
    def test_dry_run_does_not_create_table(
        self, gold_writer, kpi_df, spark
    ) -> None:
        result = gold_writer.write(kpi_df, WriteOptions(dry_run=True))
        assert result.rows_written == 0
        assert not spark.catalog.tableExists("gold_credit_kpi_encours")
