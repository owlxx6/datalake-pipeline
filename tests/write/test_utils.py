"""Tests de utils.py — `build_merge_sql` testable sans Spark."""

from __future__ import annotations

import pytest

from datalake_lib.write import (
    MergeKeyError,
    build_merge_sql,
)


# ── build_merge_sql (pure Python) ─────────────────────────────────────────


class TestBuildMergeSql:
    def test_single_key_join(self) -> None:
        sql = build_merge_sql(
            target="silver_credit_amort",
            source_alias="src",
            merge_keys=["IDF_NUM_DOSS"],
            all_columns=["IDF_NUM_DOSS", "MNT_CREDIT", "DATE_ECHEANCE"],
        )
        assert "MERGE INTO silver_credit_amort" in sql
        assert "ON tgt.IDF_NUM_DOSS = src.IDF_NUM_DOSS" in sql
        # Update SET ne doit PAS contenir la clé de merge.
        assert "tgt.MNT_CREDIT = src.MNT_CREDIT" in sql
        assert "tgt.IDF_NUM_DOSS = src.IDF_NUM_DOSS" not in sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]

    def test_composite_key_join(self) -> None:
        sql = build_merge_sql(
            target="silver_garantie_renouvellement",
            source_alias="src",
            merge_keys=["IDF_NUM_CTR_GARANTIE", "IDF_NUM_INSCRIPTION"],
            all_columns=[
                "IDF_NUM_CTR_GARANTIE",
                "IDF_NUM_INSCRIPTION",
                "IDF_DATE_RENOUV",
            ],
        )
        # ON ... AND ... pour chaque clé.
        assert (
            "ON tgt.IDF_NUM_CTR_GARANTIE = src.IDF_NUM_CTR_GARANTIE "
            "AND tgt.IDF_NUM_INSCRIPTION = src.IDF_NUM_INSCRIPTION"
            in sql
        )

    def test_includes_all_non_key_columns_in_update(self) -> None:
        sql = build_merge_sql(
            target="silver_x",
            source_alias="src",
            merge_keys=["pk"],
            all_columns=["pk", "a", "b", "c"],
        )
        update_section = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "tgt.a = src.a" in update_section
        assert "tgt.b = src.b" in update_section
        assert "tgt.c = src.c" in update_section
        # pk ne doit pas être dans l'UPDATE.
        assert "tgt.pk = src.pk" not in update_section

    def test_insert_includes_all_columns(self) -> None:
        sql = build_merge_sql(
            target="silver_x",
            source_alias="src",
            merge_keys=["pk"],
            all_columns=["pk", "a", "b"],
        )
        insert_section = sql.split("WHEN NOT MATCHED THEN INSERT")[1]
        assert "(pk, a, b)" in insert_section
        assert "VALUES (src.pk, src.a, src.b)" in insert_section

    def test_empty_merge_keys_raises(self) -> None:
        with pytest.raises(MergeKeyError, match="merge_keys.*vide"):
            build_merge_sql(
                target="silver_x",
                source_alias="src",
                merge_keys=[],
                all_columns=["a", "b"],
            )


# ── Fonctions Spark (utilisent la fixture spark) ──────────────────────────

pytest.importorskip("pyspark")

from datalake_lib.write import (
    add_technical_cols,
    count_partitions_written,
    validate_merge_keys,
)


class TestValidateMergeKeys:
    def test_valid_keys(self, spark) -> None:
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        validate_merge_keys(df, ["a"])  # ne doit rien lever

    def test_empty_keys_raises(self, spark) -> None:
        df = spark.createDataFrame([(1,)], ["a"])
        with pytest.raises(MergeKeyError, match="vide"):
            validate_merge_keys(df, [])

    def test_missing_key_raises(self, spark) -> None:
        df = spark.createDataFrame([(1,)], ["a"])
        with pytest.raises(MergeKeyError, match="absentes"):
            validate_merge_keys(df, ["a", "missing"])


class TestAddTechnicalCols:
    def test_silver_adds_updated_ts_and_job_name(self, spark) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        out = add_technical_cols(df, layer="silver", job_name="JOB_X")
        assert out.columns[-2:] == ["_updated_ts", "_job_name"]
        assert out.collect()[0]["_job_name"] == "JOB_X"

    def test_gold_adds_gold_ts_and_job_name(self, spark) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        out = add_technical_cols(df, layer="gold", job_name="JOB_GOLD_X")
        assert out.columns[-2:] == ["_gold_ts", "_job_name"]
        assert out.collect()[0]["_job_name"] == "JOB_GOLD_X"

    def test_timestamp_columns_have_correct_type(self, spark) -> None:
        from pyspark.sql.types import TimestampType

        df = spark.createDataFrame([(1,)], ["id"])
        out = add_technical_cols(df, layer="silver", job_name="JOB_X")
        ts_field = next(f for f in out.schema.fields if f.name == "_updated_ts")
        assert isinstance(ts_field.dataType, TimestampType)


class TestCountPartitionsWritten:
    def test_three_distinct_dates(self, spark) -> None:
        df = spark.createDataFrame(
            [(1, "2025-01-01"), (2, "2025-01-02"), (3, "2025-01-03"), (4, "2025-01-01")],
            ["id", "date_arrete"],
        )
        assert count_partitions_written(df, ["date_arrete"]) == 3

    def test_no_partition_cols_returns_one(self, spark) -> None:
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        assert count_partitions_written(df, []) == 1

    def test_all_partition_cols_missing_returns_one(self, spark) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        assert count_partitions_written(df, ["date_arrete"]) == 1
