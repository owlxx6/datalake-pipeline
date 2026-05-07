"""Tests de utils.py — nécessite SparkSession (fixture spark)."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from datalake_lib.read import add_audit_cols, cast_columns, infer_schema


# ── add_audit_cols ────────────────────────────────────────────────────────


class TestAddAuditCols:
    def test_adds_three_columns_at_end(self, spark) -> None:
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])

        out = add_audit_cols(df, source_system="evolan", layer="bronze")

        # Les 3 colonnes d'audit doivent être en fin.
        assert out.columns[-3:] == ["_ingestion_ts", "_source_system", "_layer"]
        # Les colonnes d'origine sont préservées.
        assert "id" in out.columns
        assert "label" in out.columns

    def test_ingestion_ts_is_timestamp_type(self, spark) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        out = add_audit_cols(df, "evolan", "bronze")
        ts_field = next(f for f in out.schema.fields if f.name == "_ingestion_ts")
        assert isinstance(ts_field.dataType, TimestampType)

    def test_source_system_and_layer_are_literals(self, spark) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        out = add_audit_cols(df, source_system="garweb", layer="silver")
        row = out.collect()[0]
        assert row["_source_system"] == "garweb"
        assert row["_layer"] == "silver"


# ── cast_columns ──────────────────────────────────────────────────────────


class TestCastColumns:
    def test_cast_string_to_double(self, spark) -> None:
        df = spark.createDataFrame(
            [("1", "100.5"), ("2", "200.0")],
            ["id", "montant"],
        )
        out = cast_columns(df, {"montant": "double"})

        montant_field = next(f for f in out.schema.fields if f.name == "montant")
        assert isinstance(montant_field.dataType, DoubleType)

        # Valeurs converties.
        rows = out.collect()
        assert rows[0]["montant"] == pytest.approx(100.5)

    def test_cast_string_to_int(self, spark) -> None:
        df = spark.createDataFrame([("42",)], ["nb"])
        out = cast_columns(df, {"nb": "int"})
        assert isinstance(
            next(f for f in out.schema.fields if f.name == "nb").dataType,
            IntegerType,
        )

    def test_skip_missing_column_silently(self, spark) -> None:
        df = spark.createDataFrame([("1",)], ["id"])
        out = cast_columns(df, {"missing_col": "int", "id": "int"})

        # 'missing_col' ignoré, 'id' casté.
        assert "missing_col" not in out.columns
        assert isinstance(
            next(f for f in out.schema.fields if f.name == "id").dataType,
            IntegerType,
        )

    def test_invalid_cast_keeps_column(self, spark, caplog) -> None:
        df = spark.createDataFrame([("not_a_number",)], ["x"])

        # Type 'banana' inexistant — la colonne reste StringType, warning loggé.
        with caplog.at_level("WARNING", logger="datalake_lib.read.utils"):
            out = cast_columns(df, {"x": "banana"})

        x_field = next(f for f in out.schema.fields if f.name == "x")
        assert isinstance(x_field.dataType, StringType)


# ── infer_schema ──────────────────────────────────────────────────────────


class TestInferSchema:
    def test_returns_correct_dict(self, spark) -> None:
        df = spark.createDataFrame(
            [(1, "a", 1.5)],
            ["id", "label", "score"],
        )
        schema = infer_schema(df)

        assert schema == {
            "id": "bigint",
            "label": "string",
            "score": "double",
        }

    def test_empty_dataframe_returns_schema_only(self, spark) -> None:
        from pyspark.sql.types import StringType, StructField, StructType

        struct = StructType([StructField("col", StringType(), True)])
        df = spark.createDataFrame([], struct)
        assert infer_schema(df) == {"col": "string"}
