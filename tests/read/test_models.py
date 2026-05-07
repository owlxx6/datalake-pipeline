"""Tests des modèles JobConfig / ReadOptions / ReadResult — sans Spark."""

from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from datalake_lib.read import (
    JobConfig,
    ReadOptions,
    ReadResult,
    SourceConfig,
)


class TestReadOptionsDefaults:
    def test_defaults(self) -> None:
        opts = ReadOptions()
        assert opts.filter_date is None
        assert opts.columns == []
        assert opts.limit is None
        assert opts.coalesce is None
        assert opts.encoding == ""

    def test_with_values(self) -> None:
        opts = ReadOptions(
            filter_date=date(2026, 4, 25),
            columns=["A", "B"],
            limit=100,
            coalesce=4,
            encoding="ISO-8859-1",
        )
        assert opts.filter_date == date(2026, 4, 25)
        assert opts.columns == ["A", "B"]
        assert opts.limit == 100
        assert opts.coalesce == 4
        assert opts.encoding == "ISO-8859-1"


class TestJobConfig:
    def test_minimal(self) -> None:
        cfg = JobConfig(domain="credit", table="credit_amort")
        assert cfg.domain == "credit"
        assert cfg.table == "credit_amort"
        assert cfg.layer == "bronze"
        assert cfg.source_system == ""
        assert cfg.source is None

    def test_with_source(self) -> None:
        cfg = JobConfig(
            domain="credit",
            table="credit_encours",
            source_system="evolan",
            source=SourceConfig(
                path_pattern="/landing/credit_{date_arrete}.csv",
                delimiter=";",
                has_header=True,
            ),
        )
        assert cfg.source is not None
        assert cfg.source.path_pattern == "/landing/credit_{date_arrete}.csv"
        assert cfg.source.delimiter == ";"
        assert cfg.source.encoding == "UTF-8"

    def test_invalid_layer(self) -> None:
        with pytest.raises(ValidationError):
            JobConfig(domain="credit", table="x", layer="silvr")  # typo


class TestReadResult:
    def test_accepts_expected_types(self) -> None:
        result = ReadResult(
            table="bronze_credit_credit_amort",
            row_count=10000,
            columns=["A", "B", "C"],
            partition_date=date(2026, 4, 25),
            source_path="bronze_credit_credit_amort",
            read_duration_seconds=1.234,
        )
        assert result.row_count == 10000
        assert result.partition_date == date(2026, 4, 25)
        assert result.read_duration_seconds == pytest.approx(1.234)

    def test_partition_date_optional(self) -> None:
        result = ReadResult(
            table="bronze_x",
            row_count=0,
            columns=[],
            source_path="bronze_x",
            read_duration_seconds=0.0,
        )
        assert result.partition_date is None
