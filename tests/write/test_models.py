"""Tests des modèles WriteOptions / WriteResult — sans Spark."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from datalake_lib.write import WriteOptions, WriteResult


class TestWriteOptionsDefaults:
    def test_defaults(self) -> None:
        opts = WriteOptions()
        assert opts.mode == "full"
        assert opts.merge_keys == []
        assert opts.partition_by == ["date_arrete"]
        assert opts.coalesce == 1
        assert opts.overwrite_schema is False
        assert opts.dry_run is False

    def test_with_values(self) -> None:
        opts = WriteOptions(
            mode="incremental",
            merge_keys=["IDF_NUM_DOSS"],
            partition_by=["date_arrete", "domain"],
            coalesce=4,
            overwrite_schema=True,
            dry_run=True,
        )
        assert opts.mode == "incremental"
        assert opts.merge_keys == ["IDF_NUM_DOSS"]
        assert opts.partition_by == ["date_arrete", "domain"]
        assert opts.coalesce == 4
        assert opts.overwrite_schema is True
        assert opts.dry_run is True

    def test_invalid_mode_raises(self) -> None:
        with pytest.raises(ValidationError):
            WriteOptions(mode="upsert")  # non supporté

    def test_partition_by_independent_default(self) -> None:
        """Pas de partage de liste entre instances."""
        a = WriteOptions()
        b = WriteOptions()
        a.partition_by.append("x")
        assert b.partition_by == ["date_arrete"]


class TestWriteResult:
    def test_accepts_expected_types(self) -> None:
        result = WriteResult(
            table="silver_credit_credit_amort",
            layer="silver",
            mode="full",
            rows_written=10000,
            partitions_written=1,
            write_duration_seconds=12.5,
            target_path="silver_credit_credit_amort",
        )
        assert result.layer == "silver"
        assert result.rows_written == 10000

    def test_layer_must_be_silver_or_gold(self) -> None:
        with pytest.raises(ValidationError):
            WriteResult(
                table="x",
                layer="bronze",  # interdit pour Write
                mode="full",
                rows_written=0,
                partitions_written=0,
                write_duration_seconds=0.0,
                target_path="x",
            )
