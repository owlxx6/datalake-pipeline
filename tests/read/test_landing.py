"""Tests de LandingReader — lecture CSV depuis fixtures."""

from __future__ import annotations

from datetime import date

import pytest

pytest.importorskip("pyspark")

from datalake_lib.read import (
    JobConfig,
    LandingReader,
    PathResolutionError,
    ReadError,
    ReadOptions,
    SourceConfig,
)


# ── _resolve_path (méthode statique, pas besoin de Spark) ─────────────────


class TestResolvePath:
    def test_replaces_date_placeholder(self) -> None:
        path = LandingReader._resolve_path(
            "/landing/credit_{date_arrete}.csv",
            date(2025, 1, 1),
        )
        assert path == "/landing/credit_20250101.csv"

    def test_no_placeholder_returns_pattern_as_is(self) -> None:
        path = LandingReader._resolve_path("/landing/static.csv", date(2025, 1, 1))
        assert path == "/landing/static.csv"

    def test_no_date_no_placeholder_ok(self) -> None:
        path = LandingReader._resolve_path("/landing/static.csv", None)
        assert path == "/landing/static.csv"

    def test_placeholder_without_date_raises(self) -> None:
        with pytest.raises(PathResolutionError, match="date_arrete"):
            LandingReader._resolve_path(
                "/landing/credit_{date_arrete}.csv", None
            )


# ── read() avec fixtures CSV ──────────────────────────────────────────────


@pytest.fixture()
def credit_config(fixtures_dir) -> JobConfig:
    return JobConfig(
        domain="credit",
        table="credit_encours",
        layer="bronze",
        source_system="evolan",
        source=SourceConfig(
            path_pattern=str(fixtures_dir / "csv" / "credit_encours_{date_arrete}.csv"),
            delimiter=";",
            has_header=True,
            encoding="UTF-8",
        ),
    )


class TestRead:
    def test_read_returns_non_empty_df(self, spark, credit_config) -> None:
        reader = LandingReader(spark, credit_config)
        df = reader.read(ReadOptions(filter_date=date(2025, 1, 1)))
        assert df.count() == 5

    def test_read_includes_audit_cols(self, spark, credit_config) -> None:
        reader = LandingReader(spark, credit_config)
        df = reader.read(ReadOptions(filter_date=date(2025, 1, 1)))
        for col in ["_source_file_name", "_ingestion_ts", "_source_system", "_layer"]:
            assert col in df.columns

    def test_read_applies_projection(self, spark, credit_config) -> None:
        reader = LandingReader(spark, credit_config)
        df = reader.read(
            ReadOptions(
                filter_date=date(2025, 1, 1),
                columns=["NUM_CONTRAT", "MONTANT_ACCORDE"],
            )
        )
        assert set(df.columns) == {"NUM_CONTRAT", "MONTANT_ACCORDE"}

    def test_read_applies_limit(self, spark, credit_config) -> None:
        reader = LandingReader(spark, credit_config)
        df = reader.read(
            ReadOptions(filter_date=date(2025, 1, 1), limit=2)
        )
        assert df.count() == 2

    def test_read_missing_path_raises(self, spark, fixtures_dir) -> None:
        cfg = JobConfig(
            domain="credit",
            table="ghost",
            source=SourceConfig(
                path_pattern=str(fixtures_dir / "csv" / "ghost_{date_arrete}.csv"),
            ),
        )
        reader = LandingReader(spark, cfg)
        with pytest.raises(ReadError):
            df = reader.read(ReadOptions(filter_date=date(2025, 1, 1)))
            df.count()  # forcer l'évaluation Spark

    def test_init_without_source_raises(self, spark) -> None:
        cfg = JobConfig(domain="credit", table="x")  # source=None
        with pytest.raises(ReadError, match="source"):
            LandingReader(spark, cfg)


# ── _detect_encoding (stub POC) ───────────────────────────────────────────


class TestDetectEncoding:
    def test_returns_utf8_by_default(self) -> None:
        assert LandingReader._detect_encoding("/some/path.csv") == "UTF-8"
