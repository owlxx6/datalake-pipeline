"""Fixtures partagées entre tous les tests.

`spark` : SparkSession locale, scope session, à demander par les tests
qui en ont besoin (read.bronze / read.landing / quality.engine). Les
tests qui n'utilisent pas Spark ne déclenchent pas la fixture.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark():
    """SparkSession locale partagée entre les tests.

    Si pyspark n'est pas installé, les tests demandant cette fixture
    sont automatiquement skippés.
    """
    pyspark = pytest.importorskip("pyspark")  # noqa: F841

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("datalake_lib_tests")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """Dossier des fixtures de test (CSV, etc.)."""
    return Path(__file__).resolve().parent / "fixtures"
