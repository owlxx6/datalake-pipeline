"""Tests du runner Spark DQ.

Note : les vrais tests d'intégration s'exécutent SUR LA VM avec Spark.
Lancement attendu :
    cd /media/sf_datalake-pipeline
    pytest tests/quality/test_spark_dq_runner.py --spark -v

En local (sans Spark), on se contente de vérifier l'import et la
signature publique de SparkDQRunner. PySpark doit être installable
(pip install -e ".[spark]") pour que ces tests passent.
"""

from __future__ import annotations

import inspect

import pytest


def test_runner_module_imports() -> None:
    """L'import du runner doit fonctionner si pyspark est installé."""
    pyspark = pytest.importorskip("pyspark")
    assert pyspark is not None

    from datalake_lib.quality import SparkDQRunner  # noqa: F401


def test_runner_run_signature() -> None:
    """La méthode `run` doit accepter (df, config) et retourner un tuple."""
    pytest.importorskip("pyspark")

    from datalake_lib.quality import SparkDQRunner

    sig = inspect.signature(SparkDQRunner.run)
    params = list(sig.parameters)
    assert params == ["self", "df", "config"]


def test_runner_dispatch_covers_all_rule_types() -> None:
    """Vérifie que le runner expose une méthode `_check_*` pour chaque rule_type."""
    pytest.importorskip("pyspark")

    from datalake_lib.quality import SparkDQRunner
    from datalake_lib.quality.models.rule_models import DQRule

    expected_types = {
        t for t in DQRule.model_fields["rule_type"].annotation.__args__
    }

    methods = {
        name.replace("_check_", "")
        for name in dir(SparkDQRunner)
        if name.startswith("_check_")
    }
    assert methods == expected_types
