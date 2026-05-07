"""Tests du job silver_credit_encours — pipeline Bronze → Silver."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from datalake_lib.quality import DataQualityError
from jobs.silver.silver_credit_encours import SilverCreditEncours


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture()
def bronze_table(spark):
    """Table Bronze simulée pour le test end-to-end.

    Schéma proche du canevas EDD (toutes colonnes en string : on simule
    le CSV brut avant typage Silver).
    """
    from pyspark.sql import functions as F

    table = "bronze_credit_credit_encours"
    rows = [
        # NUM_CONTRAT, MNT_ACCORDE, MNT_RESTE, TX, DATE_DEB,    DATE_ECH,    STATUT,    date_arrete
        ("CTR-0001", "100000.00", "75000.00", "5.5", "01/01/2025", "31/12/2027", "ACTIF",       "2025-01-01"),
        ("CTR-0002", "250000.00", "180000.00", "4.2", "15/03/2025", "30/06/2028", "actif",       "2025-01-01"),
        ("CTR-0003", "50000.00",  "0.00",     "6.0", "01/01/2024", "15/09/2025", "SOLDE",       "2025-01-01"),
        ("CTR-0004", "1500000.00", "1490000.00", "3.8", "01/06/2025", "31/03/2030", "Contentieux", "2025-01-01"),
        ("CTR-0005", "75000.00",  "42500.00", "5.0", "01/02/2025", "31/12/2026", "DOUTEUX",     "2025-01-01"),
    ]
    df = spark.createDataFrame(
        rows,
        [
            "NUM_CONTRAT",
            "MONTANT_ACCORDE",
            "MONTANT_RESTANT_DU",
            "TAUX_INTERET",
            "DATE_DEBUT",
            "DATE_ECHEANCE",
            "CODE_STATUT",
            "date_arrete",
        ],
    ).withColumn("date_arrete", F.to_date("date_arrete"))

    spark.sql(f"DROP TABLE IF EXISTS {table}")
    df.write.mode("overwrite").saveAsTable(table)
    yield table
    spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture(autouse=True)
def cleanup_silver(spark):
    """Nettoie les tables Silver/rejets entre tests."""
    for tbl in (
        "silver_credit_credit_encours",
        "bronze_credit_encours_rejects",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    yield
    for tbl in (
        "silver_credit_credit_encours",
        "bronze_credit_encours_rejects",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")


@pytest.fixture()
def job(spark) -> SilverCreditEncours:
    return SilverCreditEncours(
        spark=spark,
        date_arrete=date(2025, 1, 1),
        env="dev",
    )


# ── Tests transformations unitaires ───────────────────────────────────────


class TestTransformations:
    def test_transform_cast_montant_to_double(self, spark, job) -> None:
        from pyspark.sql.types import DoubleType

        df = spark.createDataFrame(
            [("CTR-1", "100.50", "50.25", "5.0", "01/01/2025", "31/12/2027", "ACTIF")],
            [
                "NUM_CONTRAT",
                "MONTANT_ACCORDE",
                "MONTANT_RESTANT_DU",
                "TAUX_INTERET",
                "DATE_DEBUT",
                "DATE_ECHEANCE",
                "CODE_STATUT",
            ],
        )
        out = job._transform(df)

        for col in ("MONTANT_ACCORDE", "MONTANT_RESTANT_DU", "TAUX_INTERET"):
            field = next(f for f in out.schema.fields if f.name == col)
            assert isinstance(field.dataType, DoubleType)

    def test_transform_duree_restante_calculated(self, spark, job) -> None:
        df = spark.createDataFrame(
            [
                ("CTR-1", "100.0", "50.0", "5.0", "01/01/2025", "31/12/2099", "ACTIF"),
                ("CTR-2", "200.0", "100.0", "5.0", "01/01/2025", None,         "ACTIF"),
            ],
            [
                "NUM_CONTRAT",
                "MONTANT_ACCORDE",
                "MONTANT_RESTANT_DU",
                "TAUX_INTERET",
                "DATE_DEBUT",
                "DATE_ECHEANCE",
                "CODE_STATUT",
            ],
        )
        out = job._transform(df)

        rows = {r["NUM_CONTRAT"]: r["DUREE_RESTANTE_JOURS"] for r in out.collect()}
        # CTR-1 : date 2099 → durée largement positive.
        assert rows["CTR-1"] is not None
        assert rows["CTR-1"] > 20000  # ~25k jours jusqu'en 2099
        # CTR-2 : DATE_ECHEANCE null → DUREE_RESTANTE_JOURS null (pas d'erreur).
        assert rows["CTR-2"] is None

    def test_transform_statut_standardise(self, spark, job) -> None:
        df = spark.createDataFrame(
            [
                ("CTR-1", "100.0", "50.0", "5.0", "01/01/2025", "31/12/2027", "actif"),
                ("CTR-2", "100.0", "50.0", "5.0", "01/01/2025", "31/12/2027", "  SOLDE  "),
                ("CTR-3", "100.0", "50.0", "5.0", "01/01/2025", "31/12/2027", "DOUTEUX"),
            ],
            [
                "NUM_CONTRAT",
                "MONTANT_ACCORDE",
                "MONTANT_RESTANT_DU",
                "TAUX_INTERET",
                "DATE_DEBUT",
                "DATE_ECHEANCE",
                "CODE_STATUT",
            ],
        )
        out = job._transform(df)

        rows = {r["NUM_CONTRAT"]: r["CODE_STATUT"] for r in out.collect()}
        assert rows["CTR-1"] == "ACTIF"
        assert rows["CTR-2"] == "SOLDE"
        assert rows["CTR-3"] == "INCONNU"  # hors typologie


# ── Tests rejets et qualité ───────────────────────────────────────────────


class TestRejection:
    def test_reject_negative_montant(self, spark, job) -> None:
        df = spark.createDataFrame(
            [
                ("CTR-1", "100.0", "50.0",  "5.0", "01/01/2025", "31/12/2027", "ACTIF"),
                ("CTR-2", "100.0", "-100.0", "5.0", "01/01/2025", "31/12/2027", "ACTIF"),
            ],
            [
                "NUM_CONTRAT",
                "MONTANT_ACCORDE",
                "MONTANT_RESTANT_DU",
                "TAUX_INTERET",
                "DATE_DEBUT",
                "DATE_ECHEANCE",
                "CODE_STATUT",
            ],
        )
        out = job._transform(df)
        # Ligne CTR-2 rejetée.
        contracts = {r["NUM_CONTRAT"] for r in out.collect()}
        assert contracts == {"CTR-1"}


class TestQualityThreshold:
    def test_quality_seuil_bloquant_raises(self, spark, job) -> None:
        """Si > 10% des lignes sont en doublon (blocking unique) → DataQualityError."""
        rows = []
        # 5 lignes uniques + 5 doublons → 50% de "doublons" sur la PK NUM_CONTRAT.
        for i in range(5):
            rows.append(
                (
                    f"CTR-{i}", "100.0", "50.0", "5.0",
                    "01/01/2025", "31/12/2027", "ACTIF",
                )
            )
        for i in range(5):
            rows.append(
                (
                    f"CTR-{i}", "100.0", "50.0", "5.0",
                    "01/01/2025", "31/12/2027", "ACTIF",
                )
            )

        df = spark.createDataFrame(
            rows,
            [
                "NUM_CONTRAT",
                "MONTANT_ACCORDE",
                "MONTANT_RESTANT_DU",
                "TAUX_INTERET",
                "DATE_DEBUT",
                "DATE_ECHEANCE",
                "CODE_STATUT",
            ],
        )

        with pytest.raises(DataQualityError, match="seuil"):
            job._transform(df)


# ── Test end-to-end ───────────────────────────────────────────────────────


class TestRunEndToEnd:
    def test_run_creates_silver_table(
        self, spark, job, bronze_table, tmp_path
    ) -> None:
        # Rediriger AuditLogger vers tmp_path pour vérifier l'écriture.
        from datalake_lib.log.audit import AuditLogger

        job.logger = AuditLogger(output_path=str(tmp_path / "audit"))

        job.run()

        # Table Silver créée.
        assert spark.catalog.tableExists("silver_credit_credit_encours")

        # Contient les 5 lignes (aucune rejetée car aucun MONTANT négatif
        # dans les fixtures Bronze).
        loaded = spark.read.table("silver_credit_credit_encours")
        assert loaded.count() == 5

        # Colonnes techniques write présentes.
        assert "_updated_ts" in loaded.columns
        assert "_job_name" in loaded.columns

        # Audit JSON écrit.
        audit_files = list(
            (tmp_path / "audit" / "silver" / "silver_credit_credit_encours").glob(
                "*.json"
            )
        )
        assert len(audit_files) == 1

    def test_run_logs_failure_on_error(
        self, spark, job, tmp_path
    ) -> None:
        """Si Bronze absente, le job échoue mais log un audit failed."""
        from datalake_lib.log.audit import AuditLogger

        job.logger = AuditLogger(output_path=str(tmp_path / "audit"))

        # Pas de fixture bronze_table → la table n'existe pas.
        spark.sql("DROP TABLE IF EXISTS bronze_credit_credit_encours")

        with pytest.raises(Exception):
            job.run()

        # Le fichier d'audit doit exister malgré l'échec.
        audit_files = list(
            (tmp_path / "audit" / "silver" / "silver_credit_credit_encours").glob(
                "*.json"
            )
        )
        assert len(audit_files) == 1
        # Et son status doit être "failed".
        import json
        data = json.loads(audit_files[0].read_text())
        assert data["status"] == "failed"
