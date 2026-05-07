"""Tests du module datalake_lib.config — chargement et validation YAML."""

from __future__ import annotations

import logging
import textwrap
from pathlib import Path

import pytest

from datalake_lib.config import (
    ConfigNotFoundError,
    ConfigValidationError,
    TableConfig,
    load_all_configs,
    load_config,
)

CONFIGS_DIR = Path(__file__).resolve().parents[2] / "dags" / "configs"

# ── Fixtures ────────────────────────────────────────────────────────────────


VALID_YAML = textwrap.dedent("""\
    metadata:
      generated_from: canevas/TEST.xlsx
      sheet_name: TEST
      generated_at: '2026-04-14T08:00:00+00:00'
    table_name: TEST_TABLE
    domain: credit
    description: Table de test
    bronze:
      ingestion_tool: nifi
      landing_path: /data/bronze/test_table/
      table: bronze_test_table
      storage: hive
      format: csv
      delimiter: ';'
      partition_by: date_arrete
      mode: append
    silver:
      table: silver_test_table
      storage: iceberg
      primary_key: [ID]
      write_mode: merge
      partition_by: date_arrete
      schema:
        - name: ID
          type: StringType
          nullable: false
          description: Identifiant
      quality_rules:
        - rule: not_null
          columns: [ID]
    gold:
      feeds: []
    schedule: '0 6 * * *'
    owner: eai-credit-garantie
    frequency: Quotidien
    storage_type: STOCK
""")


@pytest.fixture()
def valid_yaml_file(tmp_path: Path) -> Path:
    f = tmp_path / "test_table.yaml"
    f.write_text(VALID_YAML)
    return f


@pytest.fixture()
def invalid_syntax_file(tmp_path: Path) -> Path:
    f = tmp_path / "broken.yaml"
    f.write_text("table_name: [\ninvalid yaml content")
    return f


# ── Tests unitaires ─────────────────────────────────────────────────────────


class TestLoadConfig:
    """Tests de load_config()."""

    def test_load_valid_config(self, valid_yaml_file: Path) -> None:
        cfg = load_config(valid_yaml_file)
        assert cfg.table_name == "TEST_TABLE"
        assert cfg.domain == "credit"
        assert cfg.bronze.table == "bronze_test_table"
        assert cfg.silver.table == "silver_test_table"
        assert cfg.silver.storage == "iceberg"
        assert cfg.silver.primary_key == ["ID"]
        assert len(cfg.silver.schema_) == 1

    def test_load_missing_file(self) -> None:
        with pytest.raises(ConfigNotFoundError, match="introuvable"):
            load_config("/nonexistent/path/ghost.yaml")

    def test_load_invalid_yaml_syntax(self, invalid_syntax_file: Path) -> None:
        with pytest.raises(ConfigValidationError, match="Syntaxe YAML invalide"):
            load_config(invalid_syntax_file)

    def test_missing_required_field(self, tmp_path: Path) -> None:
        """YAML valide syntaxiquement mais sans table_name."""
        content = VALID_YAML.replace("table_name: TEST_TABLE\n", "")
        f = tmp_path / "no_table_name.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="table_name"):
            load_config(f)

    def test_invalid_domain(self, tmp_path: Path) -> None:
        content = VALID_YAML.replace("domain: credit", "domain: bancassurance")
        f = tmp_path / "bad_domain.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="bancassurance.*non reconnu"):
            load_config(f)

    def test_invalid_write_mode(self, tmp_path: Path) -> None:
        content = VALID_YAML.replace("write_mode: merge", "write_mode: upsert")
        f = tmp_path / "bad_write_mode.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="write_mode"):
            load_config(f)

    def test_invalid_schedule(self, tmp_path: Path) -> None:
        content = VALID_YAML.replace("schedule: '0 6 * * *'", "schedule: '0 6 * *'")
        f = tmp_path / "bad_schedule.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="cron valide"):
            load_config(f)

    def test_bronze_silver_table_coherence(self, tmp_path: Path) -> None:
        content = VALID_YAML.replace(
            "table: bronze_test_table", "table: bronze_wrong_name"
        )
        f = tmp_path / "incoherent.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="Incohérence de nommage"):
            load_config(f)


class TestPrimaryKeyWarning:
    """Vérifie que l'absence de PK émet un warning sans lever d'exception."""

    def test_primary_key_empty_warns(self, tmp_path: Path, caplog) -> None:
        content = VALID_YAML.replace("primary_key: [ID]", "primary_key: []")
        f = tmp_path / "no_pk.yaml"
        f.write_text(content)

        with caplog.at_level(logging.WARNING, logger="datalake_lib.config.models"):
            cfg = load_config(f)

        assert cfg.silver.primary_key == []
        assert "aucune clé primaire" in caplog.text


class TestReferentialRule:
    """Vérifie le support de la règle de qualité 'referential'."""

    BASE_REFERENTIAL_YAML = textwrap.dedent("""\
        metadata:
          generated_from: canevas/TEST.xlsx
          sheet_name: TEST
          generated_at: '2026-04-14T08:00:00+00:00'
        table_name: TEST_TABLE
        domain: credit
        description: Table de test
        bronze:
          ingestion_tool: nifi
          landing_path: /data/bronze/test_table/
          table: bronze_test_table
          storage: hive
          format: csv
          delimiter: ';'
          partition_by: date_arrete
          mode: append
        silver:
          table: silver_test_table
          storage: iceberg
          primary_key: [ID]
          write_mode: merge
          partition_by: date_arrete
          schema:
            - name: ID
              type: StringType
              nullable: false
              description: Identifiant
            - name: FK_COMPTE
              type: StringType
              nullable: true
              description: FK
          quality_rules:
            - rule: not_null
              columns: [ID]
            - rule: referential
              column: FK_COMPTE
              reference_table: silver_image_contrat
              reference_column: IDF_REF_EXT_CTR
        gold:
          feeds: []
        schedule: '0 6 * * *'
        owner: eai-credit-garantie
        frequency: Quotidien
        storage_type: STOCK
    """)

    def test_referential_rule_valid(self, tmp_path: Path) -> None:
        f = tmp_path / "test_referential.yaml"
        f.write_text(self.BASE_REFERENTIAL_YAML)

        cfg = load_config(f)
        rules = cfg.silver.quality_rules
        assert len(rules) == 2
        ref = next(r for r in rules if r.rule == "referential")
        assert ref.column == "FK_COMPTE"
        assert ref.reference_table == "silver_image_contrat"
        assert ref.reference_column == "IDF_REF_EXT_CTR"

    def test_referential_rule_missing_column(self, tmp_path: Path) -> None:
        content = self.BASE_REFERENTIAL_YAML.replace(
            "              column: FK_COMPTE\n", ""
        )
        f = tmp_path / "missing_column.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="rule=referential.*column"):
            load_config(f)

    def test_referential_rule_missing_reference_table(self, tmp_path: Path) -> None:
        content = self.BASE_REFERENTIAL_YAML.replace(
            "              reference_table: silver_image_contrat\n", ""
        )
        f = tmp_path / "missing_ref_table.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="rule=referential.*reference_table"):
            load_config(f)

    def test_referential_rule_missing_reference_column(self, tmp_path: Path) -> None:
        content = self.BASE_REFERENTIAL_YAML.replace(
            "              reference_column: IDF_REF_EXT_CTR\n", ""
        )
        f = tmp_path / "missing_ref_column.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="rule=referential.*reference_column"):
            load_config(f)

    def test_non_referential_rule_requires_columns(self, tmp_path: Path) -> None:
        """Une règle non-referential sans 'columns' doit échouer."""
        content = self.BASE_REFERENTIAL_YAML.replace(
            "            - rule: not_null\n              columns: [ID]\n",
            "            - rule: not_null\n              columns: []\n",
        )
        f = tmp_path / "empty_columns.yaml"
        f.write_text(content)

        with pytest.raises(ConfigValidationError, match="rule=not_null.*columns"):
            load_config(f)


class TestLoadAllConfigs:
    """Tests de load_all_configs()."""

    def test_load_all_configs(self) -> None:
        """Charge les 30 YAML réels de dags/configs/ — tous doivent être valides."""
        if not CONFIGS_DIR.exists():
            pytest.skip("dags/configs/ introuvable (exécution hors projet)")

        configs = load_all_configs(CONFIGS_DIR)
        assert len(configs) == 30
        for name, cfg in configs.items():
            assert isinstance(cfg, TableConfig)
            assert cfg.table_name == name

    def test_load_all_missing_dir(self) -> None:
        with pytest.raises(ConfigNotFoundError):
            load_all_configs("/nonexistent/dir")

    def test_load_all_aggregates_errors(self, tmp_path: Path) -> None:
        """Deux fichiers invalides → erreur agrégée mentionnant les deux."""
        (tmp_path / "a.yaml").write_text(VALID_YAML.replace("domain: credit", "domain: xxx"))
        (tmp_path / "b.yaml").write_text(VALID_YAML.replace("domain: credit", "domain: yyy"))

        with pytest.raises(ConfigValidationError, match="2 fichier.*invalide"):
            load_all_configs(tmp_path)


class TestRealYamlFiles:
    """Tests sur les YAML réels du projet."""

    def test_credit_amort(self) -> None:
        f = CONFIGS_DIR / "credit_amort.yaml"
        if not f.exists():
            pytest.skip("credit_amort.yaml introuvable")

        cfg = load_config(f)
        assert cfg.table_name == "CREDIT_AMORT"
        assert cfg.domain == "credit"
        assert cfg.silver.primary_key == ["IDF_NUM_DOSS"]
        assert len(cfg.silver.schema_) > 50

    def test_depassement_has_composite_pk(self) -> None:
        """Depuis le diagramme : PK composite [NUM_COMPTE, DATE_ENTREE, COMPTE_GENERAL, MNT_DEPASSEMENT]."""
        f = CONFIGS_DIR / "depassement.yaml"
        if not f.exists():
            pytest.skip("depassement.yaml introuvable")

        cfg = load_config(f)
        assert cfg.silver.primary_key == [
            "NUM_COMPTE",
            "DATE_ENTREE",
            "COMPTE_GENERAL",
            "MNT_DEPASSEMENT",
        ]

    def test_nevolan_ta_warns_no_pk(self, caplog) -> None:
        f = CONFIGS_DIR / "nevolan_ta.yaml"
        if not f.exists():
            pytest.skip("nevolan_ta.yaml introuvable")

        with caplog.at_level(logging.WARNING, logger="datalake_lib.config.models"):
            cfg = load_config(f)

        assert cfg.silver.primary_key == []
        assert "aucune clé primaire" in caplog.text

    def test_credit_amort_has_referential_rules(self) -> None:
        """credit_amort doit déclarer 2 jointures référentielles validées par le diagramme."""
        f = CONFIGS_DIR / "credit_amort.yaml"
        if not f.exists():
            pytest.skip("credit_amort.yaml introuvable")

        cfg = load_config(f)
        ref_rules = [r for r in cfg.silver.quality_rules if r.rule == "referential"]
        assert len(ref_rules) == 2

        targets = {(r.column, r.reference_table, r.reference_column) for r in ref_rules}
        assert ("NUM_COMPTE_EMPRUNTEUR", "silver_image_contrat", "IDF_REF_EXT_CTR") in targets
        assert ("COD_PRODUIT", "silver_pops", "IDF_CODE_PRODUIT") in targets
