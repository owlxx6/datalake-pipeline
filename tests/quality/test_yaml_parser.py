"""Tests du parser YAML → DQConfig."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from datalake_lib.quality import (
    DQConfig,
    DQConfigError,
    parse_all_dq_configs,
    parse_dq_config,
)

DQ_CONFIGS_DIR = (
    Path(__file__).resolve().parents[2]
    / "datalake_lib"
    / "quality"
    / "dq_configs"
)


VALID_DQ_YAML = textwrap.dedent("""\
    table_name: test_table
    domain: credit
    version: "1.0"
    primary_key: [ID]
    rules:
      - rule_id: RUL_001
        rule_type: not_null
        columns: [ID]
        severity: blocking
        description: PK
""")


class TestParseDQConfig:
    def test_parse_valid(self, tmp_path: Path) -> None:
        f = tmp_path / "ok.yaml"
        f.write_text(VALID_DQ_YAML)

        cfg = parse_dq_config(f)
        assert isinstance(cfg, DQConfig)
        assert cfg.table_name == "test_table"
        assert cfg.primary_key == ["ID"]
        assert len(cfg.rules) == 1
        assert cfg.rules[0].rule_id == "RUL_001"

    def test_parse_missing_file_raises(self) -> None:
        with pytest.raises(DQConfigError, match="introuvable"):
            parse_dq_config("/nonexistent/dq.yaml")

    def test_parse_invalid_yaml_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "broken.yaml"
        f.write_text("rules: [\nbad yaml")
        with pytest.raises(DQConfigError, match="Syntaxe YAML invalide"):
            parse_dq_config(f)

    def test_parse_yaml_not_a_mapping_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "list.yaml"
        f.write_text("- a\n- b\n")
        with pytest.raises(DQConfigError, match="mapping YAML"):
            parse_dq_config(f)

    def test_parse_invalid_rule_type_raises(self, tmp_path: Path) -> None:
        content = VALID_DQ_YAML.replace("rule_type: not_null", "rule_type: bogus")
        f = tmp_path / "bad_rule.yaml"
        f.write_text(content)
        with pytest.raises(DQConfigError, match="Erreur de validation DQ"):
            parse_dq_config(f)

    def test_parse_credit_amort_dq(self) -> None:
        """Parse l'exemple métier livré avec le projet."""
        f = DQ_CONFIGS_DIR / "credit_amort_dq.yaml"
        if not f.exists():
            pytest.skip("credit_amort_dq.yaml introuvable")

        cfg = parse_dq_config(f)
        assert cfg.table_name == "credit_amort"
        assert cfg.domain == "credit"
        assert cfg.primary_key == ["IDF_NUM_DOSS"]
        assert len(cfg.rules) == 6

        rule_ids = [r.rule_id for r in cfg.rules]
        assert rule_ids == [
            "RUL_001",
            "RUL_002",
            "RUL_003",
            "RUL_004",
            "RUL_005",
            "RUL_006",
        ]

        # Vérifier qu'on a bien les 2 règles referential vers IMAGE_CONTRAT et POPS.
        ref_rules = [r for r in cfg.rules if r.rule_type == "referential"]
        assert len(ref_rules) == 2
        ref_targets = {(r.column, r.reference_table) for r in ref_rules}
        assert ("NUM_COMPTE_EMPRUNTEUR", "silver_image_contrat") in ref_targets
        assert ("COD_PRODUIT", "silver_pops") in ref_targets


class TestParseAllDQConfigs:
    def test_parse_all_real_dir(self) -> None:
        if not DQ_CONFIGS_DIR.exists():
            pytest.skip("dq_configs/ introuvable")

        configs = parse_all_dq_configs(DQ_CONFIGS_DIR)
        assert "credit_amort" in configs
        assert isinstance(configs["credit_amort"], DQConfig)

    def test_parse_all_missing_dir_raises(self) -> None:
        with pytest.raises(DQConfigError, match="introuvable"):
            parse_all_dq_configs("/nonexistent/dq_configs")

    def test_parse_all_skips_underscore_files(self, tmp_path: Path) -> None:
        (tmp_path / "_summary.yaml").write_text("table_name: ignore\n")
        (tmp_path / "ok.yaml").write_text(VALID_DQ_YAML)

        configs = parse_all_dq_configs(tmp_path)
        assert list(configs.keys()) == ["test_table"]

    def test_parse_all_aggregates_errors(self, tmp_path: Path) -> None:
        bad_yaml = VALID_DQ_YAML.replace("rule_type: not_null", "rule_type: ghost")
        (tmp_path / "a.yaml").write_text(bad_yaml)
        (tmp_path / "b.yaml").write_text(bad_yaml)

        with pytest.raises(DQConfigError, match="2 fichier.*invalide"):
            parse_all_dq_configs(tmp_path)
