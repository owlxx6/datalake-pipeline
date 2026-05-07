"""Tests des modèles pydantic du module quality."""

from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from datalake_lib.quality import DQConfig, DQReport, DQResult, DQRule


# ── DQRule : champs requis selon rule_type ────────────────────────────────


class TestDQRuleShapes:
    """Vérifie que `check_rule_shape` valide la cohérence des champs."""

    def test_dq_rule_not_null_valid(self) -> None:
        rule = DQRule(
            rule_id="RUL_001",
            rule_type="not_null",
            columns=["ID"],
        )
        assert rule.severity == "blocking"
        assert rule.columns == ["ID"]

    def test_dq_rule_not_null_empty_columns_raises(self) -> None:
        with pytest.raises(ValidationError, match="not_null.*columns"):
            DQRule(rule_id="RUL_X", rule_type="not_null", columns=[])

    def test_dq_rule_unique_valid(self) -> None:
        rule = DQRule(rule_id="RUL_002", rule_type="unique", columns=["A", "B"])
        assert rule.columns == ["A", "B"]

    def test_dq_rule_referential_valid(self) -> None:
        rule = DQRule(
            rule_id="RUL_003",
            rule_type="referential",
            column="FK",
            reference_table="silver_other",
            reference_column="PK",
        )
        assert rule.reference_table == "silver_other"

    def test_dq_rule_referential_missing_reference_table_raises(self) -> None:
        with pytest.raises(ValidationError, match="reference_table"):
            DQRule(
                rule_id="RUL_X",
                rule_type="referential",
                column="FK",
                reference_column="PK",
            )

    def test_dq_rule_referential_missing_reference_column_raises(self) -> None:
        with pytest.raises(ValidationError, match="reference_column"):
            DQRule(
                rule_id="RUL_X",
                rule_type="referential",
                column="FK",
                reference_table="silver_other",
            )

    def test_dq_rule_referential_missing_column_raises(self) -> None:
        with pytest.raises(ValidationError, match="referential.*column"):
            DQRule(
                rule_id="RUL_X",
                rule_type="referential",
                reference_table="silver_other",
                reference_column="PK",
            )

    def test_dq_rule_regex_valid(self) -> None:
        rule = DQRule(
            rule_id="RUL_R",
            rule_type="regex",
            column="DATE_COL",
            pattern=r"^\d{4}-\d{2}-\d{2}$",
        )
        assert rule.pattern.startswith("^")

    def test_dq_rule_regex_missing_pattern_raises(self) -> None:
        with pytest.raises(ValidationError, match="regex.*pattern"):
            DQRule(rule_id="RUL_X", rule_type="regex", column="DATE_COL")

    def test_dq_rule_range_with_min_only(self) -> None:
        rule = DQRule(
            rule_id="RUL_5",
            rule_type="range",
            column="MNT",
            min_value=0,
        )
        assert rule.min_value == 0
        assert rule.max_value is None

    def test_dq_rule_range_without_bounds_raises(self) -> None:
        with pytest.raises(ValidationError, match="range.*min_value.*max_value"):
            DQRule(rule_id="RUL_X", rule_type="range", column="MNT")

    def test_dq_rule_in_set_valid(self) -> None:
        rule = DQRule(
            rule_id="RUL_S",
            rule_type="in_set",
            columns=["STATUT"],
            allowed_values=["A", "F", "C"],
        )
        assert rule.allowed_values == ["A", "F", "C"]

    def test_dq_rule_in_set_empty_allowed_values_raises(self) -> None:
        with pytest.raises(ValidationError, match="in_set.*allowed_values"):
            DQRule(
                rule_id="RUL_X",
                rule_type="in_set",
                columns=["STATUT"],
                allowed_values=[],
            )

    def test_dq_rule_custom_sql_valid(self) -> None:
        rule = DQRule(
            rule_id="RUL_Q",
            rule_type="custom_sql",
            sql_expr="SELECT * FROM __dq_input WHERE foo IS NULL",
        )
        assert "WHERE" in rule.sql_expr

    def test_dq_rule_custom_sql_missing_expr_raises(self) -> None:
        with pytest.raises(ValidationError, match="custom_sql.*sql_expr"):
            DQRule(rule_id="RUL_X", rule_type="custom_sql")


# ── DQConfig ──────────────────────────────────────────────────────────────


class TestDQConfig:
    def test_dq_config_valid(self) -> None:
        cfg = DQConfig(
            table_name="credit_amort",
            domain="credit",
            primary_key=["IDF_NUM_DOSS"],
            rules=[
                DQRule(rule_id="RUL_001", rule_type="not_null", columns=["IDF_NUM_DOSS"])
            ],
        )
        assert cfg.version == "1.0"
        assert cfg.primary_key == ["IDF_NUM_DOSS"]

    def test_dq_config_empty_rules_raises(self) -> None:
        with pytest.raises(ValidationError, match="au moins une règle"):
            DQConfig(table_name="x", domain="credit", rules=[])

    def test_dq_config_default_primary_key_empty(self) -> None:
        cfg = DQConfig(
            table_name="x",
            domain="credit",
            rules=[DQRule(rule_id="R", rule_type="not_null", columns=["A"])],
        )
        assert cfg.primary_key == []


# ── DQReport ──────────────────────────────────────────────────────────────


class TestDQReport:
    @staticmethod
    def _make_result(status: str, severity: str = "blocking") -> DQResult:
        return DQResult(
            rule_id="R",
            rule_type="not_null",
            status=status,
            records_checked=100,
            records_failed=5 if status != "passed" else 0,
            failure_rate=0.05 if status != "passed" else 0.0,
            severity=severity,
        )

    def test_dq_report_overall_status_failed_on_blocking_failure(self) -> None:
        report = DQReport(
            table_name="t",
            execution_date=datetime(2026, 4, 25, 6, 0, 0),
            total_rules=2,
            passed=1,
            failed=1,
            warnings=0,
            results=[
                self._make_result("passed"),
                self._make_result("failed", severity="blocking"),
            ],
            overall_status="failed",
        )
        assert report.overall_status == "failed"

    def test_dq_report_overall_status_passed_when_only_warnings(self) -> None:
        report = DQReport(
            table_name="t",
            execution_date=datetime(2026, 4, 25, 6, 0, 0),
            total_rules=2,
            passed=1,
            failed=0,
            warnings=1,
            results=[
                self._make_result("passed"),
                self._make_result("warning", severity="warning"),
            ],
            overall_status="passed",
        )
        assert report.overall_status == "passed"

    def test_dq_report_inconsistent_overall_status_raises(self) -> None:
        """Si un blocking failed existe, overall_status='passed' doit échouer."""
        with pytest.raises(ValidationError, match="overall_status incohérent"):
            DQReport(
                table_name="t",
                execution_date=datetime(2026, 4, 25, 6, 0, 0),
                total_rules=1,
                passed=0,
                failed=1,
                warnings=0,
                results=[self._make_result("failed", severity="blocking")],
                overall_status="passed",
            )

    def test_dq_report_failed_rule_with_warning_severity_does_not_block(self) -> None:
        """Une règle warning en status failed (impossible en pratique)
        ne doit pas faire échouer le rapport — seul blocking + failed bloque."""
        report = DQReport(
            table_name="t",
            execution_date=datetime(2026, 4, 25, 6, 0, 0),
            total_rules=1,
            passed=0,
            failed=1,
            warnings=0,
            results=[self._make_result("failed", severity="warning")],
            overall_status="passed",
        )
        assert report.overall_status == "passed"
