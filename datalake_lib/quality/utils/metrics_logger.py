"""Logging des métriques DQ : rapport JSON + format console."""

from __future__ import annotations

import logging
from pathlib import Path

from ..models.rule_models import DQReport, DQResult

logger = logging.getLogger(__name__)

# Icônes pour le rapport console.
_STATUS_ICONS = {
    "passed": "✅",
    "failed": "❌",
    "warning": "⚠ ",
}


def log_dq_report(report: DQReport, output_path: str | Path) -> Path:
    """Écrit le rapport DQ au format JSON.

    Le fichier est nommé `{table_name}_{execution_date}.json` et placé
    dans `output_path/`. Le dossier est créé si nécessaire.

    Args:
        report: Rapport à sérialiser.
        output_path: Dossier de destination (ex: `/data/audit/dq_reports/`).

    Returns:
        Chemin du fichier JSON écrit.
    """
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = report.execution_date.strftime("%Y%m%d_%H%M%S")
    file_path = output_path / f"{report.table_name}_{timestamp}.json"
    file_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    logger.info("Rapport DQ écrit dans '%s'", file_path)
    return file_path


def format_console_report(report: DQReport) -> str:
    """Formate le rapport pour affichage console.

    Exemple de sortie :
        ══════════════════════════════════
        DQ Report — silver_credit_amort
        Date : 2026-04-25
        ──────────────────────────────────
        ✅ RUL_001 not_null [IDF_NUM_DOSS]     PASSED  0 rejects
        ❌ RUL_003 referential [COD_PRODUIT]   FAILED  23 rejects (0.5%)
        ⚠  RUL_004 range [MNT_CAPITAL_REST_DU] WARNING 2 rejects (0.04%)
        ──────────────────────────────────
        Total : 4 rules | 2 passed | 1 failed | 1 warning
        Status : FAILED
        ══════════════════════════════════
    """
    sep_thick = "═" * 60
    sep_thin = "─" * 60

    lines: list[str] = [
        sep_thick,
        f"DQ Report — {report.table_name}",
        f"Date : {report.execution_date:%Y-%m-%d %H:%M:%S}",
        sep_thin,
    ]

    for result in report.results:
        lines.append(_format_result_line(result))

    lines.append(sep_thin)
    lines.append(
        f"Total : {report.total_rules} rules | "
        f"{report.passed} passed | "
        f"{report.failed} failed | "
        f"{report.warnings} warning"
    )
    lines.append(f"Status : {report.overall_status.upper()}")
    lines.append(sep_thick)

    return "\n".join(lines)


def _format_result_line(result: DQResult) -> str:
    """Formate une ligne de résultat (1 règle)."""
    icon = _STATUS_ICONS.get(result.status, "  ")
    rate_str = f" ({result.failure_rate:.2%})" if result.records_failed > 0 else ""
    return (
        f"{icon} {result.rule_id} {result.rule_type:<14} "
        f"{result.status.upper():<8} "
        f"{result.records_failed} rejects{rate_str}"
    )
