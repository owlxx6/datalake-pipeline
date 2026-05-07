"""Formatage console et tabulaire des JobLog."""

from __future__ import annotations

from .models import JobLog

# Icônes par statut.
_STATUS_ICONS = {
    "started": "⏳",
    "success": "✅",
    "failed": "❌",
    "partial": "⚠ ",
}

# Largeurs des colonnes pour le tableau de résumé.
_COL_WIDTHS = {
    "job": 32,
    "status": 9,
    "read": 10,
    "written": 10,
    "duration": 10,
}


def format_job_log(job_log: JobLog) -> str:
    """Formate un JobLog pour affichage console détaillé.

    Exemple :
        ══════════════════════════════════════════
        JOB_SILVER_CREDIT_AMORT — silver_credit_amort
        Layer : silver | Domain : credit
        ──────────────────────────────────────────
        Status   : ✅ SUCCESS
        Started  : 2026-04-25 06:00:00
        Finished : 2026-04-25 06:02:34
        Duration : 154.2s
        ──────────────────────────────────────────
        Records read     : 10,000
        Records written  : 9,950
        Records rejected : 50
        DQ Score         : 99.5%
        ──────────────────────────────────────────
        Input  : /data/bronze/credit_amort/
        Output : silver_credit_amort
        Partition : date_arrete=2026-04-25
        ══════════════════════════════════════════
    """
    sep_thick = "═" * 60
    sep_thin = "─" * 60
    icon = _STATUS_ICONS.get(job_log.status, "  ")

    lines: list[str] = [
        sep_thick,
        f"{job_log.job_name} — {job_log.table_name}",
        f"Layer : {job_log.layer} | Domain : {job_log.domain}",
        sep_thin,
        f"Status   : {icon} {job_log.status.upper()}",
        f"Started  : {job_log.started_at:%Y-%m-%d %H:%M:%S}",
    ]

    if job_log.finished_at is not None:
        lines.append(f"Finished : {job_log.finished_at:%Y-%m-%d %H:%M:%S}")
        lines.append(f"Duration : {job_log.metrics.duration_seconds:.1f}s")

    if job_log.error_message:
        lines.append(f"Error    : {job_log.error_message}")

    lines.append(sep_thin)

    m = job_log.metrics
    lines.extend(
        [
            f"Records read     : {m.records_read:,}",
            f"Records written  : {m.records_written:,}",
            f"Records rejected : {m.records_rejected:,}",
            f"DQ Score         : {m.dq_score:.1f}%",
        ]
    )

    if m.input_path or m.output_table or m.partition_values:
        lines.append(sep_thin)
        if m.input_path:
            lines.append(f"Input  : {m.input_path}")
        if m.output_table:
            lines.append(f"Output : {m.output_table}")
        if m.partition_values:
            partitions = ", ".join(
                f"{k}={v}" for k, v in m.partition_values.items()
            )
            lines.append(f"Partition : {partitions}")

    lines.append(sep_thick)
    return "\n".join(lines)


def format_job_summary(logs: list[JobLog]) -> str:
    """Formate plusieurs JobLog en tableau Unicode.

    Exemple :
        ┌─────────────────────────────┬─────────┬──────────┬──────────┬──────────┐
        │ Job                         │ Status  │ Read     │ Written  │ Duration │
        ├─────────────────────────────┼─────────┼──────────┼──────────┼──────────┤
        │ JOB_SILVER_CREDIT_AMORT     │ ✅ OK   │ 10,000   │ 9,950    │ 154.2s   │
        │ JOB_SILVER_IMPAYES_CDL      │ ✅ OK   │ 5,200    │ 5,200    │ 42.1s    │
        │ JOB_SILVER_DEPASSEMENT      │ ❌ FAIL │ 0        │ 0        │ 2.3s     │
        └─────────────────────────────┴─────────┴──────────┴──────────┴──────────┘
    """
    if not logs:
        return "(aucun job à afficher)"

    w = _COL_WIDTHS

    def _row(cells: list[str]) -> str:
        padded = [
            cells[0].ljust(w["job"]),
            cells[1].ljust(w["status"]),
            cells[2].rjust(w["read"]),
            cells[3].rjust(w["written"]),
            cells[4].rjust(w["duration"]),
        ]
        return "│ " + " │ ".join(padded) + " │"

    def _sep(left: str, mid: str, right: str) -> str:
        parts = [
            "─" * (w["job"] + 2),
            "─" * (w["status"] + 2),
            "─" * (w["read"] + 2),
            "─" * (w["written"] + 2),
            "─" * (w["duration"] + 2),
        ]
        return left + mid.join(parts) + right

    lines = [
        _sep("┌", "┬", "┐"),
        _row(["Job", "Status", "Read", "Written", "Duration"]),
        _sep("├", "┼", "┤"),
    ]

    for log in logs:
        icon = _STATUS_ICONS.get(log.status, "  ")
        status_label = {
            "started": "RUN",
            "success": "OK",
            "failed": "FAIL",
            "partial": "PART",
        }.get(log.status, log.status.upper())

        lines.append(
            _row(
                [
                    log.job_name,
                    f"{icon} {status_label}",
                    f"{log.metrics.records_read:,}",
                    f"{log.metrics.records_written:,}",
                    f"{log.metrics.duration_seconds:.1f}s",
                ]
            )
        )

    lines.append(_sep("└", "┴", "┘"))
    return "\n".join(lines)
