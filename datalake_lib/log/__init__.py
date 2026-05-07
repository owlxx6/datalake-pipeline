"""Module Log — audit et suivi des jobs du pipeline médaillon.

Architecture :
- `models.py`     : JobLog, JobMetrics, JobStatus
- `audit.py`      : AuditLogger (cycle de vie + écriture JSON)
- `metrics.py`    : MetricsCollector (chronomètre + builder)
- `formatters.py` : format_job_log (détail), format_job_summary (tableau)

Usage typique dans un job Spark :

    from datalake_lib.log import AuditLogger, MetricsCollector

    audit = AuditLogger()
    log = audit.start("JOB_SILVER_CREDIT_AMORT", "silver_credit_amort",
                       "silver", "credit")
    collector = MetricsCollector()
    try:
        # ... traitement ...
        collector.set_records_read(10000)
        collector.set_records_written(9950)
        audit.success(log, collector.build())
    except Exception as exc:
        audit.fail(log, str(exc), collector.build())
"""

from __future__ import annotations

from .audit import AuditLogger
from .formatters import format_job_log, format_job_summary
from .metrics import MetricsCollector
from .models import JobLog, JobMetrics, JobStatus

__all__ = [
    "AuditLogger",
    "JobLog",
    "JobMetrics",
    "JobStatus",
    "MetricsCollector",
    "format_job_log",
    "format_job_summary",
]
