"""Jobs Spark exécutables — Bronze, Silver, Gold.

Chaque job hérite de `BaseJob` et expose une méthode `run()` orchestrant
le pipeline complet (extract → transform → load) en s'appuyant sur les
modules de `datalake_lib`.
"""

from __future__ import annotations

from .base_job import BaseJob

__all__ = ["BaseJob"]
