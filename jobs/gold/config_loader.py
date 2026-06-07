"""config_loader.py — modèles + loader YAML pour le Gold Engine déclaratif.

Le modèle dimensionnel (faits / dimensions / OBT) vit dans la config, pas dans le
code. Un changement de modèle (après validation métier) = éditer le YAML, zéro code.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel


class GoldTarget(BaseModel):
    """Une table cible du Gold. Les clés utilisées dépendent du `type`."""
    name: str
    type: str  # date | dimension | fact | obt

    # type=date
    range: Optional[Dict[str, str]] = None        # {from, to}

    # type=dimension
    source: Optional[str] = None                  # table Silver
    mode: Optional[str] = "scd2_current"          # scd2_current | scd2_versioned | snapshot
    key: Optional[List[str]] = None

    # commun dimension/fait : colonnes (str ou {src, as, role, ref})
    columns: Optional[List[Any]] = None

    # type=fact
    historisation: Optional[str] = "snapshot"     # snapshot (append date_arrete)
    grain: Optional[List[str]] = None
    joins: Optional[List[Dict[str, Any]]] = None  # gèle des attributs dim "as-of"
    filters: Optional[List[str]] = None

    # type=obt
    base: Optional[str] = None                    # fait colonne vertébrale
    aggregates: Optional[List[Dict[str, Any]]] = None
    derived: Optional[Dict[str, str]] = None


class GoldConfig(BaseModel):
    database: str
    targets: List[GoldTarget]


def load_gold_config(path: str) -> GoldConfig:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return GoldConfig(**data)
