"""Lecture des fichiers YAML de règles DQ et conversion en DQConfig."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pydantic import ValidationError

from ..exceptions import DQConfigError
from ..models.rule_models import DQConfig

logger = logging.getLogger(__name__)


def _format_pydantic_errors(errors: list[dict], path: str) -> str:
    """Formate les erreurs pydantic en message français actionnable."""
    lines = [f"Erreur de validation DQ dans '{path}' :"]
    for err in errors:
        loc = " → ".join(str(x) for x in err["loc"])
        msg = err["msg"]
        lines.append(f"  - {loc} : {msg}")
    return "\n".join(lines)


def parse_dq_config(path: str | Path) -> DQConfig:
    """Charge et valide un fichier YAML de règles DQ.

    Args:
        path: Chemin vers le fichier .yaml.

    Returns:
        Instance validée de DQConfig.

    Raises:
        DQConfigError: Si le fichier n'existe pas, si la syntaxe YAML
            est invalide, ou si la validation pydantic échoue.
    """
    path = Path(path)

    if not path.exists():
        raise DQConfigError(
            f"Fichier de configuration DQ introuvable : '{path}'",
            path=str(path),
        )

    raw = path.read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise DQConfigError(
            f"Syntaxe YAML invalide dans '{path}' : {exc}",
            path=str(path),
        ) from exc

    if not isinstance(data, dict):
        raise DQConfigError(
            f"Le fichier '{path}' ne contient pas un mapping YAML valide.",
            path=str(path),
        )

    try:
        return DQConfig(**data)
    except ValidationError as exc:
        errors = exc.errors()
        message = _format_pydantic_errors(errors, str(path))
        raise DQConfigError(
            message,
            path=str(path),
            errors=errors,
        ) from exc


def parse_all_dq_configs(configs_dir: str | Path) -> dict[str, DQConfig]:
    """Charge et valide tous les YAML d'un dossier de règles DQ.

    Ignore les fichiers commençant par '_' (réservés aux annexes).
    Accumule toutes les erreurs au lieu de s'arrêter au premier fichier
    invalide.

    Args:
        configs_dir: Dossier contenant les fichiers .yaml.

    Returns:
        Dictionnaire {table_name: DQConfig}.

    Raises:
        DQConfigError: Si le dossier n'existe pas ou si au moins un
            fichier est invalide (toutes les erreurs sont agrégées).
    """
    configs_dir = Path(configs_dir)

    if not configs_dir.is_dir():
        raise DQConfigError(
            f"Dossier de configurations DQ introuvable : '{configs_dir}'",
            path=str(configs_dir),
        )

    results: dict[str, DQConfig] = {}
    all_errors: list[str] = []

    yaml_files = sorted(configs_dir.glob("*.yaml"))
    if not yaml_files:
        logger.warning("Aucun fichier .yaml trouvé dans '%s'", configs_dir)
        return results

    for yaml_path in yaml_files:
        if yaml_path.name.startswith("_"):
            continue

        try:
            config = parse_dq_config(yaml_path)
            results[config.table_name] = config
        except DQConfigError as exc:
            all_errors.append(str(exc))

    if all_errors:
        separator = "\n\n"
        raise DQConfigError(
            f"{len(all_errors)} fichier(s) DQ invalide(s) sur "
            f"{len(yaml_files)} :\n\n{separator.join(all_errors)}",
            errors=[{"files_in_error": len(all_errors)}],
        )

    logger.info(
        "%d configuration(s) DQ chargée(s) depuis '%s'",
        len(results),
        configs_dir,
    )
    return results
