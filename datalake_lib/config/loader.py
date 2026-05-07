"""Chargement et validation des fichiers de configuration YAML."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pydantic import ValidationError

from .exceptions import ConfigNotFoundError, ConfigValidationError
from .models import TableConfig

logger = logging.getLogger(__name__)


def _format_pydantic_errors(errors: list[dict], path: str) -> str:
    """Formate les erreurs pydantic en message français actionnable."""
    lines = [f"Erreur de validation dans '{path}' :"]
    for err in errors:
        loc = " → ".join(str(x) for x in err["loc"])
        msg = err["msg"]
        lines.append(f"  - {loc} : {msg}")
    return "\n".join(lines)


def load_config(path: str | Path) -> TableConfig:
    """Charge et valide un fichier YAML de configuration.

    Args:
        path: Chemin vers le fichier .yaml.

    Returns:
        Instance validée de TableConfig.

    Raises:
        ConfigNotFoundError: Si le fichier n'existe pas.
        ConfigValidationError: Si le contenu est invalide.
    """
    path = Path(path)

    if not path.exists():
        raise ConfigNotFoundError(str(path))

    raw = path.read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ConfigValidationError(
            f"Syntaxe YAML invalide dans '{path}' : {exc}",
            path=str(path),
        ) from exc

    if not isinstance(data, dict):
        raise ConfigValidationError(
            f"Le fichier '{path}' ne contient pas un mapping YAML valide.",
            path=str(path),
        )

    try:
        return TableConfig(**data)
    except ValidationError as exc:
        errors = exc.errors()
        message = _format_pydantic_errors(errors, str(path))
        raise ConfigValidationError(
            message,
            path=str(path),
            errors=errors,
        ) from exc


def load_all_configs(configs_dir: str | Path) -> dict[str, TableConfig]:
    """Charge et valide tous les YAML d'un dossier de configuration.

    Ignore les fichiers commençant par '_' (ex: _summary.md).
    Accumule toutes les erreurs au lieu de s'arrêter au premier fichier invalide.

    Args:
        configs_dir: Dossier contenant les fichiers .yaml.

    Returns:
        Dictionnaire {table_name: TableConfig} pour tous les fichiers valides.

    Raises:
        ConfigNotFoundError: Si le dossier n'existe pas.
        ConfigValidationError: Si au moins un fichier est invalide (toutes
            les erreurs sont agrégées dans le message).
    """
    configs_dir = Path(configs_dir)

    if not configs_dir.is_dir():
        raise ConfigNotFoundError(str(configs_dir))

    results: dict[str, TableConfig] = {}
    all_errors: list[str] = []

    yaml_files = sorted(configs_dir.glob("*.yaml"))
    if not yaml_files:
        logger.warning("Aucun fichier .yaml trouvé dans '%s'", configs_dir)
        return results

    for yaml_path in yaml_files:
        if yaml_path.name.startswith("_"):
            continue

        try:
            config = load_config(yaml_path)
            results[config.table_name] = config
        except (ConfigValidationError, ConfigNotFoundError) as exc:
            all_errors.append(str(exc))

    if all_errors:
        separator = "\n\n"
        raise ConfigValidationError(
            f"{len(all_errors)} fichier(s) invalide(s) sur "
            f"{len(yaml_files)} :\n\n{separator.join(all_errors)}",
            errors=[{"files_in_error": len(all_errors)}],
        )

    logger.info(
        "%d configuration(s) chargée(s) depuis '%s'",
        len(results),
        configs_dir,
    )
    return results
