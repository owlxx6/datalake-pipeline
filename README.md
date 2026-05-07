# Datalake Pipeline — Crédit-Garantie

Framework standardisé pour pipelines médaillon (Bronze / Silver / Gold)
sur l'axe Crédit-Garantie de BOA Entreprise.

## Structure

- `datalake_lib/` — bibliothèque Python réutilisable (read / quality / write / log / config)
- `dags/` — DAGs Airflow et configs YAML par table
- `scripts/` — outils (génération de configs, fixtures, etc.)
- `tests/` — tests unitaires pytest
- `canevas/` — dictionnaires de données Excel (source de vérité métier)

## Développement

Le code est écrit en local, l'exécution se fait sur la VM via dossier partagé
VirtualBox. Voir `CLAUDE.md` pour le contexte complet.

## Déploiement sur la VM

\`\`\`bash
# Sur la VM, une seule fois
cd /media/sf_datalake-pipeline
pip install -e .
\`\`\`

Les changements locaux sont immédiatement visibles via le dossier partagé.
