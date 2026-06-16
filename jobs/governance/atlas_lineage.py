#!/usr/bin/env python3
"""Pousse le lineage médaillon (Landing → Bronze → Silver → Gold) dans Apache Atlas.

Le graphe est DÉRIVÉ des mêmes configs YAML que le pipeline (sources → cibles déjà
déclarées) — cohérent avec la thèse « framework déclaratif » : on ne code pas le
lineage à la main, on le génère depuis la config.

  - Pour chaque table active : Landing → (ingest NiFi) → Bronze → (refine Spark) → Silver
  - Pour chaque cible Gold    : Silver(s)/Gold(s) → (build Spark) → Gold

Implémentation REST API v2 d'Atlas, en stdlib pure (urllib) — aucune dépendance à
installer sur la VM. Types custom `medallion_dataset` / `medallion_process` sous-typant
les `DataSet` / `Process` natifs : Atlas calcule le lineage automatiquement, et on
porte l'attribut métier `layer`.

Exécution (sur la VM, Atlas démarré) :
  python3 jobs/governance/atlas_lineage.py
  python3 jobs/governance/atlas_lineage.py --dry-run     # affiche sans pousser

Config via options ou variables d'env : ATLAS_URL, ATLAS_USER, ATLAS_PASS, ATLAS_CLUSTER.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
from urllib import error, request

import yaml

ROOT = Path(__file__).resolve().parents[2]

# Conventions médaillon (cf. CLAUDE.md) : la techno de stockage par couche.
LAYER_TECH = {"landing": "csv", "bronze": "parquet", "silver": "iceberg", "gold": "iceberg"}

# 13 tables réellement alimentées par le pipeline (= liste TABLES du DAG Airflow).
# Les autres entrées du YAML Bronze n'ont pas de donnée → exclues du lineage.
ACTIVE_TABLES = [
    "TIERS", "IMAGE_CONTRAT", "GRC_PORTEFEUILLE", "TYPOLOGIE", "ASAL", "POPS", "ORG",
    "CREDIT_AMORT", "IMPAYES_CDL", "DEPASSEMENT",
    "GARANTIE_DETAIL", "GARANTIE_CONTRAT", "IDCE_AUTORISATION",
]

# Types custom : sous-types des types natifs Atlas pour hériter du moteur de lineage.
TYPEDEFS = {
    "entityDefs": [
        {
            "name": "medallion_dataset",
            "superTypes": ["DataSet"],
            "serviceType": "medallion",
            "typeVersion": "1.0",
            "attributeDefs": [
                {"name": "layer", "typeName": "string", "isOptional": True,
                 "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                {"name": "technology", "typeName": "string", "isOptional": True,
                 "cardinality": "SINGLE", "isUnique": False, "isIndexable": False},
                {"name": "table_name", "typeName": "string", "isOptional": True,
                 "cardinality": "SINGLE", "isUnique": False, "isIndexable": False},
            ],
        },
        {
            "name": "medallion_process",
            "superTypes": ["Process"],
            "serviceType": "medallion",
            "typeVersion": "1.0",
            "attributeDefs": [
                {"name": "process_type", "typeName": "string", "isOptional": True,
                 "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                {"name": "engine", "typeName": "string", "isOptional": True,
                 "cardinality": "SINGLE", "isUnique": False, "isIndexable": False},
            ],
        },
    ]
}


# ── Construction des entités ────────────────────────────────────────────────

def _qn(layer: str, name: str, cluster: str) -> str:
    return f"{layer}.{name}@{cluster}"


def _dataset(layer: str, name: str, cluster: str, desc: str = "") -> dict:
    return {
        "typeName": "medallion_dataset",
        "attributes": {
            "qualifiedName": _qn(layer, name, cluster),
            "name": f"{layer}.{name}",
            "layer": layer,
            "technology": LAYER_TECH[layer],
            "table_name": name,
            "description": desc,
        },
    }


def _ref(layer: str, name: str, cluster: str) -> dict:
    """Référence à un dataset par son qualifiedName (résolue par Atlas)."""
    return {"typeName": "medallion_dataset",
            "uniqueAttributes": {"qualifiedName": _qn(layer, name, cluster)}}


def _process(ptype: str, name: str, cluster: str, inputs: list, outputs: list,
             engine: str, desc: str = "") -> dict:
    return {
        "typeName": "medallion_process",
        "attributes": {
            "qualifiedName": f"{ptype}_{name}@{cluster}",
            "name": f"{ptype} {name}",
            "process_type": ptype,
            "engine": engine,
            "inputs": inputs,
            "outputs": outputs,
            "description": desc,
        },
    }


def build_bronze_silver(bronze_cfg: dict, cluster: str) -> tuple[list, list]:
    """Chaînes Landing → Bronze → Silver pour les 13 tables actives."""
    datasets, processes = [], []
    known = {t["name"] for t in bronze_cfg.get("tables", [])}
    for up in ACTIVE_TABLES:
        if up not in known:
            continue  # garde-fou : la table doit exister dans le YAML Bronze
        t = up.lower()
        datasets += [
            _dataset("landing", t, cluster, "Fichier source ingéré (NiFi)"),
            _dataset("bronze", t, cluster, "Miroir brut de la source (Parquet)"),
            _dataset("silver", t, cluster, "Typé, normalisé, dédupliqué, DQ flaggé (Iceberg)"),
        ]
        processes += [
            _process("ingest", t, cluster, [_ref("landing", t, cluster)],
                     [_ref("bronze", t, cluster)], "nifi", "Ingestion Landing -> Bronze"),
            _process("refine", t, cluster, [_ref("bronze", t, cluster)],
                     [_ref("silver", t, cluster)], "spark",
                     "Bronze -> Silver : normalisation + DQ + historisation"),
        ]
    return datasets, processes


def _resolve(refstr: str) -> tuple[str, str]:
    """'silver_xxx.pops' → ('silver','pops') ; 'gdw_encours' → ('gold','gdw_encours')."""
    if "." in refstr:
        return ("silver", refstr.split(".", 1)[1])
    return ("gold", refstr)


def _gold_inputs(target: dict) -> list[tuple[str, str]]:
    """Refs qui ALIMENTENT la cible (flux de données réel ; pas les FK du modèle étoile)."""
    refs: list[tuple[str, str]] = []
    if target.get("source"):
        refs.append(_resolve(target["source"]))
    for j in target.get("joins", []):
        if j.get("with"):
            refs.append(_resolve(j["with"]))
    if target.get("base"):
        refs.append(("gold", target["base"]))
    for a in target.get("aggregates", []):
        if a.get("from"):
            refs.append(("gold", a["from"]))
    seen, out = set(), []
    for r in refs:
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out


def build_gold(gold_cfg: dict, cluster: str) -> tuple[list, list]:
    """Datasets Gold + processus de construction depuis leurs sources Silver/Gold."""
    datasets, processes = [], []
    for tgt in gold_cfg.get("targets", []):
        name = tgt["name"]
        ttype = tgt.get("type", "")
        datasets.append(_dataset("gold", name, cluster, f"Cible Gold ({ttype})"))
        inputs = [_ref(layer, n, cluster) for layer, n in _gold_inputs(tgt)]
        if inputs:  # dim_temps (générée) n'a pas de source → pas de processus
            processes.append(_process("build", name, cluster, inputs,
                                       [_ref("gold", name, cluster)], "spark",
                                       f"Construction Gold {name} ({ttype})"))
    return datasets, processes


# ── Client Atlas (REST v2, stdlib) ──────────────────────────────────────────

class Atlas:
    def __init__(self, url: str, user: str, pwd: str):
        self.url = url.rstrip("/")
        token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        self.auth = f"Basic {token}"

    def _call(self, method: str, path: str, payload: dict | None = None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = request.Request(self.url + path, data=data, method=method)
        req.add_header("Authorization", self.auth)
        req.add_header("Content-Type", "application/json")
        try:
            with request.urlopen(req, timeout=60) as resp:
                body = resp.read().decode()
                return resp.status, (json.loads(body) if body else {})
        except error.HTTPError as exc:
            return exc.code, exc.read().decode()

    def ensure_typedefs(self) -> None:
        """Crée chaque type ; 409 (déjà existant) = OK. Gère un état partiel."""
        for d in TYPEDEFS["entityDefs"]:
            code, resp = self._call("POST", "/api/atlas/v2/types/typedefs", {"entityDefs": [d]})
            if code in (200, 201):
                print(f"  type créé : {d['name']}")
            elif code == 409:
                print(f"  type déjà présent : {d['name']}")
            else:
                raise RuntimeError(f"échec typedef {d['name']} : HTTP {code} {resp}")

    def upsert(self, entities: list) -> dict:
        code, resp = self._call("POST", "/api/atlas/v2/entity/bulk", {"entities": entities})
        if code not in (200, 201):
            raise RuntimeError(f"échec upsert entités : HTTP {code} {resp}")
        return resp


# ── Orchestration ───────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Pousse le lineage médaillon dans Apache Atlas.")
    ap.add_argument("--bronze-config", default=str(ROOT / "configs/bronze/credit_garantie.yaml"))
    ap.add_argument("--gold-config", default=str(ROOT / "configs/gold/credit_garantie.yaml"))
    ap.add_argument("--atlas-url", default=os.environ.get("ATLAS_URL", "http://localhost:21000"))
    ap.add_argument("--user", default=os.environ.get("ATLAS_USER", "admin"))
    ap.add_argument("--password", default=os.environ.get("ATLAS_PASS", "admin"))
    ap.add_argument("--cluster", default=os.environ.get("ATLAS_CLUSTER", "credit_garantie"))
    ap.add_argument("--dry-run", action="store_true", help="affiche les entités sans pousser")
    args = ap.parse_args()

    with open(args.bronze_config, encoding="utf-8") as fh:
        bronze_cfg = yaml.safe_load(fh)
    with open(args.gold_config, encoding="utf-8") as fh:
        gold_cfg = yaml.safe_load(fh)

    ds_bs, pr_bs = build_bronze_silver(bronze_cfg, args.cluster)
    ds_g, pr_g = build_gold(gold_cfg, args.cluster)
    datasets = ds_bs + ds_g
    processes = pr_bs + pr_g

    print(f"Lineage médaillon (cluster={args.cluster}) : "
          f"{len(datasets)} datasets, {len(processes)} processus.")

    if args.dry_run:
        print(json.dumps({"datasets": datasets, "processes": processes},
                         ensure_ascii=False, indent=2))
        return

    atlas = Atlas(args.atlas_url, args.user, args.password)
    print("1) Types :")
    atlas.ensure_typedefs()
    print(f"2) Datasets ({len(datasets)}) …")
    atlas.upsert(datasets)        # créer les datasets AVANT les processus (références)
    print(f"3) Processus ({len(processes)}) …")
    atlas.upsert(processes)
    print(f"OK — lineage poussé. UI : {args.atlas_url}/index.html "
          f"(recherche type medallion_dataset, puis onglet Lineage).")


if __name__ == "__main__":
    main()
