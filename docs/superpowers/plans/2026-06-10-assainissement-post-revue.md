# Plan d'assainissement post-revue — datalake-pipeline

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal :** Corriger les 8 findings de la revue du 2026-06-10 : données bancaires sur GitHub, tests inversés (le code vivant n'est pas testé), drift config/doc, perf DQ, stack legacy, YAML monolithique, docs périmées.

**Architecture :** On sécurise d'abord le dépôt GitHub (privé + purge historique). Ensuite on pose un filet de tests sur les moteurs vivants (`jobs/bronze/*`, `jobs/gold/*`) AVANT tout refactor, puis on refactore (SCD2 testable, DQ en une passe) sous ce filet. L'archivage du legacy et le split YAML viennent en dernier.

**Tech stack :** PySpark 3.x (session locale pour les tests), pytest, pydantic v2, git-filter-repo.

**Environnements — règle absolue du projet :**
- **Local (Mac)** : édition de code + opérations git uniquement. AUCUNE exécution Python/Spark/pytest.
- **VM ubuntu3** (`cd /media/sf_datalake-pipeline`) : toute exécution (`pytest`, `spark-submit`).

**Portes de confirmation (ne JAMAIS franchir sans accord explicite de Taha) :**
- G1 : `git filter-repo` + `git push --force` (réécriture d'historique).
- G2 : sortie de `canevas/` du suivi git.
- G3 : suppression des branches `claude/*` périmées.
- G4 : déplacement du stack legacy vers `legacy/`.

---

## Task 1 — URGENT : données bancaires sur GitHub

**Constat :** `echantillon/TIERS.xlsx`, `echantillon/IMAGE_CONTRAT.xlsx` (extraits potentiellement client/PII) et les 13 `canevas/EDD_MODEL_*.xlsx` (dictionnaires internes BOA) sont dans l'arbre `origin/main` sur `https://github.com/owlxx6/datalake-pipeline` depuis le commit initial.

**Files :**
- Modify: `.gitignore` (ajouter `canevas/*.xlsx` si G2 accepté)
- Aucun fichier de code

- [ ] **Step 1 : Vérifier la visibilité du repo GitHub (ACTION TAHA, navigateur)**

Ouvrir `https://github.com/owlxx6/datalake-pipeline` en navigation privée (déconnecté).
- Page visible → repo **PUBLIC** → passer en privé immédiatement : Settings → General → Danger Zone → *Change repository visibility* → Private.
- 404 → repo déjà privé → continuer.

Si le repo était public : considérer les fichiers comme potentiellement exposés (forks, caches). **Prévenir l'encadrant entreprise (Reda Kilani)** — c'est de la donnée banque, la décision de signalement lui appartient.

- [ ] **Step 2 : Sortir `echantillon/` du suivi git (local, terminal Mac)**

```bash
cd /Users/administrateur/datalake-pipeline
git rm --cached echantillon/IMAGE_CONTRAT.xlsx echantillon/TIERS.xlsx
git commit -m "chore(securite): retire les extraits bancaires du suivi git

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

Vérif : `git status` → les 2 fichiers apparaissent en untracked (le `.gitignore` couvre déjà `echantillon/`), et `ls echantillon/` les montre toujours sur disque.

- [ ] **Step 3 : [GATE G2] Décision canevas/**

Question à Taha : les EDD restent-ils dans git ?
- **Recommandation : NON** (documents internes BOA). Si accord :

```bash
git rm --cached canevas/EDD_MODEL_*.xlsx
printf '\n# Dictionnaires de données internes BOA — ne pas committer\ncanevas/*.xlsx\n' >> .gitignore
git add .gitignore
git commit -m "chore(securite): retire les EDD internes du suivi git

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 4 : [GATE G3] Nettoyer worktree et branches périmées (prérequis filter-repo)**

```bash
git worktree remove /Users/administrateur/datalake-pipeline/.claude/worktrees/nervous-kalam-c24a63 --force
git branch -D claude/fervent-meitner-b8b2d8 claude/magical-yonath-a437ef claude/nervous-kalam-c24a63 claude/romantic-moore-185f90
```

(Branches à 49f1a39 / 07dfb04, toutes derrière main — vérifier avec `git branch -vv` avant suppression que rien d'unique n'y vit : `git log main..claude/<branche> --oneline` doit être vide ou ne montrer que "Delete CLAUDE.md".)

- [ ] **Step 5 : [GATE G1] Purger l'historique (local, terminal Mac)**

```bash
brew install git-filter-repo
cd /Users/administrateur/datalake-pipeline
git filter-repo --force --invert-paths \
  --path echantillon/IMAGE_CONTRAT.xlsx \
  --path echantillon/TIERS.xlsx \
  --path-glob 'canevas/EDD_MODEL_*.xlsx'   # seulement si G2 accepté
```

NB : filter-repo supprime le remote `origin` par sécurité, et réécrit tous les SHA (les références de commits dans `task_plan.md` / `progress.md` deviennent caduques — c'est accepté, ce sont des notes).

- [ ] **Step 6 : [GATE G1] Re-pousser l'historique réécrit**

```bash
git remote add origin https://github.com/owlxx6/datalake-pipeline.git
git push --force --all origin
git push --force --tags origin
```

Vérif sur GitHub : l'arbre de `main` ne contient plus `echantillon/` ; l'historique du fichier renvoie 404.

- [ ] **Step 7 : Répercuter sur le clone VM + GitLab self-hosted (ACTION sur la VM)**

Le clone de la VM et le GitLab `http://localhost:8929/root/datalake-pipeline` portent encore l'ancien historique. Sur la VM :

```bash
cd /media/sf_datalake-pipeline   # si c'est le même dossier partagé, rien à faire côté clone
# sinon, sur tout clone VM : git fetch origin && git reset --hard origin/main
# GitLab self-hosted : git push --force local main   (depuis le clone à jour)
```

---

## Task 2 — Filet de tests 1 : fonctions pures du moteur Bronze

**Files :**
- Create: `tests/jobs/bronze/__init__.py` (vide)
- Create: `tests/jobs/bronze/test_engine_units.py`
- Create: `tests/jobs/bronze/test_normalization.py`
- Modify: aucun code moteur (tests de caractérisation — ils doivent passer tels quels)

- [ ] **Step 1 : Écrire les tests unitaires engine (cast, conditions DQ, colonnes, dédup)**

`tests/jobs/bronze/test_engine_units.py` :

```python
"""Tests de caractérisation des fonctions pures de jobs/bronze/engine.py."""
from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, TimestampType

from jobs.bronze.config_loader import DQCheck, TableEntry
from jobs.bronze.engine import (
    _check_condition, _normalize_columns, _smart_cast, deduplicate_silver,
)


def make_table(fields=("ID", "VAL"), deduplicate=True, pk=("ID",)):
    return TableEntry(
        name="T", domain="t", primary_key=list(pk),
        source={"path_template": "/tmp/in"},
        schema={"fields": [{"name": f, "spark_type": "StringType"} for f in fields]},
        write={"path_template": "/tmp/out"},
        silver_write={"database": "db", "table": "t", "deduplicate": deduplicate},
    )


class TestSmartCast:
    def test_decimal_europeen(self, spark):
        df = spark.createDataFrame(
            [("+000000000700000,000",), ("1 234,5",), (None,)], "v string")
        out = df.select(_smart_cast(F.col("v"), DecimalType(18, 3)).alias("v")).collect()
        assert str(out[0].v) == "700000.000"
        assert str(out[1].v) == "1234.500"
        assert out[2].v is None

    def test_dates_multi_format(self, spark):
        df = spark.createDataFrame(
            [("2026-05-31",), ("31/05/2026",), ("2026-05-31 10:11:12.123456",)],
            "v string")
        out = df.select(_smart_cast(F.col("v"), DateType()).alias("v")).collect()
        assert [r.v for r in out] == [date(2026, 5, 31)] * 3

    def test_sentinelles(self, spark):
        df = spark.createDataFrame(
            [("0001-01-01 00:00:00",), ("9999-12-31",)], "v string")
        out = df.select(_smart_cast(F.col("v"), DateType()).alias("v")).collect()
        assert out[0].v is None                      # date absente → NULL
        assert out[1].v == date(9999, 12, 31)        # date ouverte conservée


class TestCheckCondition:
    def _failed(self, spark, rows, check):
        df = spark.createDataFrame(rows, "v string")
        cond = _check_condition("v", check)
        return [r.f for r in df.select(cond.alias("f")).collect()]

    def test_not_null_attrape_vide_et_blancs(self, spark):
        check = DQCheck(type="not_null", severity="CRITICAL")
        assert self._failed(spark, [(None,), ("",), ("   ",), ("x",)], check) == \
            [True, True, True, False]

    def test_in_list_ignore_null_et_vide(self, spark):
        check = DQCheck(type="in_list", severity="MEDIUM", allowed_values=["B", "C"])
        assert self._failed(spark, [("B",), ("Z",), ("",), (None,)], check) == \
            [False, True, False, False]

    def test_max_length(self, spark):
        check = DQCheck(type="max_length", severity="LOW", max_length=3)
        assert self._failed(spark, [("abc",), ("abcd",)], check) == [False, True]


def test_normalize_columns_drop_nifi_et_uppercase(spark):
    df = spark.createDataFrame(
        [("a", "f.parquet", "x")], "idf_num_doss string, _source_file string, Cod_Prod string")
    out = _normalize_columns(df)
    assert out.columns == ["IDF_NUM_DOSS", "COD_PROD"]


class TestDeduplicateSilver:
    def test_doublons_parfaits_retires(self, spark):
        df = spark.createDataFrame(
            [("1", "a"), ("1", "a"), ("2", "b")], "ID string, VAL string")
        out, removed = deduplicate_silver(df, make_table())
        assert removed == 1
        assert out.count() == 2

    def test_collision_pk_conservee(self, spark):
        # même PK, valeurs différentes ≠ doublon → les 2 lignes restent
        df = spark.createDataFrame(
            [("1", "a"), ("1", "b")], "ID string, VAL string")
        out, removed = deduplicate_silver(df, make_table())
        assert removed == 0
        assert out.count() == 2

    def test_opt_out(self, spark):
        df = spark.createDataFrame(
            [("1", "a"), ("1", "a")], "ID string, VAL string")
        out, removed = deduplicate_silver(df, make_table(deduplicate=False))
        assert removed == 0
        assert out.count() == 2
```

- [ ] **Step 2 : Écrire les tests de normalisation**

`tests/jobs/bronze/test_normalization.py` :

```python
"""Tests du framework de normalisation déclaratif (jobs/bronze/normalization.py)."""
from __future__ import annotations

import pytest

from jobs.bronze.config_loader import NormalizationConfig, TableEntry
from jobs.bronze.normalization import normalize


class FakeConfig:
    def __init__(self, defaults):
        self.normalization = NormalizationConfig(defaults=defaults)


def make_table(field_norms: dict):
    """field_norms : {nom_colonne: normalize (None / [] / [ops])}"""
    return TableEntry(
        name="T", domain="t", primary_key=["A"],
        source={"path_template": "/tmp/in"},
        schema={"fields": [
            {"name": n, "spark_type": "StringType", "normalize": ops}
            for n, ops in field_norms.items()
        ]},
        write={"path_template": "/tmp/out"},
    )


def test_defaut_string_trim_nullify(spark):
    df = spark.createDataFrame([("  x  ", "")], "A string, B string")
    table = make_table({"A": None, "B": None})
    out = normalize(df, table, FakeConfig({"string": ["trim", "nullify_empty"]})).collect()[0]
    assert out.A == "x"
    assert out.B is None


def test_opt_out_explicite(spark):
    df = spark.createDataFrame([("  x  ",)], "A string")
    table = make_table({"A": []})           # [] = aucune normalisation
    out = normalize(df, table, FakeConfig({"string": ["trim"]})).collect()[0]
    assert out.A == "  x  "


def test_override_par_champ_lpad(spark):
    df = spark.createDataFrame([("42",)], "A string")
    table = make_table({"A": [{"op": "lpad", "width": 6, "char": "0"}]})
    out = normalize(df, table, FakeConfig({"string": ["trim"]})).collect()[0]
    assert out.A == "000042"


def test_op_inconnue_leve(spark):
    df = spark.createDataFrame([("x",)], "A string")
    table = make_table({"A": ["frobnicate"]})
    with pytest.raises(ValueError, match="frobnicate"):
        normalize(df, table, FakeConfig({})).collect()
```

- [ ] **Step 3 : Exécuter sur la VM**

```bash
cd /media/sf_datalake-pipeline
python3 -m pytest tests/jobs/bronze/ -v
```

Attendu : tout PASSE (tests de caractérisation du comportement actuel). Si un test échoue, c'est soit un bug réel découvert (le documenter dans findings.md avant de décider), soit un test faux — corriger le test.

- [ ] **Step 4 : Commit**

```bash
git add tests/jobs/bronze/
git commit -m "test(bronze): filet de caractérisation moteur (cast, DQ, dedup, normalisation)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 3 — Filet de tests 2 : `compute_dq_flags` (statut 3 états)

**Files :**
- Create: `tests/jobs/bronze/test_compute_dq_flags.py`

- [ ] **Step 1 : Écrire le test des 3 états + informational + sévérité max**

```python
"""Tests du flag DQ par ligne — PASSED / WARNING / REJECTED."""
from __future__ import annotations

from jobs.bronze.config_loader import TableEntry
from jobs.bronze.engine import compute_dq_flags


def make_table():
    return TableEntry(
        name="T", domain="t", primary_key=["ID"],
        source={"path_template": "/tmp/in"},
        schema={"fields": [
            {"name": "ID", "spark_type": "StringType"},
            {"name": "COD", "spark_type": "StringType"},
        ]},
        write={"path_template": "/tmp/out"},
        quality_rules=[
            {"field": "ID", "source_column": "ID", "data_type": "string",
             "checks": [{"type": "not_null", "severity": "CRITICAL"}]},
            {"field": "COD", "source_column": "COD", "data_type": "string",
             "checks": [
                 {"type": "in_list", "severity": "MEDIUM",
                  "allowed_values": ["B", "C", "BB"]},
                 # informational : mesuré en audit, JAMAIS dans le statut ligne
                 {"type": "max_length", "severity": "LOW", "max_length": 1,
                  "informational": True},
             ]},
        ],
    )


def _by_id(spark, rows):
    df = spark.createDataFrame(rows, "ID string, COD string")
    out = compute_dq_flags(df, make_table(), "EXEC_TEST")
    return {r.ID: r for r in out.collect()}


def test_statuts_3_etats(spark):
    rows = [("1", "B"), ("2", "Z"), (None, "B")]
    res = _by_id(spark, rows)
    assert res["1"].dq_status == "PASSED"
    assert res["1"].dq_severity_max is None
    assert res["2"].dq_status == "WARNING"
    assert res["2"].dq_severity_max == "MEDIUM"
    assert res["2"].dq_failed_checks == ["COD:in_list"]
    rejected = res[None]
    assert rejected.dq_status == "REJECTED"
    assert rejected.dq_severity_max == "CRITICAL"


def test_informational_exclu_du_statut(spark):
    # "BB" ∈ allowed_values mais viole max_length(1) — check informational
    # → mesuré en audit, exclu du statut ligne → PASSED
    res = _by_id(spark, [("1", "BB")])
    assert res["1"].dq_status == "PASSED"
    assert res["1"].dq_failed_checks == []


def test_execution_id_porte(spark):
    res = _by_id(spark, [("1", "B")])
    assert res["1"].dq_execution_id == "EXEC_TEST"
```

- [ ] **Step 2 : Exécuter sur la VM**

```bash
cd /media/sf_datalake-pipeline && python3 -m pytest tests/jobs/bronze/test_compute_dq_flags.py -v
```

Attendu : PASS (caractérisation).

- [ ] **Step 3 : Commit**

```bash
git add tests/jobs/bronze/test_compute_dq_flags.py
git commit -m "test(bronze): caractérisation compute_dq_flags (3 états, informational, sévérité)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 4 — SCD2 testable : extraire le calcul d'état pur

**Pourquoi :** la logique SCD2 (clôture / conservation / nouvelle version / soft delete) est aujourd'hui inséparable de l'écriture Iceberg → intestable hors VM+catalogue. On extrait une fonction pure DataFrame→DataFrame, `_write_scd2` devient un simple wrapper écriture.

**Files :**
- Modify: `jobs/bronze/historization.py:102-196`
- Create: `tests/jobs/bronze/test_historization_scd2.py`

- [ ] **Step 1 : Écrire le test (échoue : la fonction n'existe pas encore)**

`tests/jobs/bronze/test_historization_scd2.py` :

```python
"""Tests de la transition d'état SCD2 (logique pure, sans Iceberg)."""
from __future__ import annotations

from jobs.bronze.historization import scd2_new_state

SCHEMA = ("ID string, ATTR string, row_hash string, valid_from string, "
          "valid_to string, is_current boolean, version int")
BASE_SCHEMA = "ID string, ATTR string, row_hash string, valid_to string, is_current boolean"


def existing(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)

def photo(spark, rows):
    return spark.createDataFrame(
        [(i, a, h, None, True) for i, a, h in rows], BASE_SCHEMA)

def state(df):
    return {(r.ID, r.version): r for r in df.collect()}


def test_cle_nouvelle_version_1(spark):
    ex = existing(spark, [])
    out = scd2_new_state(ex, photo(spark, [("1", "a", "h1")]), ["ID"], "2026-06-30")
    s = state(out)
    assert s[("1", 1)].is_current and s[("1", 1)].valid_from == "2026-06-30"


def test_hash_identique_rien(spark):
    ex = existing(spark, [("1", "a", "h1", "2026-05-31", None, True, 1)])
    out = scd2_new_state(ex, photo(spark, [("1", "a", "h1")]), ["ID"], "2026-06-30")
    s = state(out)
    assert len(s) == 1 and s[("1", 1)].is_current and s[("1", 1)].valid_to is None


def test_hash_different_cloture_plus_v2(spark):
    ex = existing(spark, [("1", "a", "h1", "2026-05-31", None, True, 1)])
    out = scd2_new_state(ex, photo(spark, [("1", "b", "h2")]), ["ID"], "2026-06-30")
    s = state(out)
    assert not s[("1", 1)].is_current and s[("1", 1)].valid_to == "2026-06-30"
    assert s[("1", 2)].is_current and s[("1", 2)].ATTR == "b"


def test_cle_disparue_soft_delete(spark):
    ex = existing(spark, [("1", "a", "h1", "2026-05-31", None, True, 1)])
    out = scd2_new_state(ex, photo(spark, []), ["ID"], "2026-06-30")
    s = state(out)
    assert len(s) == 1
    assert not s[("1", 1)].is_current and s[("1", 1)].valid_to == "2026-06-30"


def test_historique_preserve(spark):
    ex = existing(spark, [
        ("1", "a", "h1", "2026-04-30", "2026-05-31", False, 1),
        ("1", "b", "h2", "2026-05-31", None, True, 2),
    ])
    out = scd2_new_state(ex, photo(spark, [("1", "c", "h3")]), ["ID"], "2026-06-30")
    s = state(out)
    assert len(s) == 3
    assert s[("1", 3)].is_current and s[("1", 3)].valid_from == "2026-06-30"
    assert not s[("1", 1)].is_current and not s[("1", 2)].is_current
```

NB `photo()` : un DataFrame vide avec schéma → `spark.createDataFrame([], BASE_SCHEMA)` ; le helper gère `rows == []`.

- [ ] **Step 2 : Vérifier l'échec sur la VM**

```bash
cd /media/sf_datalake-pipeline && python3 -m pytest tests/jobs/bronze/test_historization_scd2.py -v
```

Attendu : `ImportError: cannot import name 'scd2_new_state'`.

- [ ] **Step 3 : Extraire la fonction dans `historization.py`**

C'est un déplacement à l'identique des lignes 149-186 de `_write_scd2` (aucun changement de logique) :

```python
def scd2_new_state(
    existing: DataFrame, base: DataFrame, pk: list[str], date_arrete: str,
) -> DataFrame:
    """Calcule le nouvel état SCD2 complet (pur DataFrame, aucune écriture).

    `existing` : table SCD2 actuelle. `base` : photo du jour, déjà porteuse de
    row_hash / valid_to / is_current (cf. _write_scd2). Règles :
    clé nouvelle → v1 ; hash identique → inchangé ; hash différent → clôture + v(max+1) ;
    clé disparue → clôture (soft delete).
    """
    current = existing.where("is_current = true")
    history = existing.where("is_current = false OR is_current IS NULL")

    inc_hash = base.select(*pk, F.col("row_hash").alias("__new_hash"))
    cur_cmp = current.join(inc_hash, pk, "left")

    closed = (
        cur_cmp.where("__new_hash IS NULL OR __new_hash <> row_hash")
               .withColumn("is_current", F.lit(False))
               .withColumn("valid_to", F.lit(date_arrete))
               .drop("__new_hash")
    )
    kept = cur_cmp.where("__new_hash IS NOT NULL AND __new_hash = row_hash").drop("__new_hash")

    cur_hash = current.select(*pk, F.col("row_hash").alias("__cur_hash"))
    max_ver = existing.groupBy(*pk).agg(F.max("version").alias("__maxv"))
    new_versions = (
        base.join(cur_hash, pk, "left")
            .where("__cur_hash IS NULL OR __cur_hash <> row_hash")
            .drop("__cur_hash")
            .join(max_ver, pk, "left")
            .withColumn("version", F.coalesce(F.col("__maxv"), F.lit(0)) + F.lit(1))
            .withColumn("valid_from", F.lit(date_arrete))
            .drop("__maxv")
    )
    return history.unionByName(closed).unionByName(kept).unionByName(new_versions)
```

Cas « table vide » (1er chargement) : `_write_scd2` garde sa branche `createOrReplace` actuelle ; pour le test `test_cle_nouvelle_version_1`, `existing` vide fonctionne aussi par la voie générale (history/closed/kept vides, new_versions = v1).

Dans `_write_scd2`, remplacer les lignes 149-186 par :

```python
    existing = spark.table(full_table)
    final = scd2_new_state(existing, base, pk, date_arrete)
```

(la suite — persist / counts / createOrReplace / unpersist — inchangée).

Documenter aussi la limitation connue, en commentaire au-dessus de `scd2_new_state` :

```python
# Limitation connue : les lignes à PK NULL ne se joignent jamais (sémantique SQL)
# → elles sont clôturées puis réinsérées à CHAQUE arrêté (inflation de versions).
# Volume observé négligeable (ex. IMAGE_CONTRAT : 1 ligne REJECTED). À traiter si
# un référentiel à PK sale entre en SCD2.
```

- [ ] **Step 4 : Exécuter sur la VM — tout doit passer**

```bash
cd /media/sf_datalake-pipeline && python3 -m pytest tests/jobs/bronze/ -v
```

- [ ] **Step 5 : Non-régression réelle (VM) — dry-run sur une dimension SCD2**

```bash
spark-submit --master spark://ubuntu3:7077 jobs/bronze/engine.py \
  --config configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-31 --table POPS --dry-run
```

Attendu : même sortie qu'avant le refactor (lecture + DQ, 0 écriture).

- [ ] **Step 6 : Commit**

```bash
git add jobs/bronze/historization.py tests/jobs/bronze/test_historization_scd2.py
git commit -m "refactor(scd2): extraction scd2_new_state pure + tests des 4 transitions

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 5 — Quick wins : drift config + petits fixes code

**Files :**
- Modify: `jobs/bronze/config_loader.py:162-177`
- Modify: `jobs/bronze/engine.py:660-680` (init source_path), `jobs/bronze/engine.py:208-233` (resolve_date_arrete)
- Modify: `jobs/gold/engine.py:94-100` (scan inutile)

- [ ] **Step 1 : config_loader — corriger le contrat dedup, supprimer le champ mort**

Vérifié : `dedup_order_by` n'apparaît nulle part dans `configs/bronze/credit_garantie.yaml` → suppression sûre. Remplacer dans `SilverWriteConfig` :

```python
    # deduplicate=True : retire UNIQUEMENT les doublons PARFAITS (toutes colonnes
    # métier identiques) avant DQ. Les collisions de PK (même PK, valeurs ≠) sont
    # CONSERVÉES et flaguées REJECTED par le check `unique`. Bronze garde tout.
    deduplicate: bool = False
```

(supprimer les 2 anciennes lignes de commentaire « 1 ligne par PK… survie… » et le champ `dedup_order_by`).

- [ ] **Step 2 : engine — init `source_path` avant le try, retirer le hack `dir()`**

Dans `run_table`, juste avant `try:` (ligne ~664) :

```python
    source_path = ""
```

Et dans l'appel `write_logs` du `finally` (ligne ~745) :

```python
                source_file=source_path,
```

- [ ] **Step 3 : engine — `resolve_date_arrete` avertit si la colonne est multi-valuée**

Remplacer le bloc `row = df_raw.select(col).limit(1).collect()` (lignes 219-224) par :

```python
    agg = df_raw.agg(
        F.min(col).alias("mn"), F.max(col).alias("mx"),
    ).collect()[0]
    if agg.mn is None:
        return cli_date_arrete
    if str(agg.mn) != str(agg.mx):
        print(f"  [WARN] {col} multi-valué ({agg.mn} → {agg.mx}) — "
              f"partition basée sur min={agg.mn}")

    raw = str(agg.mn).strip()
```

(une seule action Spark, remplace le `limit(1)` qui faisait confiance à la 1ʳᵉ ligne).

- [ ] **Step 4 : gold engine — supprimer le full scan d'avertissement**

Dans `build_fact`, remplacer les lignes 96-100 par :

```python
    if date_arrete:
        df = df.where(F.col("date_arrete") == F.lit(date_arrete))
        if df.limit(1).count() == 0:
            print(f"  [WARN] aucune ligne pour date_arrete='{date_arrete}' dans {tgt.source}")
```

- [ ] **Step 5 : Exécuter le filet de tests sur la VM**

```bash
cd /media/sf_datalake-pipeline && python3 -m pytest tests/jobs/bronze/ -v
```

Attendu : PASS (aucun de ces fixes ne change le comportement testé).

- [ ] **Step 6 : Commit**

```bash
git add jobs/bronze/config_loader.py jobs/bronze/engine.py jobs/gold/engine.py
git commit -m "fix(engine): contrat dedup exact, source_path init, date_arrete multi-valuee, scan gold inutile

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 6 — DQ en une passe (perf : ~N actions → 2)

**Pourquoi :** `run_dq` lance 1 action Spark par check + 1 count de scope par règle (175 règles au total), sur un DataFrame non caché → chaque count ré-exécute lecture+normalisation+dédup. On évalue chaque condition UNE fois en colonne booléenne, puis : (a) un seul `agg` pour QUALITY_RESULTS, (b) les mêmes colonnes pour les flags ligne.

**Files :**
- Modify: `jobs/bronze/engine.py` (`run_dq`, `compute_dq_flags`, `run_table`)
- Create: `tests/jobs/bronze/test_dq_single_pass.py`

- [ ] **Step 1 : Écrire le test d'équivalence (échoue : `build_check_columns` n'existe pas)**

```python
"""Équivalence du DQ une-passe : mêmes stats QUALITY_RESULTS, mêmes flags."""
from __future__ import annotations

from jobs.bronze.engine import build_check_columns, compute_dq_flags, run_dq
from tests.jobs.bronze.test_compute_dq_flags import make_table


def _fake_config():
    from jobs.bronze.config_loader import BronzeConfig
    return BronzeConfig(
        globals={"layer_abbrev": "BR"},
        project={"layer": "BRONZE", "domain": "TEST",
                 "execution_id_template": "{layer_abbrev}_{table_name}"},
        output={"database": "db",
                "quality_results": {"name_template": "QR"},
                "log_pipeline": {"name_template": "LP"}},
        tables=[],
    )


def test_stats_une_passe(spark):
    table = make_table()
    df = spark.createDataFrame(
        [("1", "B"), ("2", "Z"), (None, "C"), (None, "Z")], "ID string, COD string")
    df_checked, metas = build_check_columns(df, table)
    results = run_dq(df_checked, metas, table, _fake_config(), "EXEC", "2026-06-30")
    by_rule = {r["rule_id"]: r for r in results}
    assert by_rule["DQ_ID_NOT_NULL"]["records_failed"] == 2
    assert by_rule["DQ_COD_IN_LIST"]["records_failed"] == 2
    assert by_rule["DQ_COD_MAX_LENGTH"]["records_failed"] == 0   # "B","Z","C" ≤ 1
    assert all(r["records_checked"] == 4 for r in results)


def test_flags_identiques_apres_refactor(spark):
    table = make_table()
    df = spark.createDataFrame([("1", "B"), (None, "Z")], "ID string, COD string")
    df_checked, metas = build_check_columns(df, table)
    out = {r.ID: r for r in compute_dq_flags(df_checked, metas, "EXEC").collect()}
    assert out["1"].dq_status == "PASSED"
    rej = [r for r in out.values() if r.ID is None][0]
    assert rej.dq_status == "REJECTED"
    assert sorted(rej.dq_failed_checks) == ["COD:in_list", "ID:not_null"]
    # les colonnes techniques __chk_* / __scope_* ne fuient pas en Silver
    assert not [c for c in compute_dq_flags(df_checked, metas, "EXEC").columns
                if c.startswith("__")]
```

(adapter `make_table()` de Task 3 si besoin pour exposer aussi `consistency_rules` ; ajouter alors un cas consistency au test.)

- [ ] **Step 2 : Vérifier l'échec sur la VM** (`ImportError: build_check_columns`)

- [ ] **Step 3 : Implémenter `build_check_columns` et refondre `run_dq` / `compute_dq_flags`**

Dans `engine.py`, remplacer `run_dq` + `compute_dq_flags` actuels par :

```python
def build_check_columns(df: DataFrame, table: TableEntry) -> tuple[DataFrame, list[dict]]:
    """Évalue chaque check UNE fois → colonne booléenne __chk_i (échec dans le scope).

    Retourne (df enrichi, metas). Chaque meta décrit un check :
    {chk_col, scope_col, rule_id, check_id, rule_type, field, severity,
     threshold, control_type, informational}.
    Servent à la fois aux agrégats QUALITY_RESULTS (run_dq) et aux flags ligne
    (compute_dq_flags) — une seule évaluation, deux usages.
    """
    metas: list[dict] = []
    out = df
    i = 0

    for rule in table.quality_rules:
        col_name = rule.source_column
        if col_name not in df.columns:
            continue
        scope_col = f"__scope_{i}"
        scope = F.expr(rule.filter_condition) if rule.filter_condition else F.lit(True)
        out = out.withColumn(scope_col, scope)
        for check in rule.checks:
            cond = _check_condition(col_name, check)
            if cond is None:
                continue
            chk_col = f"__chk_{i}"
            out = out.withColumn(chk_col, cond & F.col(scope_col))
            metas.append({
                "chk_col": chk_col, "scope_col": scope_col,
                "rule_id": f"DQ_{rule.field}_{check.type}".upper(),
                "check_id": f"{rule.field}:{check.type}",
                "rule_type": check.type, "field": col_name,
                "severity": check.severity, "threshold": check.alert_threshold,
                "control_type": check.control_type,
                "informational": check.informational,
            })
            i += 1

    for rule in table.consistency_rules:
        null_conds = [F.col(f).isNull() for f in rule.required_fields if f in df.columns]
        if not null_conds:
            continue
        any_null = null_conds[0]
        for c in null_conds[1:]:
            any_null = any_null | c
        scope_col, chk_col = f"__scope_{i}", f"__chk_{i}"
        out = (out.withColumn(scope_col, F.expr(rule.condition))
                  .withColumn(chk_col, any_null & F.col(scope_col)))
        metas.append({
            "chk_col": chk_col, "scope_col": scope_col,
            "rule_id": f"DQ_CONSISTENCY_{rule.name}".upper(),
            "check_id": f"CONSISTENCY:{rule.name}",
            "rule_type": "consistency",
            "field": ", ".join(rule.required_fields),
            "severity": rule.severity, "threshold": rule.alert_threshold,
            "control_type": rule.control_type, "informational": False,
        })
        i += 1

    return out, metas


def run_dq(
    df_checked: DataFrame, metas: list[dict], table: TableEntry,
    config: BronzeConfig, execution_id: str, date_situation: str,
) -> list[dict]:
    """Agrégats DQ par règle — UNE action Spark pour tous les checks."""
    if not metas:
        return []
    aggs = []
    for m in metas:
        aggs.append(F.sum(F.when(F.col(m["chk_col"]), 1).otherwise(0)).alias("f_" + m["chk_col"]))
        aggs.append(F.sum(F.when(F.col(m["scope_col"]), 1).otherwise(0)).alias("s_" + m["chk_col"]))
    row = df_checked.agg(*aggs).collect()[0].asDict()

    results = []
    for m in metas:
        failed = row["f_" + m["chk_col"]] or 0
        scope_count = row["s_" + m["chk_col"]] or 0
        rate = (failed / scope_count * 100) if scope_count > 0 else 0.0
        results.append(_qr_row(
            execution_id, date_situation, m["rule_id"], m["rule_type"],
            m["field"], m["severity"], scope_count, failed, rate,
            m["threshold"], m["control_type"], config, table,
        ))
    return results


def compute_dq_flags(
    df_checked: DataFrame, metas: list[dict], execution_id: str,
) -> DataFrame:
    """Flags ligne depuis les colonnes __chk_* (informational exclus), puis drop __*."""
    fail_exprs = [
        F.when(F.col(m["chk_col"]), F.lit(m["check_id"]))
        for m in metas if not m["informational"]
    ]
    weight_exprs = [
        F.when(F.col(m["chk_col"]), F.lit(SEVERITY_WEIGHT.get(m["severity"], 4)))
        for m in metas if not m["informational"]
    ]
    tech_cols = [c for c in df_checked.columns if c.startswith("__chk_") or c.startswith("__scope_")]

    if not fail_exprs:
        return (
            df_checked.drop(*tech_cols)
            .withColumn("dq_failed_checks", F.array().cast("array<string>"))
            .withColumn("dq_status", F.lit("PASSED"))
            .withColumn("dq_severity_max", F.lit(None).cast(StringType()))
            .withColumn("dq_execution_id", F.lit(execution_id))
        )
    # … suite identique à l'actuelle (arrays bruts → filter → __dq_min_weight →
    # dq_status / dq_severity_max / dq_execution_id), en ajoutant `.drop(*tech_cols)`
    # sur le DataFrame final. Reprendre les lignes 446-481 actuelles telles quelles.
```

NB unification `unique` : l'ancien `run_dq` comptait les doublons via un `withColumn+filter` dédié, `_check_condition("unique")` fait le même décompte (le `count(col) over` d'une partition NULL vaut 0 → jamais > 1). Le test d'équivalence le verrouille.

Dans `run_table`, remplacer les étapes 4-5 (lignes ~706-713) par :

```python
        # 4-5. Évaluation DQ une passe : colonnes booléennes → agrégats + flags
        df_checked, dq_metas = build_check_columns(df_silver_base, table)
        df_checked.persist()
        quality_results = run_dq(
            df_checked, dq_metas, table, config, execution_id, date_situation,
        )
        print_report(quality_results, table.name)
        df_silver = compute_dq_flags(df_checked, dq_metas, execution_id)
```

et après le bloc `status_counts` (ligne ~728) ajouter `df_checked.unpersist()`.

- [ ] **Step 3bis : Adapter `tests/jobs/bronze/test_compute_dq_flags.py` au nouveau contrat**

La signature de `compute_dq_flags` change → remplacer le helper `_by_id` de Task 3 :

```python
from jobs.bronze.engine import build_check_columns, compute_dq_flags


def _by_id(spark, rows):
    df = spark.createDataFrame(rows, "ID string, COD string")
    df_checked, metas = build_check_columns(df, make_table())
    out = compute_dq_flags(df_checked, metas, "EXEC_TEST")
    return {r.ID: r for r in out.collect()}
```

(les assertions des 3 tests de Task 3 restent valides telles quelles).

- [ ] **Step 4 : Tous les tests sur la VM**

```bash
cd /media/sf_datalake-pipeline && python3 -m pytest tests/jobs/bronze/ -v
```

- [ ] **Step 5 : Non-régression réelle (VM) — comparer aux stats connues**

```bash
spark-submit --master spark://ubuntu3:7077 jobs/bronze/engine.py \
  --config configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-31 --table TIERS --dry-run
```

Comparer le RAPPORT DQ affiché avec le dernier run TIERS connu (154 245 lignes, WARN 14 069 — cf. task_plan.md). Les `records_failed` par règle doivent être identiques.

- [ ] **Step 6 : Commit**

```bash
git add jobs/bronze/engine.py tests/jobs/bronze/test_dq_single_pass.py
git commit -m "perf(dq): evaluation une passe (1 agg pour QUALITY_RESULTS + flags partages)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 7 — [GATE G4] Archiver le stack legacy

**Constat :** `datalake_lib/` (64 fichiers, testé) n'est importé par AUCUN code vivant — seulement par `jobs/base_job.py` (mort), `jobs/silver/silver_credit_encours.py` (remplacé par le moteur Bronze) et le DAG d'avril jamais exécuté. On ARCHIVE (git mv, historique préservé), on ne supprime pas.

**Files :**
- Create: `legacy/README.md`
- Move: `datalake_lib/` → `legacy/datalake_lib/` ; `jobs/base_job.py`, `jobs/silver/` → `legacy/` ; `dags/` → `legacy/dags/` ; `tests/{read,write,quality,log,config}/` + `tests/jobs/test_silver_credit_encours.py` → `legacy/tests_datalake_lib/` ; `scripts/generate_configs.py` → `legacy/`
- Modify: `pyproject.toml:32-34`

- [ ] **Step 1 : Demander confirmation à Taha (G4)** — liste exacte ci-dessus. Rien ne bouge sans accord.

- [ ] **Step 2 : Déplacer (local, terminal Mac)**

```bash
cd /Users/administrateur/datalake-pipeline
mkdir -p legacy/tests_datalake_lib
git mv datalake_lib legacy/datalake_lib
git mv jobs/base_job.py jobs/silver legacy/
git mv dags legacy/dags
git mv tests/read tests/write tests/quality tests/log tests/config legacy/tests_datalake_lib/
git mv tests/jobs/test_silver_credit_encours.py legacy/tests_datalake_lib/
git mv scripts/generate_configs.py legacy/
rm -f datalake_lib.zip
```

- [ ] **Step 3 : `legacy/README.md`**

```markdown
# Legacy — première architecture (avril–mai 2026)

Bibliothèque `datalake_lib` + job Silver dédié + DAG Airflow : première itération
du framework, remplacée en mai 2026 par les moteurs déclaratifs `jobs/bronze/engine.py`
et `jobs/gold/engine.py` (config YAML, cf. `configs/`).

Archivé tel quel pour référence (les tests documentent le design). Les imports
`datalake_lib.*` ne sont plus résolus depuis ce dossier — ce code n'est pas exécutable
sans réinstallation, et c'est voulu.
```

- [ ] **Step 4 : `pyproject.toml` — ne plus empaqueter le legacy**

```toml
[tool.setuptools.packages.find]
include = ["jobs*"]
exclude = ["tests*", "legacy*", "scripts*", "canevas*"]
```

(le reste du fichier inchangé ; `testpaths = ["tests"]` ne voit plus que les tests des moteurs vivants).

- [ ] **Step 5 : Vérifier sur la VM que les tests vivants tournent toujours**

```bash
cd /media/sf_datalake-pipeline && python3 -m pytest tests/ -v
```

Attendu : seuls les tests `tests/jobs/bronze/` s'exécutent, tous PASS.

- [ ] **Step 6 : Commit**

```bash
git add -A
git commit -m "chore(legacy): archive datalake_lib + jobs/dags associes sous legacy/

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 8 — Documentation : README + CLAUDE.md à jour

**Files :**
- Rewrite: `README.md`
- Modify: `CLAUDE.md` (section stack + structure)

- [ ] **Step 1 : Réécrire `README.md`**

```markdown
# Datalake Pipeline — Crédit-Garantie

Framework déclaratif pour pipelines médaillon (Bronze / Silver / Gold)
sur l'axe Crédit-Garantie de BOA Entreprise. POC — le modèle vit dans
les YAML, les moteurs sont génériques.

## Structure

- `jobs/bronze/` — moteur Bronze→Silver : typage tolérant, DQ déclaratif
  (3 états PASSED/WARNING/REJECTED), normalisation, historisation
  (snapshot faits / SCD2 dimensions)
- `jobs/gold/` — moteur Gold déclaratif (date / dimension / fact / OBT, jointures as-of)
- `configs/` — YAML par couche (`bronze/`, `gold/`) : schémas, règles DQ, modèle Gold
- `tests/jobs/` — tests pytest des moteurs (SparkSession locale)
- `scripts/` — outils d'inspection et d'audit (EDD ↔ YAML)
- `canevas/` — dictionnaires de données Excel (source de vérité métier, non committés)
- `legacy/` — première architecture (datalake_lib), archivée
- `docs/` — modèle dimensionnel, runbook, état projet

## Exécution (sur la VM ubuntu3 uniquement)

\`\`\`bash
cd /media/sf_datalake-pipeline

# Tests
python3 -m pytest tests/ -v

# Bronze→Silver (une table ou toutes)
spark-submit --master spark://ubuntu3:7077 jobs/bronze/engine.py \
  --config configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-31 [--table TIERS] [--dry-run]

# Gold
spark-submit jobs/gold/engine.py \
  --config configs/gold/credit_garantie.yaml --date-arrete 2026-05-31
\`\`\`

Le code est édité en local (Mac), exécuté sur la VM via dossier partagé
VirtualBox. Voir `CLAUDE.md` et `docs/runbook.md`.
```

- [ ] **Step 2 : Patcher `CLAUDE.md`**

Remplacements ciblés :
- `- **BI** : Tableau` → `- **BI** : Power BI (décision 2026-06-07, remplace Tableau)`
- Dans « Structure de datalake_lib » : remplacer le titre et l'intro par
  `## Structure (moteurs vivants)` décrivant `jobs/bronze/engine.py + config_loader + historization + normalization`, `jobs/gold/engine.py + config_loader`, `configs/bronze|gold/*.yaml`, et une ligne `legacy/ : première architecture (datalake_lib) — archivée, ne pas étendre`.

- [ ] **Step 3 : Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs: README et CLAUDE.md alignes sur l'architecture reelle (moteurs + Power BI)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 9 (OPTIONNEL) — Split du YAML Bronze par table

À faire seulement si le YAML 9 734 lignes devient un point de friction réel (diffs, conflits). Sinon YAGNI.

**Files :**
- Modify: `jobs/bronze/config_loader.py:280-289` (`load_config`)
- Create: `scripts/split_bronze_yaml.py` (one-shot)
- Create: `tests/jobs/bronze/test_config_loader_dir.py`

- [ ] **Step 1 : Test du loader répertoire (échoue d'abord)**

```python
"""load_config accepte un répertoire : _globals.yaml + tables/*.yaml."""
import textwrap

from jobs.bronze.config_loader import load_config

GLOBALS = """
globals: {layer_abbrev: BR}
project: {layer: BRONZE, domain: TEST, execution_id_template: '{layer_abbrev}_{table_name}'}
output:
  database: db
  quality_results: {name_template: QR}
  log_pipeline: {name_template: LP}
"""

TABLE = """
name: T1
domain: t1
primary_key: [ID]
source: {path_template: /tmp/in}
schema:
  fields:
    - {name: ID, spark_type: StringType}
write: {path_template: /tmp/out}
"""


def test_load_config_directory(tmp_path):
    (tmp_path / "_globals.yaml").write_text(textwrap.dedent(GLOBALS), encoding="utf-8")
    tables = tmp_path / "tables"
    tables.mkdir()
    (tables / "t1.yaml").write_text(textwrap.dedent(TABLE), encoding="utf-8")

    config = load_config(str(tmp_path))
    assert [t.name for t in config.tables] == ["T1"]
    assert config.project.domain == "TEST"
```

- [ ] **Step 2 : Implémenter dans `load_config`**

```python
def load_config(yaml_path: str) -> BronzeConfig:
    """Charge le YAML Bronze : fichier unique OU répertoire (_globals.yaml + tables/*.yaml)."""
    path = Path(yaml_path)
    if path.is_dir():
        raw = yaml.safe_load((path / "_globals.yaml").read_text(encoding="utf-8"))
        raw["tables"] = [
            yaml.safe_load(f.read_text(encoding="utf-8"))
            for f in sorted((path / "tables").glob("*.yaml"))
        ]
        return BronzeConfig(**raw)
    if not path.exists():
        raise FileNotFoundError(f"Config introuvable : {yaml_path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return BronzeConfig(**raw)
```

- [ ] **Step 3 : Script de split one-shot `scripts/split_bronze_yaml.py`**

```python
"""Découpe configs/bronze/credit_garantie.yaml → credit_garantie/{_globals.yaml,tables/*.yaml}.

À exécuter UNE FOIS sur la VM :
    python3 scripts/split_bronze_yaml.py configs/bronze/credit_garantie.yaml
"""
import argparse
import sys
from pathlib import Path

import yaml


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="YAML Bronze groupé")
    args = parser.parse_args()

    src = Path(args.source)
    raw = yaml.safe_load(src.read_text(encoding="utf-8"))
    out_dir = src.parent / src.stem
    (out_dir / "tables").mkdir(parents=True, exist_ok=True)

    tables = raw.pop("tables")
    (out_dir / "_globals.yaml").write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
    for t in tables:
        (out_dir / "tables" / f"{t['name'].lower()}.yaml").write_text(
            yaml.safe_dump(t, sort_keys=False, allow_unicode=True), encoding="utf-8")
    print(f"{len(tables)} tables → {out_dir}/", file=sys.stderr)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4 : Équivalence sur la VM** — `load_config('configs/bronze/credit_garantie.yaml')` et `load_config('configs/bronze/credit_garantie/')` doivent produire le même `BronzeConfig` (comparer `model_dump()`), puis dry-run d'une table avec le répertoire. Garder l'ancien fichier jusqu'à validation, puis le retirer (demander avant — règle projet).

- [ ] **Step 5 : Commit**

```bash
git add jobs/bronze/config_loader.py scripts/split_bronze_yaml.py tests/jobs/bronze/test_config_loader_dir.py
git commit -m "feat(config): loader repertoire (_globals + tables/*.yaml) + script de split

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Hors scope (volontairement)

- `valid_from`/`valid_to` en `DateType` au lieu de string : changerait le schéma des 6 dims SCD2 → re-baseline. À faire EN MÊME TEMPS que l'activation de la normalisation (Phase N), qui impose déjà un re-baseline (row_hash change). Une seule réécriture au lieu de deux.
- Lignes PK NULL re-versionnées en SCD2 : documenté en limitation (Task 4), volume négligeable au POC.
- Orchestration Airflow, Atlas/Ranger, Power BI : périmètre cible du projet, pas de l'assainissement.

## Ordre et dépendances

```
T1 (sécurité, indépendant, FAIRE EN PREMIER)
T2 → T3 → T4 → T5 → T6   (le filet de tests précède chaque refactor)
T7 (gate G4) → T8         (le README décrit l'état post-archivage)
T9 optionnel, après T6
```
