"""normalization.py — Framework de normalisation déclaratif.

Applique des opérations de normalisation déclarées dans le YAML, APRÈS l'écriture
Bronze (qui reste fidèle à la source) et AVANT dédup / DQ / Silver. Donc :

    Landing → Bronze (brut) → [NORMALISATION] → dédup → DQ → Silver

Objectif : aligner les valeurs (padding des codes, casse, vides) pour que les clés de
jointure matchent et que les codes collent aux `allowed_values` EDD.

Config ::

    normalization:
      defaults:
        string: [trim, nullify_empty]      # appliqué à toute colonne string
    # ... et par champ (schema.fields[].normalize) :
    schema:
      fields:
        - name: COD_PRODUIT
          normalize: [trim, upper]
        - name: IDF_CODE_PRODUIT
          normalize:
            - {op: replace, pattern: '\\s+', with: ''}
            - {op: lpad, width: 12, char: '0'}

Règles de priorité :
  - champ avec `normalize: [...]`   → ces ops (override le défaut)
  - champ avec `normalize: []`      → AUCUNE normalisation (opt-out explicite)
  - champ string sans `normalize`   → ops par défaut `defaults.string`
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def _apply_op(col, op):
    """Applique une opération à une colonne. `op` = str (op simple) ou dict {op, args}."""
    if isinstance(op, str):
        name, args = op, {}
    elif isinstance(op, dict):
        name, args = op.get("op"), op
    else:
        return col

    if name == "trim":
        return F.trim(col)
    if name == "ltrim":
        return F.ltrim(col)
    if name == "rtrim":
        return F.rtrim(col)
    if name == "upper":
        return F.upper(col)
    if name == "lower":
        return F.lower(col)
    if name == "collapse_spaces":           # espaces multiples → un seul (+ trim)
        return F.trim(F.regexp_replace(col, r"\s+", " "))
    if name == "remove_spaces":             # supprime tous les espaces
        return F.regexp_replace(col, r"\s+", "")
    if name == "nullify_empty":             # chaîne vide / blanche → NULL
        return F.when(F.trim(col) == F.lit(""), F.lit(None)).otherwise(col)
    if name == "replace":                   # regex
        return F.regexp_replace(col, args["pattern"], args.get("with", ""))
    if name == "lpad":
        return F.lpad(col, int(args["width"]), args.get("char", "0"))
    if name == "rpad":
        return F.rpad(col, int(args["width"]), args.get("char", "0"))
    raise ValueError(f"Opération de normalisation inconnue : {name}")


def normalize(df: DataFrame, table, config) -> DataFrame:
    """Applique la normalisation déclarative : défauts globaux + override par champ."""
    norm = getattr(config, "normalization", None)
    defaults = (getattr(norm, "defaults", None) or {}) if norm else {}
    default_string_ops = defaults.get("string", []) or []

    string_cols = {f.name for f in df.schema.fields if isinstance(f.dataType, StringType)}
    # None = champ sans clé normalize ; [] = opt-out explicite ; [...] = ops du champ
    field_ops = {f.name: f.normalize for f in table.schema.fields}

    applied = 0
    for fname in df.columns:
        ops = field_ops.get(fname, None)
        if ops is None and fname in string_cols:
            ops = default_string_ops          # défaut sur colonne string non spécifiée
        if not ops:
            continue
        col = F.col(fname)
        for op in ops:
            col = _apply_op(col, op)
        df = df.withColumn(fname, col)
        applied += 1

    if applied:
        print(f"  [NORM] normalisation appliquée sur {applied} colonne(s)")
    return df
