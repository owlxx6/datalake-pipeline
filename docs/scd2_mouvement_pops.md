# Historisation SCD2 — démonstration sur la dimension POPS

## Objet

Validation du pattern **SCD2 (Slowly Changing Dimension type 2)** appliqué à la couche
Silver, sur la dimension produits `silver_credit_garantie.pops`. Chaque changement d'un
attribut historisé crée une nouvelle version ; aucune donnée n'est écrasée ni supprimée.

## Principe

À chaque arrêté, le moteur compare la **photo du jour** à l'**état courant** de la table
(lignes `is_current = true`), via une empreinte (`row_hash`) calculée sur les champs à
historiser. Quatre cas :

| Cas source | Action SCD2 |
|---|---|
| Clé nouvelle | INSERT version 1, `is_current = true` |
| Clé connue, hash identique | aucun mouvement |
| Clé connue, hash différent | clôture de la version courante + INSERT nouvelle version |
| Clé disparue de la photo | clôture de la version courante (soft delete) |

## Colonnes techniques SCD2

| Colonne | Rôle |
|---|---|
| `row_hash` | empreinte SHA-256 des champs historisés (signal de changement) |
| `valid_from` | date d'arrêté d'ouverture de la version |
| `valid_to` | date d'arrêté de clôture (`NULL` si version courante) |
| `is_current` | `true` pour la version active, `false` sinon |
| `version` | numéro de version incrémental par clé |
| `load_ts` | horodatage technique de chargement |

## Scénario de démonstration

Deux arrêtés sur POPS :
- **2026-05-31** : chargement initial — 1226 produits, tous en version 1.
- **2026-06-30** : photo modifiée (script `scripts/simulate_pops_arrete2.py`) —
  3 produits modifiés (`COD_USAGE` basculé), 1 produit retiré, 1 produit ajouté.

Résultat du MERGE SCD2 : **4 nouvelles versions, 1230 lignes au total**.

## Mouvement observé (requête de contrôle)

```sql
SELECT TRIM(idf_code_produit) AS code, version, is_current, valid_from, valid_to
FROM silver_credit_garantie.pops
WHERE is_current = false OR version > 1 OR TRIM(idf_code_produit) = 'ZZZNEW001'
ORDER BY code, version;
```

| code | version | is_current | valid_from | valid_to |
|---|---|---|---|---|
| 40000 M | 1 | false | 2026-05-31 | 2026-06-30 |
| 40000 M | 2 | true | 2026-06-30 | NULL |
| 41110 M | 1 | false | 2026-05-31 | 2026-06-30 |
| 41110 M | 2 | true | 2026-06-30 | NULL |
| 41201 M | 1 | false | 2026-05-31 | 2026-06-30 |
| 41201 M | 2 | true | 2026-06-30 | NULL |
| 41211 M | 1 | false | 2026-05-31 | 2026-06-30 |
| ZZZNEW001 | 1 | true | 2026-06-30 | NULL |

Lecture :
- **40000 / 41110 / 41201** (modifiés) : la v1 est clôturée au 2026-06-30, une v2 courante
  est ouverte. L'historique de la valeur précédente reste consultable.
- **41211** (disparu) : v1 clôturée, pas de nouvelle version — le produit n'est plus actif
  mais reste tracé.
- **ZZZNEW001** (nouveau) : v1 courante.

> Note : `idf_code_produit` est stocké **paddé** (largeur fixe) conformément à la source.
> Utiliser `TRIM()` pour les filtres par code.

## Requêtes types

État courant (vision « aujourd'hui ») :
```sql
SELECT * FROM silver_credit_garantie.pops WHERE is_current = true;
```

Photo à une date passée (vision historique métier) :
```sql
SELECT * FROM silver_credit_garantie.pops
WHERE valid_from <= '2026-06-15'
  AND (valid_to IS NULL OR valid_to > '2026-06-15');
```

Historique complet d'un produit :
```sql
SELECT version, is_current, valid_from, valid_to, cod_usage
FROM silver_credit_garantie.pops
WHERE TRIM(idf_code_produit) = '40000'
ORDER BY version;
```

## Implémentation

Fonction `write_silver_scd2` dans `jobs/bronze/engine.py`.

Le mouvement est calculé **intégralement en DataFrame** (classement clôture / inchangé /
nouvelle version), puis la table est réécrite via `createOrReplace`. Le `MERGE` / `UPDATE`
SQL copy-on-write d'Iceberg **n'est pas utilisé** : sur Iceberg 1.3.1 il déclenche un
`NullPointerException` (`UTF8String` null) lors de la recopie des lignes inchangées. La
réécriture bulk gère correctement les valeurs nulles et reste déterministe et portable.

Activation par table (YAML) :
```yaml
silver_write:
  scd2: true
  scd2_signal:
    type: hash
```

Pilote validé : **POPS**. Famille « hash » (empreinte des attributs métier).
