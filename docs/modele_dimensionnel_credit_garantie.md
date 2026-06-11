# Modèle dimensionnel — axe Crédit-Garantie

> Source de vérité : `docs/diagram jointure.jpeg`. Toutes les jointures ci-dessous
> sont celles du diagramme validé. Aucune jointure inventée.

## Principe

Schéma en **constellation** : plusieurs tables de faits partagent des dimensions
conformes. Le hub central est **IMAGE_CONTRAT** (`dim_contrat`), qui relie chaque fait
aux tiers et aux produits via la clé contrat `IDF_REF_EXT_CTR`.

- **Faits** : portent les mesures, restent en **snapshot** partitionné par `date_arrete`.
- **Dimensions** : référentiels descriptifs, passent en **SCD2** (historisation des versions).
- **Ponts** : tables de liaison sans mesure (relient faits et dimensions).

## Classification des tables

### Faits (snapshot, grain = `date_arrete`)
| Fait | Table Silver | Mesure(s) (diagramme) |
|---|---|---|
| `fact_credit` | `credit_amort` | `MNT_CAPITAL_REST_DU` |
| `fact_impayes` | `impayes_cdl` | `MNT_IMPAYES_CDL` |
| `fact_depassement` | `depassement` | `MNT_DEPASSEMENT` |
| `fact_garantie` | `garantie_detail` | `MNT_INITIAL`, `MNT_ACTUALISE` |
| `fact_autorisation` | `idce_autorisation` | crédits non amortissables (autorisations) |

### Dimensions (SCD2)
| Dimension | Table Silver | PK métier | Famille SCD2 |
|---|---|---|---|
| `dim_contrat` | `image_contrat` | `IDF_REF_EXT_CTR` (+ tiers/produit) | hash |
| `dim_tiers` | `tiers` | `IDF_NUM_TIERS` | hash |
| `dim_produit` | `pops` | `IDF_CODE_PRODUIT` | hash ✅ (pilote) |
| `dim_typologie` | `typologie` | `IDF_NUM_TIERS` (1:1 tiers) | à confirmer (interval si DATE_EFFET/FIN, sinon hash) |
| `dim_portefeuille` | `grc_portefeuille` | `IDF_NUM_GUI` (+ ORG) | hash |
| `dim_agent` | `asal` | agent | hash (hors diagramme — dimension autonome) |

### Ponts (liaison, sans mesure)
| Pont | Table | Relie |
|---|---|---|
| contrat↔garantie | `garantie_contrat` | `garantie_detail` ↔ `image_contrat` |
| tiers↔garantie | `garantie_tiers` *(non industrialisée)* | `tiers` ↔ `garantie_detail` |

## Jointures (d'après le diagramme)

> **Révision 2026-06-11** — toutes les jointures testables ont été **validées
> empiriquement** contre la donnée réelle (arrêté 2026-05-31) par
> `scripts/test_jointures_diagramme.py` et `scripts/diag_jointures_ko.py`.
> 3 jointures du diagramme étaient fausses (rapprochement par similarité de noms) ;
> elles sont corrigées ci-dessous avec les taux de match.

Hub `image_contrat` (clé `IDF_REF_EXT_CTR`) :
- `credit_amort.NUM_COMPTE_EMPRUNTEUR` = `image_contrat.IDF_REF_EXT_CTR` — **99,99%**
- `depassement.NUM_COMPTE` = `image_contrat.IDF_REF_EXT_CTR` — **100%**
- ⚠️ CORRIGÉE : `idce_autorisation.`**`NUM_COMPTE`** = `image_contrat.IDF_REF_EXT_CTR` — **100%**
  *(le diagramme disait IDF_REF_AUTO → 0% : c'est la PK interne de l'autorisation)*

Garantie → crédit (ambiguïté n°6 — **option b confirmée**) :
- ⚠️ CORRIGÉE : `garantie_contrat.IDF_REF_EXT_CREDIT` = **`credit_amort.IDF_NUM_DOSS`** — **100%**
  *(lien DIRECT garantie↔crédit ; l'option a du diagramme — via le hub — donnait 0%.
  EDD : « Référence BTA ». NUM_CTR_CREDIT est « Non exploitée » selon l'EDD → 0%)*

Tiers et produits :
- `image_contrat.IDF_NUM_TIERS` = `tiers.IDF_NUM_TIERS` — **100%**
- `image_contrat.IDF_NUM_TIERS` = `typologie.IDF_NUM_TIERS` — **100%**
- ⚠️ CORRIGÉE : `credit_amort.COD_PRODUIT` = `pops.`**`COD_SOP`** — **99,49%**
  *(le diagramme disait IDF_CODE_PRODUIT → 0% : c'est un « code activité » selon l'EDD ;
  COD_PRODUIT est le code produit du système opérant)*
- `CONCAT(image_contrat.IDF_TYP_PRD, IDF_REF_PRD)` = `pops.IDF_CODE_PRODUIT` — **100%**
  *(chemin officiel encadrant : type du produit d'un contrat)*
- `idce_autorisation.NUM_TIERS` = `tiers.IDF_NUM_TIERS` — **99,96%**

Organisation et garanties :
- `tiers.IDF_NUM_TIERS` = `grc_portefeuille.IDF_NUM_TIERS` — **100%**
- `grc_portefeuille.IDF_NUM_GUI` = `org.IDF_NUM_GUI` — **100%** (ORG ingérée via NiFi, 762 lignes, dimension SCD2)
- `tiers ↔ garantie_tiers` — ⏳ non testable (table non industrialisée)
- `garantie_contrat.IDF_NUM_CTR_GARANTIE` = `garantie_detail.IDF_NUM_CTR_GARANTIE` — **99,91%**

Chaînage fait↔fait :
- `impayes_cdl.IDF_NUM_DOSSIER` = `credit_amort.IDF_NUM_DOSS` — **100%**

## Schéma (vue d'ensemble, jointures validées)

```
   dim_organisation(ORG) ──IDF_NUM_GUI── grc_portefeuille ──IDF_NUM_TIERS──┐
                                                              │
   dim_typologie ──IDF_NUM_TIERS────────────────────────  dim_tiers ◄──NUM_TIERS── fact_autorisation
                                                              │ IDF_NUM_TIERS
   dim_produit ◄──CONCAT(typ,ref)=IDF_CODE_PRODUIT──┐        │
        ▲                                           │        ▼
        │ COD_PRODUIT=COD_SOP (99,5%)    ┌── dim_contrat (IMAGE_CONTRAT, IDF_REF_EXT_CTR) ──┐
        │                                │          ▲                ▲                       │
   fact_credit ──NUM_COMPTE_EMPRUNTEUR───┘   fact_depassement   fact_autorisation            │
        ▲  ▲                                  (NUM_COMPTE)       (NUM_COMPTE)                │
        │  └──IDF_REF_EXT_CREDIT=IDF_NUM_DOSS── garantie_contrat (pont)                      │
        │ IDF_NUM_DOSSIER                            │ IDF_NUM_CTR_GARANTIE                  │
   fact_impayes                                 fact_garantie (garantie_detail)              │
```

Changements structurels vs diagramme initial :
1. **La garantie se relie au crédit en direct** (IDF_NUM_DOSS), pas via le hub.
2. **L'autorisation rejoint le hub par NUM_COMPTE** (et les tiers par NUM_TIERS).
3. **Le produit du crédit se joint sur COD_SOP** ; la PK POPS (code activité) sert
   au chemin hub CONCAT(typ, ref).

## Conséquences pour le sprint H3 (SCD2)

- **6 dimensions** passent en SCD2 : `pops` (fait ✅), `tiers`, `image_contrat`,
  `grc_portefeuille`, `asal` (famille hash), `typologie` (famille à confirmer).
- **5 faits** restent en snapshot `date_arrete` : `credit_amort`, `impayes_cdl`,
  `depassement`, `garantie_detail`, `idce_autorisation`.
- **1 pont** : `garantie_contrat` (snapshot, sans SCD2).

## Points ouverts

1. **Famille SCD2 de `typologie`** : le diagramme ne montre que `COD_MARCHE`. Vérifier
   dans l'EDD la présence de `DATE_EFFET`/`DATE_FIN` → famille interval, sinon hash.
2. **`dim_contrat` vs pont pur** : `image_contrat` est modélisé en dimension (hub porteur
   d'identité contrat). Alternative : factless/bridge. Retenu = dimension SCD2.
3. **`garantie_tiers`** présente au diagramme mais non industrialisée en Silver — à
   intégrer si la chaîne garantie↔tiers est requise pour le Gold.
