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

Hub `image_contrat` (clé `IDF_REF_EXT_CTR`) :
- `credit_amort.NUM_COMPTE_EMPRUNTEUR` = `image_contrat.IDF_REF_EXT_CTR`
- `depassement.NUM_COMPTE` = `image_contrat.IDF_REF_EXT_CTR`
- `idce_autorisation.IDF_REF_AUTO` = `image_contrat.IDF_REF_EXT_CTR`
- `garantie_contrat.IDF_REF_EXT_CREDIT` = `image_contrat.IDF_REF_EXT_CTR` *(résout l'ambiguïté n°6 → option a)*

Tiers et produits :
- `image_contrat.IDF_NUM_TIERS` = `tiers.IDF_NUM_TIERS`
- `image_contrat.IDF_NUM_TIERS` = `typologie.IDF_NUM_TIERS`
- `credit_amort.COD_PRODUIT` = `pops.IDF_CODE_PRODUIT`

Organisation et garanties :
- `tiers.IDF_NUM_TIERS` = `portefeuille (grc_portefeuille).IDF_NUM_TIERS`
- `grc_portefeuille.IDF_NUM_GUI` = `org.IDF_NUM_GUI`
- `tiers.IDF_NUM_TIERS` = `garantie_tiers.IDF_NUM_TIERS`
- `garantie_tiers.IDF_NUM_CTR_GARANTIE` = `garantie_detail.IDF_NUM_CTR_GARANTIE`
- `garantie_contrat.IDF_NUM_CTR_GARANTIE` = `garantie_detail.IDF_NUM_CTR_GARANTIE`

Chaînage fait↔fait :
- `impayes_cdl.IDF_NUM_DOSS` = `credit_amort.IDF_NUM_DOSS`

## Schéma (vue d'ensemble)

```
   ORG ──IDF_NUM_GUI── grc_portefeuille ──IDF_NUM_TIERS──┐
                                                          │
   dim_typologie ──IDF_NUM_TIERS──────────────────── dim_tiers
                                                          │ IDF_NUM_TIERS
   dim_produit ──COD_PRODUIT/refs──┐                      │
                                   ▼                      ▼
              ┌──────────── dim_contrat (IMAGE_CONTRAT, IDF_REF_EXT_CTR) ───────────┐
              │                    ▲           ▲              ▲                       │
   fact_credit       fact_depassement   fact_autorisation   garantie_contrat (pont) │
   (NUM_COMPTE_       (NUM_COMPTE)        (IDF_REF_AUTO)      (IDF_REF_EXT_CREDIT)    │
    EMPRUNTEUR)            │                                       │                  │
        │ IDF_NUM_DOSS     │                                  fact_garantie          │
   fact_impayes            │                                  (garantie_detail)      │
                           └─────────── tous via IDF_REF_EXT_CTR ───────────────────┘
```

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
