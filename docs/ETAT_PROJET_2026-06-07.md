# État du projet — résultats session du 2026-06-07

Plateforme DATA standardisée Crédit-Garantie (BOA Entreprise) — framework médaillon
Bronze / Silver / Gold. Synthèse des résultats de la session.

## Résumé exécutif

Trois livrables majeurs cette session :
1. **Historisation Silver (H3)** — 6 dimensions passées en SCD2 via un framework unique.
2. **Modèle dimensionnel** — faits / dimensions / OBT figés d'après le diagramme de jointures.
3. **Framework Gold déclaratif** — moteur générique config-driven, mécaniques prouvées.

Tout est committé sur `main`, working tree propre.

---

## 1. Récupération de session

La session précédente (3 semaines de travail, 15 mai → 3 juin) avait dépassé la fenêtre de
contexte (« Prompt is too long »). Le transcript complet a été récupéré sur disque et résumé :
- `docs/recap_session_c57cf119.md` — transcript condensé (749 messages)
- aucune perte : code et historique git intacts.

## 2. Correction du bug SCD2 (NullPointerException Iceberg)

Le MERGE/UPDATE copy-on-write d'**Iceberg 1.3.1** déclenchait un `NullPointerException`
(`UTF8String` null) à la recopie des lignes inchangées. Diagnostic : ni schéma (toutes
colonnes nullable), ni codegen (désactivé = même erreur) → bug de l'opération copy-on-write.

**Correctif** : `write_silver_scd2` reconstruit l'état complet **100% en DataFrame**
(clôture / inchangé / nouvelle version) puis réécrit via `createOrReplace` (écriture bulk,
gère les null, déterministe et portable). Validé sur POPS (mouvement modifié/disparu/nouveau).

## 3. Framework d'historisation unique

`jobs/bronze/historization.py` — point d'entrée `historize()` qui dispatche selon la config :
- **snapshot** (faits) : Iceberg partitionné `date_arrete`, chaque arrêté gardé
- **SCD2** (dimensions) : versionné (valid_from/valid_to/is_current/version/row_hash)
- `scd2_signal.columns` : sous-ensemble de colonnes comparées (optionnel)

`engine.py` allégé : la logique d'écriture est sortie du monolithe.

## 4. H3 — 6 dimensions Silver en SCD2 (validées VM)

Activation par simple flag YAML (`scd2: true` + `scd2_signal: {type: hash}`), zéro code.

| Dimension | Table | PK | Lignes v1 |
|---|---|---|---|
| Produits | `pops` | simple | 1 226 |
| Agents | `asal` | simple | 4 464 |
| Contrat (hub) | `image_contrat` | composite 4 col. | 158 322 |
| Portefeuille | `grc_portefeuille` | composite 3 col. | 151 528 |
| Clients | `tiers` | simple (111 col.) | 154 245 |
| Typologie | `typologie` | simple | 154 249 |

Famille **hash** retenue partout (règle « historiser tout » : tout changement = version).
Famille *interval* reportée (raffinement futur). **Faits** restent en snapshot `date_arrete`.

## 5. Modèle dimensionnel (source : `docs/diagram jointure.jpeg`)

`docs/modele_dimensionnel_credit_garantie.md` — constellation, hub = `image_contrat`.

- **5 faits** : `fact_credit` (credit_amort), `fact_impayes` (impayes_cdl),
  `fact_depassement` (depassement), `fact_garantie` (garantie_detail), `fact_autorisation`
  (idce_autorisation).
- **6 dimensions** : contrat, tiers, produit, typologie, portefeuille, agent.
- **1 pont** : `garantie_contrat`.
- **Ambiguïté n°6 résolue** par le diagramme : `GARANTIE_CONTRAT.IDF_REF_EXT_CREDIT =
  IMAGE_CONTRAT.IDF_REF_EXT_CTR` (CLAUDE.md mis à jour).

## 6. Modèle Gold (étoile + OBT)

`docs/modele_gold_credit_garantie.md` — convention `gdw_<fait>` / `dim_<entité>` /
`bridge_<hub>` / `dim_temps`.

- 5 faits `gdw_*` + 6 dimensions + **OBT `gdw_synthese_credit`** : table large agrégée
  (tous les champs calculables au grain dossier × arrêté) → un nouveau KPI = une formule,
  sans nouveau modèle. L'OBT est un « + » au-dessus des faits atomiques (qui gardent le détail).

## 7. Framework Gold déclaratif (prouvé)

`jobs/gold/engine.py` + `jobs/gold/config_loader.py` + `configs/gold/credit_garantie.yaml`.

Moteur générique **config-driven** : le modèle vit dans le YAML → robuste au changement de
modèle métier (un changement = config, zéro code). 4 types de cibles :

| Type | Rôle | Historisation |
|---|---|---|
| `date` | dim_temps générée | refresh |
| `dimension` | depuis dim SCD2 Silver (état courant / versionné) | refresh |
| `fact` | depuis fait snapshot Silver | snapshot `date_arrete` |
| `obt` | base + dims aplaties + faits 1:N agrégés + dérivés | snapshot `date_arrete` |

**Mécaniques prouvées sur la VM** (arrêté 2026-05-31) :
- `dim_temps` (4 018 jours), `dim_produit` (1 226, SCD2 état courant)
- `gdw_encours` (200 000) — **jointure as-of** : tiers gelé 199 981/200 000, sans fan-out
- `gdw_impayes` (1 700 767)
- `gdw_synthese_credit` (200 000) — **agrégat 1:N** + **dérivé** : `flag_cdl` = 96 474 CDL

## 8. Décisions actées (2026-06-07)

- **Transactionnel → snapshot**, **référentiel → SCD2**. « Historiser tout » sur un fait/OBT
  = snapshot append (chaque arrêté gardé), PAS de SCD2 (qui créerait une fausse version par
  ligne par arrêté). → `dim_credit` abandonné au profit de l'OBT snapshot.
- **Gold = framework déclaratif** (modèle en YAML).
- **BI = Power BI** (remplace Tableau).
- Historisation par couche : Landing/Bronze = brut snapshot `date_arrete` ; Silver = faits
  snapshot + dims SCD2 ; Gold = faits/OBT snapshot (+ dims gelées as-of) + dims SCD2.

## 9. Commits de la session (sur `main`)

| Commit | Objet |
|---|---|
| `68f0c77` | SCD2 dimensions — pilote POPS (réécriture DataFrame anti-NPE) |
| `1a9e59c` | modèle dimensionnel Crédit-Garantie + ambiguïté n°6 résolue |
| `d934b02` | framework d'historisation unique (`historization.py`) |
| `f862856` | SCD2 hash ASAL |
| `2a0f996` | SCD2 hash IMAGE_CONTRAT |
| `c041a2e` | SCD2 hash GRC_PORTEFEUILLE |
| `fe75d48` | SCD2 hash TIERS |
| `a4b1643` | SCD2 hash TYPOLOGIE (6/6 dimensions) |
| `2adaf35` | framework Gold déclaratif (moteur config-driven) |
| `756a3ca` | mécaniques as-of + agrégats OBT prouvées |
| `6c62aba` | mise à jour du plan |

## 10. Point connu à traiter

- **Padding des codes** : `cod_produit` (CREDIT_AMORT) ne matche pas `idf_code_produit`
  (POPS) — codes stockés paddés (ex. `40000      M`). La jointure fait↔dim s'exécute mais
  ne matche pas (`cod_famille` NULL en OBT). À normaliser (TRIM / normalisation Silver).

## 11. Reste à faire

1. **Modèle Gold final** après validation métier (config only) : dims tiers/typologie/agent/
   portefeuille/bridge_contrat (+ ORG quand la table arrivera), faits depassement/garantie/
   autorisation, OBT complet.
2. Fix padding des codes (jointures fait↔dim).
3. Détection de variation inter-arrêtés (diff N vs N-1).
4. Orchestration Airflow, Power BI, gouvernance (Atlas/Ranger), livrable PFE.

### Dimension ORG (à venir)
Non encore disponible. À son arrivée : entrée YAML Bronze/Silver (schéma depuis son canevas)
+ `scd2: true` → dimension SCD2 ; cible `dim_organisation` en Gold. Jointure
`grc_portefeuille.idf_num_gui → org.idf_num_gui`. Zéro code (framework).
