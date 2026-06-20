# Guide Power BI — Dashboards Crédit-Garantie

Recette pour monter les 5 dashboards à partir des CSV exportés (`exports/`), en **mode Import**.
Colonnes = en-têtes réels des CSV (toutes en minuscules).

---

## 0. Import des CSV
Power BI Desktop → **Obtenir les données → Texte/CSV** → importer les 11 fichiers de `exports/` :

| Fichier | Rôle |
|--|--|
| `gdw_synthese_credit.csv` | **fait central** (OBT : 1 ligne/dossier, déjà enrichie) |
| `gdw_impayes.csv` | fait impayés (détail créance) |
| `fait_garanties.csv` | fait garanties |
| `fait_autorisations.csv` | fait autorisations |
| `fait_depassement.csv` | fait dépassements |
| `dim_temps.csv` | dimension date |
| `dim_produit.csv` | dimension produit |
| `dim_tiers.csv` | dimension client |
| `dim_organisation.csv` | dimension réseau (agences) |
| `dim_portefeuille.csv` | pont tiers → agence (pour « par agence ») |
| `br_credit_garantie_quality_results.csv` | audit DQ (dashboard Data Quality) |

> `gdw_encours` et `v_kpi_credit` ne sont **pas exportés** (doublons) : le premier est déjà
> inclus dans l'OBT, le second n'est que des ratios (= mesures DAX, cf. §2).

- Encodage : **65001 (UTF-8)**.
- Vérifier les types : montants → **Nombre décimal** ; `date_accord`, `date_arrete`,
  `date_mise_impaye`, `date_echeance` → **Date**.
- Table de faits crédit centrale = **`gdw_synthese_credit`** (OBT : 1 ligne/dossier, déjà enrichie).

---

## 1. Modèle (relations à créer dans l'onglet *Modèle*)

| De (fait) | Vers (dim/fait) | Clés | Cardinalité |
|--|--|--|--|
| gdw_synthese_credit[cod_produit] | dim_produit[cod_sop] | produit | * → 1 |
| gdw_synthese_credit[idf_num_tiers] | dim_tiers[idf_num_tiers] | tiers | * → 1 |
| gdw_synthese_credit[date_accord] | dim_temps[date] | temps | * → 1 |
| gdw_impayes[idf_num_doss] | gdw_synthese_credit[idf_num_doss] | dossier | * → 1 |
| fait_garanties[idf_ref_ext_credit] | gdw_synthese_credit[idf_num_doss] | garantie→crédit | * → 1 |
| fait_autorisations[num_tiers] | dim_tiers[idf_num_tiers] | tiers | * → 1 |
| fait_depassement[num_compte] | gdw_synthese_credit[num_compte_emprunteur] | compte | * → 1 |
| gdw_synthese_credit[idf_num_tiers] | dim_portefeuille[idf_num_tiers] | tiers→agence | * → 1 |
| dim_portefeuille[idf_num_gui] | dim_organisation[idf_num_gui] | agence | * → 1 |

> **Chaîne « par agence »** : `gdw_synthese_credit → dim_portefeuille → dim_organisation`
> (le crédit pointe son tiers, le pont donne le guichet, l'organisation donne le libellé d'agence).
> `dim_portefeuille` est unique par `idf_num_tiers` (SCD2 courant) → relation `* → 1` propre.
> ⚠️ `dim_organisation` est au grain (guichet × bureau) : si Power BI signale la relation
> `idf_num_gui` en *plusieurs-à-plusieurs*, utilise directement `dim_portefeuille[idf_num_gui]`
> comme axe « agence » (code guichet), ou filtre `dim_organisation` à une ligne par guichet.

---

## 2. Mesures DAX (à coller — *Nouvelle mesure*)

```DAX
Encours          = SUM(gdw_synthese_credit[encours])
Montant Crédit   = SUM(gdw_synthese_credit[mnt_credit])
Nombre Crédits   = DISTINCTCOUNT(gdw_synthese_credit[idf_num_doss])
Ticket Moyen     = DIVIDE([Montant Crédit], [Nombre Crédits])

Montant Impayés  = SUM(gdw_impayes[mnt_impayes_cdl])
Montant Créances = SUM(gdw_impayes[mnt_creance])
Taux Impayé      = DIVIDE([Montant Impayés], [Encours])

Montant en Défaut = CALCULATE([Encours], gdw_synthese_credit[flag_cdl] = TRUE())
NPL              = DIVIDE([Montant en Défaut], [Encours])

Valeur Garanties = SUM(fait_garanties[mnt_total_garantie])
Taux Couverture  = DIVIDE([Valeur Garanties], [Encours])
Risque Net       = [Encours] - [Valeur Garanties]

Montant Autorisé = SUM(fait_autorisations[montant_autorise])
Montant Utilisé  = SUM(fait_autorisations[montant_utilise])
Taux Utilisation = DIVIDE([Montant Utilisé], [Montant Autorisé])

Montant Dépassement = SUM(fait_depassement[mnt_depassement])
```

**Colonne calculée** (table gdw_impayes) pour libeller les classes CDL :
```DAX
Classe CDL = SWITCH(gdw_impayes[cod_statut_incident],
  "PD","1-Pré-douteux", "DT","2-Douteux", "CP","3-Compromis",
  "IM","0-Impayé", "RB","Sain", BLANK())
```
(NPL « réglementaire » par classe : filtrer sur `Classe CDL ∈ {Pré-douteux,Douteux,Compromis}`.)

**Aging calendaire** (colonnes calculées sur `gdw_impayes`) — ancienneté de l'impayé en jours
depuis la mise en impayé jusqu'à l'arrêté, puis tranches Bâle :
```DAX
Aging Jours = DATEDIFF(gdw_impayes[date_mise_impaye], gdw_impayes[date_arrete], DAY)

Tranche Aging = SWITCH(TRUE(),
  ISBLANK(gdw_impayes[date_mise_impaye]), "Non daté",
  gdw_impayes[Aging Jours] <= 30,  "0-30j",
  gdw_impayes[Aging Jours] <= 60,  "30-60j",
  gdw_impayes[Aging Jours] <= 90,  "60-90j",
  "90j+")
```
> Prérequis : `date_mise_impaye` et `date_arrete` typés **Date** à l'import (§0). `date_echeance`
> est aussi disponible si tu préfères calculer l'ancienneté depuis l'échéance impayée.

---

## 3. Visuels par dashboard

### D1 — Pilotage Crédit Consolidé
- Cartes : `[Encours]`, `[Montant Impayés]`, `[Valeur Garanties]`, `[Risque Net]`
- Courbe : `[Encours]` & `[Montant Impayés]` par `dim_temps[mois]` (tendance)
- Camembert : `[Encours]` par `dim_produit[lib_famille]`
- Table : `dim_tiers[raison_sociale]` × `[Montant en Défaut]` (tri desc → Top clients à risque)

### D2 — Suivi Risque & Impayés
- Cartes : `[Taux Impayé]`, `[NPL]`, `[Montant en Défaut]`
- Barres : `[Montant Impayés]` par `gdw_impayes[Classe CDL]` (= aging réglementaire)
- Barres : `[Montant Impayés]` par `gdw_impayes[Tranche Aging]` (= aging calendaire 0-30/30-60/60-90/90j+)
- Matrice : lignes `dim_tiers[cod_catg_jur]` × colonnes `dim_produit[lib_famille]`, valeur `[Taux Impayé]`
- Table : `gdw_impayes[idf_num_creance]` × `[Montant Créances]` filtré souffrance (Top créances)

### D3 — Suivi Production Crédit
- Cartes : `[Nombre Crédits]`, `[Montant Crédit]`, `[Ticket Moyen]`
- Histogramme : `[Montant Crédit]` par `dim_temps[mois]` (production mensuelle, via date_accord)
- Barres : `[Montant Crédit]` par `dim_produit[lib_famille]`
- Barres : `[Montant Crédit]` (ou `[Encours]`) par `dim_organisation[lib_court_gui]` (= **par agence**,
  via la chaîne `dim_portefeuille`) ; à défaut, par `dim_portefeuille[idf_num_gui]` (code guichet)

### D4 — Suivi des Autorisations
- Cartes : `[Montant Autorisé]`, `[Montant Utilisé]`, `[Taux Utilisation]`, `[Montant Dépassement]`
- Barres : `[Montant Autorisé]` vs `[Montant Utilisé]` par `fait_autorisations[cod_typ_auto]`
- Table : dépassements `fait_depassement[num_compte]` × `[Montant Dépassement]`

### D5 — Suivi des Garanties
- Cartes : `[Valeur Garanties]`, `[Taux Couverture]`, `[Risque Net]`
- Camembert : `[Valeur Garanties]` par `fait_garanties[cod_typ_garant]` (hypothèque/caution/…)
- Barres : `[Valeur Garanties]` par `dim_produit[lib_famille]` (garanties par produit)
- Nuage de points : `[Valeur Garanties]` vs `[Encours]` par dossier (couverture vs encours)

### Bonus — Dashboard Data Quality (`br_credit_garantie_quality_results`)
- Cartes : nb règles `PASSED` / `WARNING` / `REJECTED` (mesures `COUNTROWS` filtrées sur `status`)
- Barres : `records_failed` par `table_name` / `rule_type`

---

## 4. Limites connues (honnêtes) & petits ajouts

| Sujet | État | Détail |
|--|--|--|
| Aging **calendaire** (0-30/30-60/60-90/90j+) | ✅ | `date_mise_impaye` + `date_echeance` dans `gdw_impayes` → colonne `Tranche Aging` (§2) |
| Aging **par classe CDL** | ✅ | `cod_statut_incident` déjà présent |
| Par agence (D3) | ✅ | `dim_portefeuille` exporté → chaîne `dim_portefeuille → dim_organisation` (§1) |
| Segment fin (marché) | ⚠️ grossier | `dim_tiers` porte `cod_catg_jur` ; `COD_MARCHE` (typologie) = enrichissement |
| Taux de recouvrement / Taux d'acceptation / funnel | ⛔ | donnée absente (cf. rapport modélisation §6) |

> Tendances multi-arrêtés (Encours vs Impayés dans le temps) : il n'y a qu'**un arrêté**
> (2026-05-31) chargé → courbe à un point. Charger d'autres arrêtés pour l'évolution.
> (La production mensuelle, elle, marche : elle s'appuie sur `date_accord`.)
