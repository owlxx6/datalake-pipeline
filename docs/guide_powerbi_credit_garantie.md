# Guide Power BI — Dashboards Crédit-Garantie

Recette pour monter les 5 dashboards à partir des CSV exportés (`exports/`), en **mode Import**.
Colonnes = en-têtes réels des CSV (toutes en minuscules).

---

## 0. Import des CSV
Power BI Desktop → **Obtenir les données → Texte/CSV** → importer les 11 fichiers de `exports/` :
`dim_temps, dim_produit, dim_organisation, dim_tiers, gdw_synthese_credit, gdw_impayes,
fait_garanties, fait_autorisations, fait_depassement` (+ `gdw_encours` si besoin, redondant avec
l'OBT) (+ `br_credit_garantie_quality_results` pour le dashboard DQ).

- Encodage : **65001 (UTF-8)**.
- Vérifier les types : montants → **Nombre décimal** ; `date`, `date_accord`, `date_arrete` → **Date**.
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

> **dim_organisation** : pas de lien direct (le rattachement agence passe par `grc_portefeuille`,
> non exporté). Pour « Par agence » (D3) → exporter `grc_portefeuille` et relier
> `idf_num_tiers → idf_num_gui → dim_organisation`. (Petit ajout, cf. §4.)

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
- Matrice : lignes `dim_tiers[cod_catg_jur]` × colonnes `dim_produit[lib_famille]`, valeur `[Taux Impayé]`
- Table : `gdw_impayes[idf_num_creance]` × `[Montant Créances]` filtré souffrance (Top créances)

### D3 — Suivi Production Crédit
- Cartes : `[Nombre Crédits]`, `[Montant Crédit]`, `[Ticket Moyen]`
- Histogramme : `[Montant Crédit]` par `dim_temps[mois]` (production mensuelle, via date_accord)
- Barres : `[Montant Crédit]` par `dim_produit[lib_famille]`
- (Par agence : après ajout `grc_portefeuille`, cf. §4)

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

| Sujet | État | Pour le débloquer |
|--|--|--|
| Aging **calendaire** (0-30/30-60/90j+) | ❌ `date_mise_impaye` pas dans `gdw_impayes` | ajouter `date_mise_impaye` au fait `gdw_impayes` (YAML) + ré-export → bucket `DATEDIFF` |
| Aging **par classe CDL** | ✅ | `cod_statut_incident` déjà présent |
| Par agence (D3) | ❌ pont manquant | exporter `grc_portefeuille` + relier à `dim_organisation` |
| Segment fin (marché) | ⚠️ grossier | `dim_tiers` porte `cod_catg_jur` ; `COD_MARCHE` (typologie) = enrichissement |
| Taux de recouvrement / Taux d'acceptation / funnel | ⛔ | donnée absente (cf. rapport modélisation §6) |

> Tendances multi-arrêtés (Encours vs Impayés dans le temps) : il n'y a qu'**un arrêté**
> (2026-05-31) chargé → courbe à un point. Charger d'autres arrêtés pour l'évolution.
> (La production mensuelle, elle, marche : elle s'appuie sur `date_accord`.)
