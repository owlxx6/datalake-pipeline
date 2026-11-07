# Modèle Gold (étoile) — axe Crédit-Garantie

> Schéma en constellation : faits `gdw_*` (mesures, grain `date_arrete`) + dimensions
> conformes `dim_*` / `bridge_*`. Colonnes issues des canevas (EDD), sélection curatée
> (le Gold n'embarque pas toutes les colonnes Bronze/Silver, seulement l'utile BI).
> Conventions : `gdw_<fait>`, `dim_<entité>`, `bridge_<hub>`, `dim_temps` (date).

---

## DIMENSIONS

### dim_temps  (dimension date conforme — générée)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| date_key | INT | PK | clé technique AAAAMMJJ |
| date | DATE | | date pleine |
| jour / mois / trimestre / annee | INT | | éléments calendaires |
| lib_mois | STRING | | libellé mois |
| date_arrete | DATE | | arrêté Bâle III |

### dim_produit  ← POPS  (SCD2 hash)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_code_produit | STRING | PK | code produit POPS |
| cod_activite / lib_activite | STRING | | activité |
| cod_famille / lib_famille | STRING | | famille produit |
| prod_commercial / lib_produit | STRING | | produit commercial |
| cod_sop / nom_sop | STRING | | système opérant |
| cod_usage / cod_typ_produit | STRING | | usage / type |

### dim_tiers  ← TIERS  (SCD2 hash — 111 col. source, curaté)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_num_tiers | STRING | PK | identifiant tiers |
| cod_typ_tiers / cod_nat_tiers | STRING | | type / nature (PP/PM) |
| cod_catg_jur / cod_statut_tiers | STRING | | catégorie juridique / statut |
| raison_sociale / nom_usg_tiers / prenom_tiers | STRING | | identité |
| cod_sex / date_naissance | STRING/DATE | | personne physique |
| cod_activite / cod_sec_activite | STRING | | activité (PM) |
| code_guichet / code_federation | STRING | | rattachement |
| date_anciennete | DATE | | ancienneté relation |

### dim_typologie  ← TYPOLOGIE  (SCD2 **interval** — DATE_EFFET/FIN)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_num_tiers | STRING | PK/FK | tiers (1:1) |
| cod_marche / cod_ss_marche | STRING | | marché |
| date_effet_marche / date_fin_marche | DATE | | bornes validité marché |
| cod_stn / cod_ss_stn | STRING | | segment |
| date_effet_stn / date_fin_stn | DATE | | bornes validité segment |
| cod_indic_activ | STRING | | indicateur activité |

### dim_agent  ← ASAL  (SCD2 hash)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_personnel | STRING | PK | identifiant agent |
| nom_personnel / prn_personnel | STRING | | identité |
| adr_email / matricule | STRING | | contacts |
| num_guichet / num_bureau | STRING | | affectation |
| code_rattachement / code_federation | STRING | | rattachement org |

### dim_portefeuille  ← GRC_PORTEFEUILLE (+ ORG)  (SCD2 hash)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_num_gui | STRING | PK | guichet / portefeuille |
| idf_num_tiers | STRING | PK/FK | tiers rattaché → dim_tiers |
| idf_code_cc | STRING | PK | code chargé de clientèle |
| date_rattachement | DATE | | début rattachement |
| cod_typ_rattach | STRING | | type de rattachement |

### bridge_image_contrat  ← IMAGE_CONTRAT  (hub central, SCD2 hash)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_ref_ext_ctr | STRING | PK | n° contrat (clé de liaison des faits) |
| idf_num_tiers | STRING | FK | tiers → dim_tiers |
| idf_typ_prd / idf_ref_prd | STRING | FK | produit → dim_produit (CONCAT) |
| cod_cyc_vie / date_cyc_vie | STRING/DATE | | cycle de vie du contrat |
| date_ouv_ctr / date_creation | DATE | | ouverture |
| cod_role_tiers | STRING | | rôle du tiers sur le contrat |

---

## FAITS

### gdw_encours  ← CREDIT_AMORT  (grain : dossier × arrêté)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_num_doss | STRING | PK | identifiant dossier |
| num_doss_cdl | STRING | | n° dossier CDL |
| num_compte_emprunteur | STRING | FK | → bridge_image_contrat |
| cod_produit | STRING | FK | → dim_produit |
| date_accord | DATE | FK | → dim_temps |
| duree | BIGINT | | durée (mois) |
| cod_sit_doss | STRING | | situation (2=actif) |
| mnt_capital_rest_du | DECIMAL(18,3) | | **encours** (mesure) |
| mnt_credit | DECIMAL(18,3) | | montant initial (mesure) |
| tx_credit | DECIMAL(18,3) | | taux d'intérêt |
| nbr_impayes | BIGINT | | nb impayés (mesure) |
| dt_load | DATE | | date chargement |

### gdw_impayes  ← IMPAYES_CDL  (grain : créance × arrêté)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_num_dossier | STRING | PK/FK | dossier → gdw_encours |
| idf_num_creance | STRING | PK | n° créance |
| num_compte | STRING | FK | → bridge_image_contrat |
| date_creance | DATE | FK | → dim_temps |
| cod_statut_incident | STRING | | statut incident |
| nat_declassement | STRING | | nature déclassement (CDL) |
| mnt_impayes_cdl | DECIMAL(18,3) | | **montant impayé** (mesure) |
| mnt_creance | DECIMAL(18,3) | | montant créance (mesure) |
| mnt_interet_retard | DECIMAL(18,3) | | intérêts de retard (mesure) |
| mnt_penalite_retard | DECIMAL(18,3) | | pénalités (mesure) |
| dt_load | DATE | | date chargement |

### gdw_depassement  ← DEPASSEMENT  (grain : dépassement × arrêté)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| num_compte | STRING | PK/FK | → bridge_image_contrat |
| date_entree | DATE | PK/FK | entrée → dim_temps |
| date_sortie | DATE | | sortie du dépassement |
| compte_general | STRING | PK | compte général |
| mnt_depassement | DECIMAL(18,3) | PK | **montant dépassement** (mesure) |
| dt_load | DATE | | date chargement |

### gdw_garantie  ← GARANTIE_DETAIL (+ GARANTIE_CONTRAT)  (grain : garantie × arrêté)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_num_ctr_garantie | STRING | PK | n° contrat de garantie |
| idf_ref_ext_credit | STRING | FK | crédit garanti → bridge_image_contrat |
| cod_nat_garantie / cod_typ_garant | STRING | | nature / type |
| cod_statut_garantie | STRING | | statut |
| date_prise_effet / date_echeance | DATE | FK | → dim_temps |
| mnt_initial | DECIMAL(18,3) | | **montant initial garanti** (mesure) |
| mnt_actualise | DECIMAL(18,3) | | **montant actualisé** (mesure) |
| mnt_total_garantie | DECIMAL(18,3) | | montant total (mesure) |
| pourcentage_garantie | DECIMAL(18,3) | | quotité garantie |
| dt_load | DATE | | date chargement |

### gdw_autorisation  ← IDCE_AUTORISATION  (grain : autorisation × arrêté)
| Colonne | Type | Clé | Description |
|---|---|---|---|
| idf_ref_auto | STRING | PK/FK | autorisation → bridge_image_contrat |
| num_tiers | STRING | FK | → dim_tiers |
| ref_produit | STRING | | produit autorisé |
| cod_typ_auto / cod_etat_auto / cod_nat_auto | STRING | | type / état / nature |
| date_comite / date_ouverture / date_echeance | DATE | FK | → dim_temps |
| mnt_auto_tot_dev | DECIMAL(18,3) | | **montant autorisé total** (mesure) |
| mnt_auto_deb_dev | DECIMAL(18,3) | | montant débloqué (mesure) |
| mnt_com / mnt_frais | DECIMAL(18,3) | | commissions / frais (mesure) |
| dt_load | DATE | | date chargement |

---

## Table de faits agrégée — `gdw_synthese_credit` (One Big Table)

But : **réunir tous les champs calculables** à un grain unique pour qu'un nouveau KPI
demandé par le métier se calcule **par simple formule sur cette table**, sans refaire le
modèle ni rejoindre les faits.

- **Grain** : un crédit (`idf_num_doss`) × `date_arrete` — une ligne par crédit par arrêté.
- **Construction** : `gdw_encours` (1:1) **+** dimensions aplaties (tiers, produit,
  typologie, portefeuille) **+** faits 1:N **agrégés** au dossier/contrat
  (impayés, garanties, dépassements, autorisations).
- **Rafraîchissement** : recalculée à chaque arrêté (snapshot).

### Clés & axes d'analyse (dimensions aplaties — slicing sans jointure)
| Colonne | Type | Origine | Description |
|---|---|---|---|
| idf_num_doss | STRING | encours | PK dossier crédit |
| date_arrete | DATE | — | arrêté (grain) |
| idf_ref_ext_ctr | STRING | bridge | contrat |
| idf_num_tiers | STRING | bridge | tiers |
| idf_code_produit | STRING | encours | produit |
| cod_typ_tiers / cod_activite | STRING | dim_tiers | type / activité client |
| cod_marche / cod_stn | STRING | dim_typologie | marché / segment |
| cod_famille / lib_famille | STRING | dim_produit | famille produit |
| idf_num_gui / idf_code_cc | STRING | dim_portefeuille | guichet / chargé clientèle |
| date_accord / annee / mois | DATE/INT | dim_temps | calendrier |

### Briques de mesure (toutes additives — base des KPI)
| Colonne | Type | Calcul | Description |
|---|---|---|---|
| mnt_credit | DECIMAL(18,3) | encours | montant initial |
| mnt_capital_rest_du | DECIMAL(18,3) | encours | **encours** |
| tx_credit | DECIMAL(18,3) | encours | taux |
| duree / nbr_echeance | BIGINT | encours | durée / échéances |
| mnt_impaye_total | DECIMAL(18,3) | SUM(impayes.mnt_impayes_cdl) | impayés cumulés |
| mnt_interet_retard_total | DECIMAL(18,3) | SUM(impayes.mnt_interet_retard) | intérêts de retard |
| mnt_penalite_total | DECIMAL(18,3) | SUM(impayes.mnt_penalite_retard) | pénalités |
| nb_creances_impayees | BIGINT | COUNT(impayes) | nb créances en impayé |
| flag_cdl | BOOLEAN | nb_creances_impayees > 0 | crédit en CDL |
| mnt_garantie_total | DECIMAL(18,3) | SUM(garantie.mnt_total_garantie) | garanties (total) |
| mnt_garantie_actualise | DECIMAL(18,3) | SUM(garantie.mnt_actualise) | garanties (actualisé) |
| quotite_garantie_moy | DECIMAL(18,3) | AVG(garantie.pourcentage_garantie) | quotité moyenne |
| nb_garanties | BIGINT | COUNT(garantie) | nb garanties |
| mnt_depassement_total | DECIMAL(18,3) | SUM(depassement.mnt_depassement) | dépassements cumulés |
| nb_depassements | BIGINT | COUNT(depassement) | nb dépassements |
| flag_depassement | BOOLEAN | nb_depassements > 0 | présence dépassement |
| mnt_autorise_total | DECIMAL(18,3) | SUM(autorisation.mnt_auto_tot_dev) | autorisé total |
| mnt_debloque_total | DECIMAL(18,3) | SUM(autorisation.mnt_auto_deb_dev) | débloqué |
| dt_load | DATE | — | date chargement |

### Exemple — nouveau KPI sans toucher au modèle
> « Taux de couverture des garanties » et « taux d'impayé » :
```sql
SELECT idf_num_doss, date_arrete,
       mnt_garantie_total   / NULLIF(mnt_capital_rest_du, 0) AS taux_couverture,
       mnt_impaye_total     / NULLIF(mnt_capital_rest_du, 0) AS taux_impaye
FROM gold_credit_garantie.gdw_synthese_credit
WHERE date_arrete = '2026-05-31';
```
Tout KPI futur (ratio d'endettement, taux de provisionnement, exposition nette de
garantie…) = une formule sur les colonnes ci-dessus, **zéro nouvelle jointure**.

---

## Récapitulatif

**5 faits** : `gdw_encours`, `gdw_impayes`, `gdw_depassement`, `gdw_garantie`, `gdw_autorisation`.
**1 table OBT** : `gdw_synthese_credit` (tous les champs calculables, grain dossier × arrêté).
**6 dimensions** : `dim_temps`, `dim_produit`, `dim_tiers`, `dim_typologie`, `dim_agent`,
`dim_portefeuille` + le hub `bridge_image_contrat`.

Liaison commune : tous les faits pointent vers `bridge_image_contrat` (clé `idf_ref_ext_ctr`),
qui relie aux tiers et produits → dimensions conformes partagées.

## Points ouverts
1. `gdw_encours` embarque les attributs contractuels (`duree`, `tx_credit`, `mnt_credit`)
   dénormalisés dans le fait. La version SCD2 historisée vit en Silver (`dim_credit`).
2. Sélection des colonnes curatée — à compléter selon les besoins BI (Tableau).
