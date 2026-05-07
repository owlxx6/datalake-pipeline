# Résumé des configs YAML générées

> **⚠ ALERTE : 1 table(s) sans PK détectée**
> - **NEVOLAN_TA** (domain: credit)

| table_name | domain | nb_champs | PK | write_mode | schedule | nb_TODO |
|---|---|---|---|---|---|---|
| ASAL | asal | 11 | IDF_PERSONNEL | merge | `0 6 * * *` | 3 |
| CREDIT_NON_AMORT | autorisations | 36 | IDF_REF_AUTO | merge | `0 6 * * *` | 3 |
| CREDIT_AMORT | credit | 85 | IDF_NUM_DOSS | merge | `0 6 * * *` | 3 |
| CREDIT_AMORT_TA | credit | 18 | IDF_NUMERO_DOSSIER, IDF_NUMERO_SOUS_LIGNE | merge | `0 6 1 * *` | 3 |
| CREDIT_RH | credit | 18 | NUM_COMPTE_CLIENT, DATE_ECHEANCE | merge | `0 6 1 * *` | 3 |
| NEVOLAN_CONTRAT | credit | 31 | IDF_REFERENCE_CONTRAT | merge | `0 6 1 * *` | 3 |
| NEVOLAN_TA | credit | 26 | ⚠ AUCUNE | merge | `0 6 1 * *` | 3 |
| DEPASSEMENT | incident | 5 | NUM_COMPTE, DATE_ENTREE, COMPTE_GENERAL, MNT_DEPASSEMENT | merge | `0 6 * * *` | 3 |
| GARANTIE_BIEN | garweb | 9 | IDF_NUM_CTR_GARANTIE, IDF_NUM_BIEN | merge | `0 6 * * *` | 3 |
| GARANTIE_CONTRAT | garweb | 8 | IDF_REF_EXT_CREDIT, IDF_NUM_CTR_GARANTIE | merge | `0 6 * * *` | 3 |
| GARANTIE_DETAIL | garweb | 38 | IDF_NUM_CTR_GARANTIE | merge | `0 6 * * *` | 3 |
| GARANTIE_INSCRIPTION | garweb | 10 | IDF_NUM_CTR_GARANTIE, IDF_NUM_INSCRIPTION | merge | `0 6 * * *` | 3 |
| GARANTIE_RENOUVELLEMENT | garweb | 5 | IDF_NUM_CTR_GARANTIE, IDF_NUM_INSCRIPTION, IDF_DATE_RENOUV | merge | `0 6 * * *` | 3 |
| GARANTIE_TIERS | garweb | 5 | IDF_NUM_CTR_GARANTIE, IDF_NUM_TIERS | merge | `0 6 * * *` | 3 |
| IDCE_AUTORISATION | idce | 35 | IDF_REF_AUTORISATION | merge | `0 6 * * *` | 3 |
| IDCE_DOSSIER | idce | 13 | IDF_NUM_DOSSIER | merge | `0 6 * * *` | 3 |
| IDCE_EVENT_AUTO | idce | 15 | IDF_REF_AUTORISATION, IDF_DATE_EVENEMENT | merge | `0 6 * * *` | 3 |
| IDCE_EVENT_DOSSIER | idce | 15 | IDF_DOSSIER, IDF_DATE_EVENEMENT | merge | `0 6 * * *` | 3 |
| IDCE_LIENS_AUTO | idce | 12 | IDF_REF_AUTO_SUJET, IDF_REF_AUTO_OBJET, IDF_COD_CYC_VIE, IDF_DATE_DEB_LIEN | merge | `0 6 * * *` | 3 |
| IDCE_TIERS_AUTO | idce | 12 | IDF_REF_AUTORISATION, IDF_NUM_TIERS | merge | `0 6 * * *` | 3 |
| IDCE_TIERS_DOSSIER | idce | 7 | IDF_NUM_DOSSIER, IDF_NUM_TIERS | merge | `0 6 * * *` | 3 |
| IMAGE_CONTRAT | image_contrat | 40 | IDF_NUM_TIERS, IDF_REF_EXT_CTR, IDF_TYP_PRD, IDF_REF_PRD | merge | `0 6 * * *` | 3 |
| IMPAYES_CDL | impayes_cdl | 36 | IDF_NUM_DOSSIER, IDF_NUM_CREANCE | merge | `0 6 * * *` | 3 |
| ORG | org | 40 | IDF_NUM_GUI, IDF_NUM_BUR | merge | `0 6 * * *` | 3 |
| POPS | pops | 20 | IDF_CODE_PRODUIT | merge | `0 6 * * *` | 3 |
| CONTACTS | tiers | 9 | IDF_NUM_TIERS, IDF_INDIC_LIGNE, IDF_NUM_TEL | merge | `0 6 * * *` | 3 |
| REPRESENTANT_LEG | tiers | 3 | IDF_NUM_REP_LEG, IDF_NUM_TIERS | merge | `0 6 * * *` | 3 |
| TIERS | tiers | 111 | IDF_NUM_TIERS | merge | `0 6 * * *` | 3 |
| TIERS_EI | tiers | 62 | IDF_NUM_TIERS | merge | `0 6 * * *` | 3 |
| TYPOLOGIE | typologie | 13 | IDF_NUM_TIERS | merge | `0 6 * * *` | 3 |

---
*Généré par scripts/generate_configs.py*
