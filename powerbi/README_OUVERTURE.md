# Projet Power BI pré-câblé — Crédit-Garantie

Projet `.pbip` (format texte) avec **modèle + relations + mesures DAX déjà construits**.
Il ne reste qu'à **glisser les champs** pour faire les visuels (cf. `guide_powerbi_credit_garantie.md`).

## Contenu
- `CreditGarantie.pbip` — fichier à ouvrir
- `CreditGarantie.SemanticModel/` — 11 tables, 7 relations, 16 mesures, 3 colonnes calculées (aging, classe CDL)
- `CreditGarantie.Report/` — rapport vide, 5 pages prêtes (D1 Pilotage → D5 Garanties)

## Ouverture (PC Windows)
1. Copie **tout le dossier `powerbi/`** sur le PC Windows, ex. dans `C:\datalake\`.
2. Mets les 11 CSV dans **`C:\datalake\exports\`** (chemin câblé dans le modèle).
   - Chemin différent ? Relance `python scripts/gen_powerbi_project.py --csv-path "TON\CHEMIN"`,
     ou édite les fichiers `CreditGarantie.SemanticModel/definition/tables/*.tmdl` (ligne `File.Contents`).
3. Power BI Desktop → **Fichier → Ouvrir** → `CreditGarantie.pbip`.
   - Si `.pbip` n'apparaît pas : **Fichier → Options → Fonctionnalités en préversion** → coche
     « Enregistrement au format projet Power BI (.pbip) » (+ « TMDL »), redémarre Power BI.
4. À l'ouverture, le modèle se charge → clique **Actualiser** pour importer les données.
5. Onglet **Rapport** : sur chaque page D1-D5, glisse les mesures/champs selon le guide.

## Limites câblées (par choix, vérifiées sur la donnée)
- **Par famille produit** : utilise `gdw_synthese_credit[lib_famille]` (déjà dans l'OBT).
  `dim_produit` n'est pas reliée (`cod_sop` non unique : 36 doublons).
- **Dépassements** : `fait_depassement` est autonome (`num_compte_emprunteur` non unique côté crédit).
  La mesure `[Montant Dépassement]` fonctionne seule, sans filtrage croisé par produit/tiers.
- **Par agence** : chaîne `gdw_synthese_credit → dim_portefeuille → dim_organisation`.
  `dim_portefeuille` est dédoublonnée (1 agence/tiers) pour valider la relation.
- **Tendance via date_accord** : `dim_temps` couvre 2020→2030 ; les crédits accordés avant 2020
  ne matchent pas dim_temps (beaucoup de vieux crédits) → pour une courbe complète, utilise la
  hiérarchie de dates auto sur `gdw_synthese_credit[date_accord]`.
- Si Power BI signale une relation invalide à l'ouverture : supprime-la dans la vue **Modèle**
  (le reste du modèle reste fonctionnel).

## En cas d'échec d'ouverture
Le format `.pbip`/TMDL dépend de la version de Power BI. Si le projet refuse de s'ouvrir, bascule
sur le **montage manuel** : import des CSV + relations + DAX, tout est détaillé pas à pas dans
`guide_powerbi_credit_garantie.md`. Le DAX y est identique (copier-coller).
