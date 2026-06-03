const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, LevelFormat, HeadingLevel, BorderStyle,
  WidthType, ShadingType, VerticalAlign, PageNumber, PageBreak,
  TableOfContents, TabStopType, TabStopPosition
} = require("docx");

// ---------- Palette ----------
const NAVY = "1F4E79";
const BLUE = "2E75B6";
const HEADFILL = "D9E2F3";
const ZEBRA = "F2F6FB";
const GREY = "595959";

// ---------- Page (A4) ----------
const PAGE_W = 11906, MARGIN = 1134; // 0.79"
const CONTENT_W = PAGE_W - 2 * MARGIN; // ~9638

// ---------- Helpers ----------
const T = (text, opt = {}) => new TextRun({ text, font: "Arial", ...opt });
const P = (children, opt = {}) =>
  new Paragraph({ children: Array.isArray(children) ? children : [T(children)], spacing: { after: 120, line: 264 }, ...opt });
const H1 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_1, children: [T(text, { bold: true, color: NAVY, size: 30 })], spacing: { before: 280, after: 160 } });
const H2 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [T(text, { bold: true, color: BLUE, size: 25 })], spacing: { before: 220, after: 120 } });
const H3 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_3, children: [T(text, { bold: true, color: "333333", size: 22 })], spacing: { before: 160, after: 100 } });
const bullet = (children) => new Paragraph({ numbering: { reference: "bul", level: 0 }, children: Array.isArray(children) ? children : [T(children)], spacing: { after: 60, line: 264 } });
const note = (text) => new Paragraph({ children: [T(text, { italics: true, color: GREY, size: 20 })], spacing: { after: 140 }, border: { left: { style: BorderStyle.SINGLE, size: 18, color: BLUE, space: 10 } } });

const border = { style: BorderStyle.SINGLE, size: 1, color: "BFBFBF" };
const borders = { top: border, bottom: border, left: border, right: border };
function cell(content, { w, head = false, fill = null, bold = false, align = AlignmentType.LEFT } = {}) {
  const runs = Array.isArray(content) ? content : [T(String(content), { bold: head || bold, color: head ? "FFFFFF" : "000000", size: 19 })];
  return new TableCell({
    borders, width: { size: w, type: WidthType.DXA },
    shading: { fill: head ? NAVY : (fill || "FFFFFF"), type: ShadingType.CLEAR },
    margins: { top: 60, bottom: 60, left: 110, right: 110 },
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({ children: runs, alignment: align, spacing: { after: 0, line: 252 } })]
  });
}
function table(headers, rows, widths) {
  const headRow = new TableRow({ tableHeader: true, children: headers.map((h, i) => cell(h, { w: widths[i], head: true })) });
  const bodyRows = rows.map((r, ri) =>
    new TableRow({ children: r.map((c, i) => cell(c, { w: widths[i], fill: ri % 2 ? ZEBRA : "FFFFFF" })) }));
  return new Table({ width: { size: widths.reduce((a, b) => a + b, 0), type: WidthType.DXA }, columnWidths: widths, rows: [headRow, ...bodyRows] });
}
const spacer = (after = 120) => new Paragraph({ children: [T("")], spacing: { after } });

// ================= CONTENU =================
const body = [];

// ---- Page de titre ----
body.push(new Paragraph({ children: [T("")], spacing: { before: 1800 } }));
body.push(new Paragraph({ alignment: AlignmentType.CENTER, children: [T("RAPPORT D’HISTORISATION", { bold: true, size: 52, color: NAVY })], spacing: { after: 120 } }));
body.push(new Paragraph({ alignment: AlignmentType.CENTER, children: [T("Plateforme DATA standardisée — Axe Crédit-Garantie", { size: 30, color: BLUE })], spacing: { after: 80 } }));
body.push(new Paragraph({ alignment: AlignmentType.CENTER, children: [T("Architecture médaillon · Bronze – Silver – Gold", { size: 24, color: GREY })], spacing: { after: 600 } }));
body.push(new Paragraph({ alignment: AlignmentType.CENTER, children: [T("Techniques d’historisation par couche et par table, avec exemples", { italics: true, size: 22, color: "333333" })], spacing: { after: 1200 } }));
body.push(new Paragraph({ alignment: AlignmentType.CENTER, children: [T("BOA Entreprise — Projet PFE", { size: 22, color: GREY })], spacing: { after: 60 } }));
body.push(new Paragraph({ alignment: AlignmentType.CENTER, children: [T("Version 1.0 — Juin 2026", { size: 22, color: GREY })] }));
body.push(new Paragraph({ children: [new PageBreak()] }));

// ---- Sommaire ----
body.push(H1("Sommaire"));
body.push(new TableOfContents("Sommaire", { hyperlink: true, headingStyleRange: "1-2" }));
body.push(new Paragraph({ children: [new PageBreak()] }));

// ---- 1. Objet & périmètre ----
body.push(H1("1. Objet et périmètre"));
body.push(P("Ce rapport documente la stratégie d’historisation de la plateforme DATA Crédit-Garantie, de l’ingestion jusqu’à la couche décisionnelle. Pour chaque couche médaillon (Landing, Bronze, Silver, Gold) et pour chaque table, il précise la technique d’historisation retenue, son mécanisme et un exemple concret."));
body.push(P("L’objectif est de garantir deux exigences : (1) la traçabilité et la reconstitution à l’identique de tout arrêté (conformité Bâle III) ; (2) la conservation de l’historique des changements d’attributs — lorsqu’une information change, l’ancienne et la nouvelle valeur sont toutes deux conservées."));
body.push(H2("1.1 Périmètre"));
body.push(P([T("12 tables ", { bold: true }), T("de l’axe Crédit-Garantie sont industrialisées (Bronze + Silver + Qualité). Elles se répartissent en faits, dimensions et table pivot, et alimentent la cible Gold (modèle en étoile « Pilotage Crédit Consolidé »).")]));
body.push(table(
  ["Catégorie", "Tables"],
  [
    ["Faits", "CREDIT_AMORT, IMPAYES_CDL, GARANTIE_CONTRAT, GARANTIE_DETAIL, DEPASSEMENT, IDCE_AUTORISATION"],
    ["Dimensions", "TIERS, POPS, TYPOLOGIE, GRC_PORTEFEUILLE, ASAL"],
    ["Pivot", "IMAGE_CONTRAT (tiers ↔ produits)"],
  ],
  [2600, 7038]
));

// ---- 2. Principes directeurs ----
body.push(H1("2. Principes directeurs"));
body.push(bullet([T("Immutabilité : ", { bold: true }), T("aucune donnée n’est jamais supprimée. Tout est conservé et historisé ; les anomalies sont signalées (flag), pas effacées.")]));
body.push(bullet([T("Source en mode STOCK : ", { bold: true }), T("la source livre chaque jour un instantané (snapshot) complet de chaque table — confirmé par les EDD (« Type stockage = STOCK »). Ce n’est pas un flux de deltas.")]));
body.push(bullet([T("Chaîne médaillon : ", { bold: true }), T("Landing → Bronze (brut) → Silver (typé, dédupliqué, qualifié, historisé) → Gold (modèle en étoile).")]));
body.push(bullet([T("Deux temps (bi-temporel) : ", { bold: true }), T("la date métier d’arrêté (date_arrete) est distinguée de la date technique de traitement (ingestion_ts), présente sur chaque ligne.")]));
body.push(note("Principe fondateur du projet : « Toute information doit être conservée et historisée. Le pipeline ne supprime jamais de données. »"));

// ---- 3. Catalogue des techniques ----
body.push(H1("3. Catalogue des techniques d’historisation"));
body.push(P("Le tableau ci-dessous recense les techniques d’historisation évaluées, en précisant celles retenues et leur justification."));
body.push(table(
  ["Technique", "Définition", "Statut", "Où / Pourquoi"],
  [
    ["Snapshot (STOCK)", "Instantané complet à chaque arrêté, partitionné par date_arrete", "Retenu", "Bronze, Silver (faits), Gold (faits) — c’est le mode de livraison source"],
    ["Append-only", "Aucune partition/ligne écrasée entre arrêtés ; audit cumulatif", "Retenu", "Partitions date_arrete + tables d’audit Iceberg (LOG_PIPELINE, QUALITY_RESULTS)"],
    ["SCD2", "Versionnement d’attribut : valid_from / valid_to / is_current, toutes versions conservées", "Retenu", "Dimensions Silver — conserver l’historique des changements"],
    ["Bi-temporel", "Date métier (date_arrete) distincte de la date technique (ingestion_ts)", "Retenu", "Colonnes présentes ; formalisé valid_from/valid_to sur dimensions"],
    ["CDC par comparaison", "Détection de changement par diff entre deux snapshots successifs", "Retenu", "Alimente le MERGE SCD2 et la détection de variation Gold"],
    ["Time-travel", "Requête d’une table à un instant passé (AS OF) grâce à Iceberg", "Retenu", "Format Iceberg sur Silver et Gold"],
    ["Data Vault", "Modélisation hubs / links / satellites", "Écarté", "Source STOCK (pas de flux delta) → surcouche inutile pour le POC"],
  ],
  [1700, 3300, 1100, 3538]
));

// ---- 4. Historisation par couche ----
body.push(new Paragraph({ children: [new PageBreak()] }));
body.push(H1("4. Historisation par couche"));

body.push(H2("4.1 Landing — snapshot brut, append-only"));
body.push(P("NiFi dépose chaque livraison dans un répertoire dédié à l’arrêté. Chaque date_arrete est un dossier distinct, jamais écrasé. La donnée est brute (CSV → Parquet), fidèle à la source."));
body.push(P([T("Technique : ", { bold: true }), T("snapshot + append-only.")]));
body.push(P([T("Exemple : ", { bold: true }), T("hdfs://ubuntu3:9000/data/landing/tiers/date_arrete=20260531/", { font: "Consolas", size: 19 })]));

body.push(H2("4.2 Bronze — miroir typé, snapshot immuable"));
body.push(P("Bronze est le miroir fidèle de la source : toutes les lignes (doublons compris), partitionné par date_arrete. L’écriture est path-based : le mode overwrite ne remplace que la partition du jour — les arrêtés précédents restent intacts. Trois colonnes de traçabilité sont ajoutées à chaque ligne."));
body.push(P([T("Technique : ", { bold: true }), T("snapshot immuable par arrêté + traçabilité (bi-temporel).")]));
body.push(table(
  ["Colonne", "Rôle"],
  [
    ["execution_id", "Identifiant unique du run — lien vers LOG_PIPELINE"],
    ["source_file", "Fichier source HDFS d’origine"],
    ["ingestion_ts", "Horodatage technique du traitement (temps de connaissance)"],
    ["date_arrete", "Date métier de l’arrêté (partition, temps de validité)"],
  ],
  [2400, 7238]
));
body.push(note("Bronze conserve les doublons : c’est la mémoire brute. La déduplication n’intervient qu’en Silver — ainsi rien n’est jamais perdu au niveau le plus bas."));

body.push(H2("4.3 Silver — donnée propre, dédupliquée, historisée (Iceberg)"));
body.push(P("Silver est typé, dédupliqué par clé primaire, et qualifié (statut DQ par ligne). Stockage cible : tables Apache Iceberg (données Parquet sur HDFS, métadonnées au Hive Metastore), ce qui ouvre le time-travel et l’opération MERGE nécessaire au SCD2."));
body.push(H3("Faits → snapshot par date_arrete"));
body.push(P("Un fait est une photo à une date (un encours, un montant d’impayé). On ne versionne pas ses attributs : son évolution se lit d’un arrêté à l’autre."));
body.push(H3("Dimensions → SCD2 complet"));
body.push(P("Une dimension décrit une entité stable (client, produit). Lorsqu’un attribut change, on clôt l’ancienne version et on ouvre la nouvelle, via un MERGE INTO comparant le nouveau snapshot à l’état courant (is_current = true). Toutes les versions sont conservées."));
body.push(P([T("Exemple — le client ", {}), T("IDF_NUM_TIERS = 12345", { font: "Consolas", size: 19 }), T(" change de segment commercial COD_MARCHE : U (mai) → R (juin) → G (juillet).")]));
body.push(table(
  ["IDF_NUM_TIERS", "COD_MARCHE", "valid_from", "valid_to", "is_current"],
  [
    ["12345", "U", "2026-05-31", "2026-06-30", "false"],
    ["12345", "R", "2026-06-30", "2026-07-31", "false"],
    ["12345", "G", "2026-07-31", "(null)", "true"],
  ],
  [2300, 2000, 1900, 1900, 1538]
));
body.push(P([T("Résultat : ", { bold: true }), T("on peut connaître le segment du client à n’importe quelle date passée (au 15 juin → R), et obtenir l’état courant via is_current = true. Aucune version perdue.")]));

body.push(H2("4.4 Gold — modèle en étoile et détection de variation"));
body.push(P("Gold expose les KPIs « Pilotage Crédit Consolidé ». Les tables de faits restent en snapshot par date_arrete ; les dimensions sont jointes en mode point-in-time (via le SCD2 Silver) pour rattacher chaque fait à l’état des dimensions à sa date. Une logique de détection de variation compare deux arrêtés successifs (créé / modifié / disparu)."));
body.push(P([T("Exemple : ", { bold: true }), T("comparer l’arrêté d’avril et de mai pour identifier les crédits passés en impayé, ou les garanties tombées sur la période.")]));

// ---- 5. Étude par table avec exemples concrets ----
body.push(new Paragraph({ children: [new PageBreak()] }));
body.push(H1("5. Étude par table — exemples concrets"));
body.push(P("Chaque table est traitée selon le signal de changement disponible dans ses champs (EDD). Les faits sont conservés en snapshot par arrêté ; les dimensions et le pivot sont versionnés en SCD2. Un exemple chiffré illustre chaque cas."));

// helper exemple
const ex = (label) => new Paragraph({ children: [T("Exemple — ", { bold: true, color: BLUE }), ...(Array.isArray(label) ? label : [T(label)])], spacing: { after: 80 } });

// ===== 5.1 Famille 1 — signal MAJ natif =====
body.push(H2("5.1 Famille 1 — signal de mise à jour natif (DATE_MAJ / COD_MOD_MODIF)"));
body.push(P("Le changement est détecté directement via la date (ou le code) de modification portée par la ligne — sans comparaison coûteuse."));

body.push(H3("IMAGE_CONTRAT — pivot tiers ↔ produits (SCD2)"));
body.push(P([T("Clé : IDF_NUM_TIERS + IDF_REF_EXT_CTR + IDF_TYP_PRD + IDF_REF_PRD · Signal : ", {}), T("COD_MOD_MODIF / DATE_MODIF", { font: "Consolas", size: 19 })]));
body.push(ex("le cycle de vie d’un contrat COD_CYC_VIE passe de C (en cours) à T (terminé)."));
body.push(table(
  ["IDF_REF_EXT_CTR", "COD_CYC_VIE", "DATE_MODIF", "valid_from", "valid_to", "is_current"],
  [
    ["011...528020", "C", "2026-04-10", "2026-05-31", "2026-06-30", "false"],
    ["011...528020", "T", "2026-06-18", "2026-06-30", "(null)", "true"],
  ],
  [2300, 1500, 1700, 1500, 1300, 1338]
));

body.push(H3("GARANTIE_CONTRAT — fait garantie (signal MAJ)"));
body.push(P([T("Clé : IDF_REF_EXT_CREDIT + IDF_NUM_CTR_GARANTIE · Signal : ", {}), T("DATE_MAJ_OCCURENCE", { font: "Consolas", size: 19 })]));
body.push(ex("l’état du crédit garanti COD_ETAT_CREDIT passe de 1 (actif) à 3, détecté par l’évolution de DATE_MAJ_OCCURENCE."));
body.push(table(
  ["date_arrete", "IDF_NUM_CTR_GARANTIE", "COD_ETAT_CREDIT", "DATE_MAJ_OCCURENCE"],
  [
    ["2026-05-31", "G0099812", "1", "2026-05-12"],
    ["2026-06-30", "G0099812", "3", "2026-06-22"],
  ],
  [2200, 3000, 2200, 2238]
));

body.push(H3("GARANTIE_DETAIL — détail garantie (signal MAJ)"));
body.push(P([T("Clé : IDF_NUM_CTR_GARANTIE · Signal : ", {}), T("DATE_MAJ_OCCURENCE", { font: "Consolas", size: 19 })]));
body.push(ex("une garantie passe de CTT (contractée) à TER (terminée) et son montant actualisé évolue."));
body.push(table(
  ["date_arrete", "IDF_NUM_CTR_GARANTIE", "COD_STATUT_GARANTIE", "MNT_ACTUALISE"],
  [
    ["2026-05-31", "G0099812", "CTT", "250 000,00"],
    ["2026-06-30", "G0099812", "TER", "0,00"],
  ],
  [2200, 3000, 2400, 2038]
));

body.push(H3("IDCE_AUTORISATION — autorisation de crédit (signal MAJ)"));
body.push(P([T("Clé : IDF_REF_AUTO · Signal : ", {}), T("DATE_MAJ", { font: "Consolas", size: 19 })]));
body.push(ex("une autorisation passe de GES (en gestion) à TER ; le montant débloqué MNT_AUTO_DEB_MAD progresse."));
body.push(table(
  ["date_arrete", "IDF_REF_AUTO", "COD_ETAT_AUTO", "MNT_AUTO_DEB_MAD", "DATE_MAJ"],
  [
    ["2026-05-31", "0000036156", "GES", "60 000,00", "2026-05-09"],
    ["2026-06-30", "0000036156", "TER", "84 000,00", "2026-06-15"],
  ],
  [1900, 2100, 1900, 2100, 1638]
));

body.push(H3("GRC_PORTEFEUILLE — rattachement portefeuille (signal DATE_RATTACHEMENT)"));
body.push(P([T("Clé : IDF_NUM_GUI + IDF_NUM_TIERS + IDF_CODE_CC · Signal : ", {}), T("DATE_RATTACHEMENT", { font: "Consolas", size: 19 })]));
body.push(ex("le rattachement d’un client passe de P (principal) à S (secondaire), détecté via DATE_RATTACHEMENT."));
body.push(table(
  ["IDF_NUM_TIERS", "COD_TYP_RATTACH", "DATE_RATTACHEMENT", "valid_from", "is_current"],
  [
    ["5187654", "P", "2025-09-01", "2025-09-01", "false"],
    ["5187654", "S", "2026-06-30", "2026-06-30", "true"],
  ],
  [2100, 2200, 2200, 1700, 1438]
));

// ===== 5.2 Famille 2 — intervalles de validité dans la donnée =====
body.push(H2("5.2 Famille 2 — intervalles de validité fournis par la source"));
body.push(P("La source porte déjà des bornes de validité : le SCD2 est de fait présent dans la donnée et doit être préservé tel quel."));

body.push(H3("TIERS — référentiel clients (SCD2 natif sur adresses)"));
body.push(P([T("Clé : IDF_NUM_TIERS · Signal : ", {}), T("DATE_DEB_ADRESSE / DATE_FIN_ADRESSE", { font: "Consolas", size: 19 })]));
body.push(ex([T("le client IDF_NUM_TIERS = 5187654 déménage : son adresse change, l’ancienne est clôturée (DATE_FIN_ADRESSE renseignée), la nouvelle ouverte. En parallèle, son segment "), T("COD_MARCHE", { font: "Consolas", size: 19 }), T(" passe de U à R.")]));
body.push(table(
  ["IDF_NUM_TIERS", "COD_MARCHE", "DATE_DEB_ADRESSE", "DATE_FIN_ADRESSE", "is_current"],
  [
    ["5187654", "U", "2019-03-01", "2026-06-15", "false"],
    ["5187654", "R", "2026-06-15", "(null)", "true"],
  ],
  [2100, 1800, 2200, 2200, 1338]
));

body.push(H3("TYPOLOGIE — typologie client (SCD2)"));
body.push(P([T("Clé : IDF_NUM_TIERS · Signal : ", {}), T("DATE_EFFET / DATE_FIN", { font: "Consolas", size: 19 })]));
body.push(ex("le statut COD_STN d’un client passe de S2 à S3 (changement de segmentation risque)."));
body.push(table(
  ["IDF_NUM_TIERS", "COD_STN", "valid_from", "valid_to", "is_current"],
  [
    ["5187654", "S2", "2026-01-01", "2026-06-30", "false"],
    ["5187654", "S3", "2026-06-30", "(null)", "true"],
  ],
  [2100, 1600, 2100, 2100, 1738]
));

body.push(H3("DEPASSEMENT — dépassement de plafond (événement borné entrée/sortie)"));
body.push(P([T("Clé : NUM_COMPTE + DATE_ENTREE + COMPTE_GENERAL + MNT_DEPASSEMENT · Signal : ", {}), T("DATE_ENTREE / DATE_SORTIE", { font: "Consolas", size: 19 })]));
body.push(ex("un dépassement de 8 000 entre en vie le 31/05 (DATE_ENTREE) puis est régularisé le 12/06 (DATE_SORTIE renseignée) — l’intervalle borne l’événement."));
body.push(table(
  ["NUM_COMPTE", "MNT_DEPASSEMENT", "DATE_ENTREE", "DATE_SORTIE"],
  [
    ["011...528020", "8 000,00", "2026-05-31", "(null) — en cours"],
    ["011...528020", "8 000,00", "2026-05-31", "2026-06-12 — clôturé"],
  ],
  [2400, 2200, 2100, 2938]
));

// ===== 5.3 Famille 3 — faits, montants mouvants =====
body.push(H2("5.3 Famille 3 — faits à montants mouvants (snapshot par arrêté)"));
body.push(P("Aucun marqueur de changement ; les montants évoluent en continu. Ce sont des faits : on conserve un snapshot par date_arrete (chaque photo est figée et gardée), et la variation se lit d’un arrêté à l’autre."));

body.push(H3("CREDIT_AMORT — crédits amortissables"));
body.push(P([T("Clé : IDF_NUM_DOSS · Attribut mouvant : ", {}), T("MNT_CAPITAL_REST_DU", { font: "Consolas", size: 19 })]));
body.push(ex("le capital restant dû d’un crédit décroît à chaque échéance — chaque arrêté en garde la photo."));
body.push(table(
  ["date_arrete", "IDF_NUM_DOSS", "MNT_CAPITAL_REST_DU"],
  [
    ["2026-05-31", "10009552", "500 000,00"],
    ["2026-06-30", "10009552", "485 000,00"],
    ["2026-07-31", "10009552", "470 000,00"],
  ],
  [3000, 3300, 3338]
));

body.push(H3("IMPAYES_CDL — historique d’impayés"));
body.push(P([T("Clé : IDF_NUM_DOSSIER + IDF_NUM_CREANCE · Attributs mouvants : ", {}), T("MNT_IMPAYES_CDL, MNT_INTERET_RETARD", { font: "Consolas", size: 19 })]));
body.push(ex("l’impayé d’un dossier s’aggrave : le montant et les intérêts de retard s’accumulent d’un arrêté à l’autre."));
body.push(table(
  ["date_arrete", "IDF_NUM_DOSSIER", "MNT_IMPAYES_CDL", "MNT_INTERET_RETARD"],
  [
    ["2026-05-31", "10009552", "12 000,00", "350,00"],
    ["2026-06-30", "10009552", "18 500,00", "920,00"],
  ],
  [2200, 2700, 2300, 2238]
));

// ===== 5.4 Dimensions de référence sans signal natif =====
body.push(H2("5.4 Dimensions de référence — SCD2 par comparaison de snapshots"));
body.push(P("Ces référentiels ne portent ni date de MAJ ni intervalle de validité. Le changement est détecté en comparant le nouveau snapshot à l’état courant (CDC), puis versionné en SCD2."));

body.push(H3("POPS — référentiel produits"));
body.push(P([T("Clé : IDF_CODE_PRODUIT · Attribut : ", {}), T("COD_USAGE", { font: "Consolas", size: 19 })]));
body.push(ex("un produit voit son usage COD_USAGE passer de F à P (reclassement)."));
body.push(table(
  ["IDF_CODE_PRODUIT", "COD_USAGE", "valid_from", "valid_to", "is_current"],
  [
    ["B214CIM02", "F", "2025-01-01", "2026-06-30", "false"],
    ["B214CIM02", "P", "2026-06-30", "(null)", "true"],
  ],
  [2400, 1700, 1900, 1900, 1738]
));

body.push(H3("ASAL — référentiel agents commerciaux"));
body.push(P([T("Clé : IDF_PERSONNEL · Attribut : ", {}), T("NUM_GUICHET", { font: "Consolas", size: 19 })]));
body.push(ex("un agent est muté : son guichet de rattachement NUM_GUICHET change."));
body.push(table(
  ["IDF_PERSONNEL", "NUM_GUICHET", "valid_from", "valid_to", "is_current"],
  [
    ["A12345", "00123", "2024-02-01", "2026-06-30", "false"],
    ["A12345", "00450", "2026-06-30", "(null)", "true"],
  ],
  [2200, 1900, 1900, 1900, 1738]
));

// ---- 6. Tableau récapitulatif ----
body.push(new Paragraph({ children: [new PageBreak()] }));
body.push(H1("6. Tableau récapitulatif par table"));
body.push(table(
  ["Table", "Type", "Historisation", "Signal de changement"],
  [
    ["CREDIT_AMORT", "Fait", "Snapshot date_arrete", "Hash / DATE_DR_MODIFICATION (F3)"],
    ["IMPAYES_CDL", "Fait", "Snapshot date_arrete", "Aucun (hash — Famille 3)"],
    ["DEPASSEMENT", "Fait / événement", "Snapshot + intervalle", "DATE_ENTREE / DATE_SORTIE (F2)"],
    ["GARANTIE_CONTRAT", "Fait", "Snapshot + signal MAJ", "DATE_MAJ_OCCURENCE (F1)"],
    ["GARANTIE_DETAIL", "Fait", "Snapshot + signal MAJ", "DATE_MAJ_OCCURENCE (F1)"],
    ["IDCE_AUTORISATION", "Fait", "Snapshot + signal MAJ", "DATE_MAJ (F1)"],
    ["IMAGE_CONTRAT", "Pivot", "Snapshot + SCD2 (signal MAJ)", "DATE_MODIF (F1)"],
    ["TIERS", "Dimension", "SCD2", "Intervalles adresses (F2)"],
    ["TYPOLOGIE", "Dimension", "SCD2", "DATE_EFFET / DATE_FIN (F2)"],
    ["POPS", "Dimension", "SCD2", "Comparaison snapshot"],
    ["GRC_PORTEFEUILLE", "Dimension", "SCD2", "DATE_RATTACHEMENT (F1)"],
    ["ASAL", "Dimension", "SCD2", "Comparaison snapshot"],
  ],
  [2600, 1500, 2900, 2638]
));
body.push(spacer(60));
body.push(new Paragraph({ children: [T("Légende — signal de changement", { bold: true, color: NAVY, size: 21 })], spacing: { before: 80, after: 80 } }));
body.push(bullet([T("Date de MAJ (F1) : ", { bold: true }), T("la ligne porte une date de dernière modification (DATE_MODIF, DATE_MAJ, DATE_MAJ_OCCURENCE, DATE_RATTACHEMENT). Le changement est détecté quand cette date avance.")]));
body.push(bullet([T("Intervalle de validité (F2) : ", { bold: true }), T("la source fournit déjà les bornes début/fin (DATE_DEB/FIN, DATE_EFFET/FIN, DATE_ENTREE/SORTIE). Le mouvement est déjà décrit dans la donnée — on le préserve.")]));
body.push(bullet([T("Hash (F3, comparaison) : ", { bold: true }), T("aucune date de MAJ disponible. On calcule une empreinte des colonnes suivies — sha2(concat_ws('|', …)) — et on compare l’empreinte du jour à celle de l’arrêté précédent. Une empreinte différente = mouvement.")]));
body.push(note("Cas CREDIT_AMORT : la date DATE_DR_MODIFICATION capte les modifications administratives, mais pas l’amortissement automatique (le capital restant dû baisse sans « modification » au sens métier). Le hash sert donc de filet général pour capter le mouvement des montants ; la date n’est qu’un indice complémentaire."));

// ================= DOCUMENT =================
const doc = new Document({
  creator: "BOA Entreprise — PFE",
  title: "Rapport d'historisation — Crédit-Garantie",
  styles: {
    default: { document: { run: { font: "Arial", size: 21 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true, run: { size: 30, bold: true, font: "Arial", color: NAVY }, paragraph: { spacing: { before: 280, after: 160 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true, run: { size: 25, bold: true, font: "Arial", color: BLUE }, paragraph: { spacing: { before: 220, after: 120 }, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true, run: { size: 22, bold: true, font: "Arial", color: "333333" }, paragraph: { spacing: { before: 160, after: 100 }, outlineLevel: 2 } },
    ]
  },
  numbering: { config: [{ reference: "bul", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 280 } } } }] }] },
  sections: [{
    properties: { page: { size: { width: PAGE_W, height: 16838 }, margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN } } },
    headers: { default: new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, children: [T("Rapport d’historisation — Crédit-Garantie", { size: 16, color: GREY })], border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "BFBFBF", space: 4 } } })] }) },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [T("Page ", { size: 16, color: GREY }), new TextRun({ children: [PageNumber.CURRENT], size: 16, color: GREY, font: "Arial" }), T(" / ", { size: 16, color: GREY }), new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 16, color: GREY, font: "Arial" })] })] }) },
    children: body
  }]
});

Packer.toBuffer(doc).then(buf => { fs.writeFileSync("Rapport_Historisation_Credit_Garantie.docx", buf); console.log("OK — docx écrit"); });
