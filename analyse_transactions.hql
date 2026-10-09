-- ============================================================
-- Exposé 6 - Hive et Data Warehouse distribué (Groupe 6)
-- Script HiveQL : création des tables externes + partitionnement + requêtes d'analyse
-- ============================================================

SET hive.cli.print.header=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 1. Création des tables externes pointant vers HDFS
--    transactions       : jeu medium (500 000 lignes) - développement et validation
--    transactions_small : jeu small  (10 000 lignes)  - tests rapides
CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
    transaction_id STRING,
    datetime       STRING,
    client_id      STRING,
    ville          STRING,
    magasin_id     STRING,
    categorie      STRING,
    produit        STRING,
    quantite       INT,
    prix_unitaire  DOUBLE,
    remise         DOUBLE,
    mode_paiement  STRING,
    age_client     INT,
    type_client    STRING,
    stock_avant    INT,
    stock_apres    INT,
    promotion      INT,
    jour_semaine   STRING,
    livraison      INT,
    montant        DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/raw/transactions/medium'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS transactions_small (
    transaction_id STRING,
    datetime       STRING,
    client_id      STRING,
    ville          STRING,
    magasin_id     STRING,
    categorie      STRING,
    produit        STRING,
    quantite       INT,
    prix_unitaire  DOUBLE,
    remise         DOUBLE,
    mode_paiement  STRING,
    age_client     INT,
    type_client    STRING,
    stock_avant    INT,
    stock_apres    INT,
    promotion      INT,
    jour_semaine   STRING,
    livraison      INT,
    montant        DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/raw/transactions/small'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. Vérification rapide
SELECT * FROM transactions LIMIT 10;
SELECT COUNT(*) AS nb_transactions FROM transactions;
SELECT COUNT(*) AS nb_transactions_small FROM transactions_small;
SELECT ROUND(SUM(montant), 2) AS ca_total FROM transactions;

-- 3. Organisation partitionnée par (annee, mois) au format ORC
--    24 partitions (2 ans x 12 mois) : le filtre WHERE annee=... AND mois=...
--    ne lit qu'une partition (partition pruning)
CREATE TABLE IF NOT EXISTS transactions_part (
    transaction_id STRING,
    datetime       STRING,
    client_id      STRING,
    ville          STRING,
    magasin_id     STRING,
    categorie      STRING,
    produit        STRING,
    quantite       INT,
    prix_unitaire  DOUBLE,
    remise         DOUBLE,
    mode_paiement  STRING,
    age_client     INT,
    type_client    STRING,
    stock_avant    INT,
    stock_apres    INT,
    promotion      INT,
    jour_semaine   STRING,
    livraison      INT,
    montant        DOUBLE
)
PARTITIONED BY (annee INT, mois INT)
STORED AS ORC;

INSERT OVERWRITE TABLE transactions_part PARTITION (annee, mois)
SELECT transaction_id, datetime, client_id, ville, magasin_id, categorie, produit,
       quantite, prix_unitaire, remise, mode_paiement, age_client, type_client,
       stock_avant, stock_apres, promotion, jour_semaine, livraison, montant,
       CAST(SUBSTR(datetime, 1, 4) AS INT) AS annee,
       CAST(SUBSTR(datetime, 6, 2) AS INT) AS mois
FROM transactions;

SHOW PARTITIONS transactions_part;

-- 4. Q1a - Chiffre d'affaires mensuel
SELECT annee, mois,
       COUNT(*) AS nb_transactions,
       ROUND(SUM(montant), 2) AS ca_mensuel
FROM transactions_part
GROUP BY annee, mois
ORDER BY annee, mois;

-- 5. Q1b - Chiffre d'affaires annuel
SELECT annee,
       COUNT(*) AS nb_transactions,
       ROUND(SUM(montant), 2) AS ca_annuel
FROM transactions_part
GROUP BY annee
ORDER BY annee;

-- 6. Q2 - Chiffre d'affaires et panier moyen par ville
--    (une ligne = une transaction, donc panier moyen = AVG(montant))
SELECT ville,
       COUNT(*) AS nb_transactions,
       ROUND(SUM(montant), 2) AS ca_total,
       ROUND(AVG(montant), 2) AS panier_moyen
FROM transactions_part
GROUP BY ville
ORDER BY ca_total DESC;

-- 7. Q3 - Top 10 des produits (par chiffre d'affaires)
SELECT produit, categorie,
       SUM(quantite) AS quantite_vendue,
       ROUND(SUM(montant), 2) AS ca_total
FROM transactions_part
GROUP BY produit, categorie
ORDER BY ca_total DESC
LIMIT 10;

-- 8. Q4 - Ventes par catégorie et par mode de paiement
SELECT categorie, mode_paiement,
       COUNT(*) AS nb_transactions,
       SUM(quantite) AS quantite_vendue,
       ROUND(SUM(montant), 2) AS ca_total
FROM transactions_part
GROUP BY categorie, mode_paiement
ORDER BY categorie, ca_total DESC;

-- 9. Q5 - Impact des promotions (promotion = 1) par rapport à l'absence de promotion (0)
--    cout_remises = CA brut - CA net
SELECT promotion,
       COUNT(*) AS nb_transactions,
       ROUND(SUM(montant), 2) AS ca_net,
       ROUND(SUM(quantite * prix_unitaire), 2) AS ca_brut,
       ROUND(SUM(quantite * prix_unitaire) - SUM(montant), 2) AS cout_remises,
       ROUND(AVG(montant), 2) AS panier_moyen,
       ROUND(AVG(quantite), 3) AS quantite_moyenne,
       ROUND(AVG(remise), 4) AS remise_moyenne
FROM transactions_part
GROUP BY promotion
ORDER BY promotion;

-- 10. Q6 - Activité par jour de la semaine (ordre calendaire)
--     La colonne 'ordre' doit figurer dans le SELECT externe : Hive 2.3 refuse un ORDER BY
--     sur une colonne absente du SELECT de la requête qui trie.
SELECT jour_semaine, nb_transactions, ca_total, panier_moyen, ordre
FROM (
    SELECT jour_semaine,
           COUNT(*) AS nb_transactions,
           ROUND(SUM(montant), 2) AS ca_total,
           ROUND(AVG(montant), 2) AS panier_moyen,
           CASE jour_semaine
                WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
                ELSE 7 END AS ordre
    FROM transactions_part
    GROUP BY jour_semaine
) t
ORDER BY ordre;

-- 11. Q7 - Équivalents des 5 jobs MapReduce (Exposé 3 / Exposé 4)
--     a) nombre de transactions par ville
SELECT ville, COUNT(*) AS nb_transactions
FROM transactions
GROUP BY ville
ORDER BY ville;

--     b) chiffre d'affaires par ville : somme de q x p x (1 - r)
SELECT ville, ROUND(SUM(quantite * prix_unitaire * (1 - remise)), 2) AS ca
FROM transactions
GROUP BY ville
ORDER BY ville;

--     c) quantité vendue par produit
SELECT produit, SUM(quantite) AS quantite_vendue
FROM transactions
GROUP BY produit
ORDER BY produit;

--     d) chiffre d'affaires par catégorie
SELECT categorie, ROUND(SUM(quantite * prix_unitaire * (1 - remise)), 2) AS ca
FROM transactions
GROUP BY categorie
ORDER BY categorie;

--     e) nombre de transactions par mode de paiement
SELECT mode_paiement, COUNT(*) AS nb_transactions
FROM transactions
GROUP BY mode_paiement
ORDER BY mode_paiement;
