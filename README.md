# TP6 – Hive et Data Warehouse distribué (Groupe 6)

**Big Data, Hadoop et applications Python** – Exposé 6
Université Catholique d'Afrique de l'Ouest – Unité Universitaire au Togo
École Supérieure d'Ingénieurs – Cycle Ingénieur, Master 1 – Année scolaire 2025-2026

**Enseignant :** Dr. Sena Apeke

**Étudiants :**
- AMAH-TCHOUTCHOUI Ekué Donald
- FIOKLOU Donia
- MINGANE Gracia
- MOUN-RE Maxime

---

## Présentation

Ce projetconstruit un **Data Warehouse distribué** avec **Apache Hive** au-dessus de HDFS. Il analyse des transactions commerciales multisites réparties dans **huit villes du Togo**. Il fait suite aux exposés précédents sur HDFS (Exposé 2), MapReduce (Exposé 3) et Hadoop Streaming (Exposé 4).

Les objectifs sont les suivants :

- Déployer un cluster Hadoop/Hive avec Docker.
- Créer des tables externes Hive sur des fichiers CSV stockés dans HDFS, puis une table **partitionnée par (année, mois) au format ORC**.
- Écrire des requêtes HiveQL d'analyse : chiffre d'affaires, panier moyen, top produits, promotions, etc.
- Piloter Hive depuis Python avec **PyHive**, exporter les résultats en CSV et les visualiser avec pandas et matplotlib.
- **Comparer les résultats Hive à ceux de MapReduce** et mesurer les temps d'exécution.

## Architecture

Le cluster est défini dans `docker-compose.yml` et comprend 5 conteneurs :

| Service | Image | Rôle |
|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | NameNode HDFS (ports 9870, 9000) |
| `datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | DataNode HDFS |
| `hive-server` | `bde2020/hive:2.3.2-postgresql-metastore` | HiveServer2 (port 10000) |
| `hive-metastore` | `bde2020/hive:2.3.2-postgresql-metastore` | Metastore Hive (port 9083) |
| `hive-metastore-postgresql` | `bde2020/hive-metastore-postgresql:2.3.0` | Base PostgreSQL du metastore |

La configuration Hadoop et Hive est centralisée dans `hadoop-hive.env`. HiveServer2 y est configuré en **NOSASL**, ce qui évite d'installer le module `sasl`, impossible à compiler sous Windows.

## Contenu du dépôt

```
tp6-hive-groupe6/
├── docker-compose.yml               # Cluster Hadoop + Hive
├── hadoop-hive.env                  # Configuration Hadoop / Hive
├── analyse_transactions.hql         # Tables Hive + requêtes HiveQL
├── tp6_hive_python.py               # Script PyHive (requêtes, graphiques, comparaison)
├── transactions_small.csv           # Jeu de données small (10 000 lignes)
├── transactions_medium.csv          # Jeu de données medium (500 000 lignes)
├── resultats_hive/                  # Résultats exportés (un CSV par requête)
│   ├── Q1a_ca_mensuel.csv
│   ├── Q1b_ca_annuel.csv
│   ├── Q2_ca_panier_ville.csv
│   ├── Q3_top10_produits.csv
│   ├── Q4_ventes_categorie_paiement.csv
│   ├── Q5_impact_promotions.csv
│   ├── Q6_activite_jour_semaine.csv
│   └── Q7_*.csv                     # Les 5 requêtes équivalentes aux jobs MapReduce
├── ca_mensuel.png                   # Graphiques générés
├── ca_annuel.png
├── ca_par_ville.png
├── top10_produits.png
├── ca_categorie_paiement.png
├── impact_promotions.png
├── activite_jour_semaine.png
├── benchmark_hive.csv               # Temps d'exécution des requêtes
├── comparaison_hive_mapreduce.csv   # Validation Hive vs MapReduce
└── Expose6_Hive_TP_Python.pdf       # Rapport complet du TP
```

## Données

Chaque ligne du CSV correspond à une transaction. Les 19 colonnes sont :

`transaction_id, datetime, client_id, ville, magasin_id, categorie, produit, quantite, prix_unitaire, remise, mode_paiement, age_client, type_client, stock_avant, stock_apres, promotion, jour_semaine, livraison, montant`

- **Période :** 2024 – 2025 (24 mois, donc 24 partitions)
- **Villes :** Lomé, Kara, Sokodé, Kpalimé, Atakpamé, Tsévié, Dapaong, Aného
- **Catégories :** Alimentation, Boissons, Électronique, Hygiène, Papeterie
- **Montants :** en FCFA

## Prérequis

- Windows 10 (build 19041+) avec WSL2 et Docker Desktop
- Virtualisation activée dans le BIOS/UEFI
- 8 Go de RAM minimum (4 Go alloués à Docker recommandés)
- Python 3.9+ sur la machine hôte

```bash
pip install pyhive thrift future pandas matplotlib
```

>  **Ne pas installer** `pyhive[hive]` ni `sasl` : ils nécessitent un compilateur C++ sous Windows. Ils sont inutiles car HiveServer2 est en mode NOSASL.

## Exécution pas à pas

### 1. Démarrer le cluster

```bash
docker compose up -d
```

HiveServer2 peut mettre **1 à 3 minutes** à être réellement prêt après le démarrage. Le script Python réessaie la connexion automatiquement.

### 2. Charger les données dans HDFS

```bash
docker cp transactions_small.csv namenode:/transactions_small.csv
docker cp transactions_medium.csv namenode:/transactions_medium.csv

docker exec -it namenode hdfs dfs -mkdir -p /datalake/raw/transactions/small
docker exec -it namenode hdfs dfs -mkdir -p /datalake/raw/transactions/medium

docker exec -it namenode hdfs dfs -put /transactions_small.csv /datalake/raw/transactions/small/
docker exec -it namenode hdfs dfs -put /transactions_medium.csv /datalake/raw/transactions/medium/
```

### 3. Créer les tables Hive

```bash
docker cp analyse_transactions.hql hive-server:/analyse_transactions.hql
docker exec -it hive-server hive -f /analyse_transactions.hql
```

Ce script crée :
- `transactions` et `transactions_small` : tables **externes** sur HDFS (TEXTFILE)
- `transactions_part` : table **partitionnée** par `(annee, mois)` au format **ORC**, alimentée par insertion dynamique

### 4. Lancer l'analyse Python

```bash
python tp6_hive_python.py
```

Le script se connecte à HiveServer2 sur `localhost:10000`, exécute toutes les requêtes et exporte les CSV dans `resultats_hive/`. Il génère aussi les graphiques `.png`, le benchmark et la comparaison avec MapReduce.

## Requêtes d'analyse

| Requête | Description |
|---|---|
| **Q1a / Q1b** | Chiffre d'affaires mensuel et annuel |
| **Q2** | Chiffre d'affaires et panier moyen par ville |
| **Q3** | Top 10 des produits par chiffre d'affaires |
| **Q4** | Ventes par catégorie et mode de paiement |
| **Q5** | Impact des promotions (CA net/brut, coût des remises, panier moyen) |
| **Q6** | Activité par jour de la semaine |
| **Q7** | 5 requêtes équivalentes aux jobs MapReduce, pour la comparaison |

## Résultats principaux

Jeu **medium** : 500 000 transactions, chiffre d'affaires total d'environ **4,596 milliards FCFA**.

- **Évolution annuelle :** le CA est quasi identique entre 2024 (≈ 2,298 Md FCFA, 251 005 transactions) et 2025 (≈ 2,298 Md FCFA, 248 995 transactions).
- **Villes :** Lomé domine nettement, avec environ 40 % des transactions et du CA (≈ 1,84 Md FCFA). Le panier moyen est stable d'une ville à l'autre, autour de 9 200 FCFA.
- **Top produits :** les produits Électronique dominent. Le Powerbank arrive en tête avec ≈ 888 M FCFA, suivi de Souris, Clé USB et Écouteurs. Le Riz 5 kg est le premier produit alimentaire du classement.
- **Catégories :** l'Électronique représente la plus grande part du CA (≈ 2,47 Md FCFA).
- **Promotions :** 109 678 transactions en promotion, avec une remise moyenne de 13,17 %. Le coût total des remises est d'environ **136,9 M FCFA**. Le panier moyen est plus bas avec promotion (≈ 8 231 FCFA contre ≈ 9 462 FCFA sans promotion), pour une quantité moyenne quasi identique (≈ 3,2). Dans ce jeu de données, les promotions n'augmentent donc pas les quantités achetées.

Les graphiques correspondants sont fournis sous forme de fichiers `.png` à la racine du dépôt.

## Comparaison Hive / MapReduce

Les 5 jobs MapReduce de référence (Exposé 3) ont été comparés à leurs équivalents Hive. Le détail est dans `comparaison_hive_mapreduce.csv`.

| Job | Clés Hive | Clés référence | Écart max | Statut |
|---|---|---|---|---|
| transactions_par_ville | 8 | 8 | 0.0 | PASS |
| ca_par_ville | 8 | 8 | 0.0 |  PASS |
| quantite_par_produit | 21 | 21 | 0.0 |  PASS |
| ca_par_categorie | 5 | 5 | 0.0 |  PASS |
| transactions_par_paiement | 5 | 5 | 0.0 |  PASS |

>  **Anomalie à signaler :** Les références ont donc été **reconstruites localement** en Python, selon le schéma Map → Shuffle/Sort → Reduce, à partir de `transactions_medium.csv`. La validation reste pertinente, mais elle ne compare pas directement aux sorties des jobs Hadoop des autres groupes.

## Temps d'exécution

Les temps mesurés sur le jeu medium dans l'environnement Docker local sont dans `benchmark_hive.csv`. Ils vont de **≈ 6 s** (quantité par produit) à **≈ 24 s** (CA et panier moyen par ville).

## Dépannage courant

- **`TSocket read 0 bytes` ou connexion refusée :** HiveServer2 n'est pas encore prêt. Attendre 1 à 3 minutes, le script réessaie tout seul.
- **Vérifier l'état du cluster :** `docker ps` (tous les conteneurs doivent être `Up`) puis `docker logs hive-server --tail 40`.
- **Vérifier le mode NOSASL :**
```bash
  docker exec hive-server sh -c "grep -r -A1 server2.authentication /opt/hive/conf /etc/hive"
```
  Si ce n'est pas `NOSASL` : `docker compose up -d --force-recreate hive-server`.
- **Erreur sur `ORDER BY` d'une colonne absente du `SELECT` :** Hive 2.3 la refuse. La colonne `ordre` de Q6 est donc incluse dans le `SELECT` externe.

## Documentation

Le rapport complet du TP (installation de WSL2 et Docker, étapes détaillées, captures et résultats) est disponible dans `Expose6_Hive_TP_Python.pdf`.
