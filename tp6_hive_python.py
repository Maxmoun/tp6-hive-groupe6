"""
Exposé 6 - Hive et Data Warehouse distribué (Groupe 6)
TP en Python : connexion à Hive via PyHive, exécution des requêtes
d'analyse, visualisation des résultats avec pandas / matplotlib et
comparaison avec les résultats MapReduce.

Pré-requis (sur la machine hôte Windows, hors conteneur) :
    pip install pyhive thrift future pandas matplotlib
    (NE PAS installer pyhive[hive] ni sasl : le module sasl exige un compilateur C++ sous Windows.
     HiveServer2 est configuré en mode NOSASL dans hadoop-hive.env, donc sasl est inutile.)

Pré-requis côté cluster :
    - Le cluster Docker (namenode, datanode, hive-metastore, hive-server)
      doit être démarré (docker compose up -d)
    - Les fichiers transactions_small.csv et transactions_medium.csv doivent être chargés dans HDFS :
        docker cp transactions_small.csv namenode:/transactions_small.csv
        docker cp transactions_medium.csv namenode:/transactions_medium.csv
        docker exec -it namenode hdfs dfs -mkdir -p /datalake/raw/transactions/small
        docker exec -it namenode hdfs dfs -mkdir -p /datalake/raw/transactions/medium
        docker exec -it namenode hdfs dfs -put /transactions_small.csv /datalake/raw/transactions/small/
        docker exec -it namenode hdfs dfs -put /transactions_medium.csv /datalake/raw/transactions/medium/
    - Les tables Hive doivent être créées (voir analyse_transactions.hql), par exemple :
        docker cp analyse_transactions.hql hive-server:/analyse_transactions.hql
        docker exec -it hive-server hive -f /analyse_transactions.hql

Pour la comparaison avec MapReduce (question 7) :
    - placer transactions_medium.csv dans le dossier courant
    - (facultatif) placer dans le dossier reference_mapreduce/ les CSV de référence
      des Groupes 3/4 : ref_transactions_par_ville.csv, ref_ca_par_ville.csv,
      ref_quantite_par_produit.csv, ref_ca_par_categorie.csv,
      ref_transactions_par_paiement.csv  (en-tête puis cle,valeur).
      S'ils sont absents, ils sont RECONSTRUITS localement (Map -> Shuffle/Sort -> Reduce)
      et cette reconstruction doit être signalée comme anomalie dans le README.
"""

import csv
import os
import time
from itertools import groupby
from operator import itemgetter

from pyhive import hive
from thrift.transport import TSocket, TTransport
import pandas as pd
import matplotlib.pyplot as plt

# ----------------------------------------------------------------
# 0. Paramètres
# ----------------------------------------------------------------
CSV_MEDIUM = "transactions_medium.csv"      # source de la reconstruction MapReduce locale
DOSSIER_RESULTATS = "resultats_hive"        # CSV exportés (un par requête)
DOSSIER_REFERENCE = "reference_mapreduce"   # CSV de référence (Groupes 3/4), facultatif
TOLERANCE = 0.01                            # tolérance absolue de comparaison (FCFA)

os.makedirs(DOSSIER_RESULTATS, exist_ok=True)
temps_requetes = []                         # (nom de la requête, durée en secondes)

# ----------------------------------------------------------------
# 1. Connexion au serveur HiveServer2 exposé par le conteneur Docker
# ----------------------------------------------------------------
def connecter(tentatives=30, pause=10):
    """Se connecte à HiveServer2. Après un démarrage ou un redémarrage du conteneur, HiveServer2
    met 1 à 3 minutes à écouter : le port 10000 est déjà ouvert côté Docker mais la connexion est
    coupée ('TSocket read 0 bytes'). On réessaie donc automatiquement."""
    derniere_erreur = None
    for i in range(1, tentatives + 1):
        try:
            # Transport NOSASL (hive.server2.authentication=NOSASL dans hadoop-hive.env) avec un
            # délai maximal de 15 s par tentative : sans lui, la connexion peut rester bloquée
            # indéfiniment si HiveServer2 ne répond pas.
            # HiveServer2 tourne dans Docker mais le port 10000 est mappé sur l'hôte (localhost).
            sock = TSocket.TSocket("localhost", 10000)
            sock.setTimeout(15000)
            c = hive.Connection(
                thrift_transport=TTransport.TBufferedTransport(sock),
                username="root",
                database="default",
            )
            sock.setTimeout(None)   # pas de délai pour les requêtes (elles peuvent durer des minutes)
            print("Connecté à HiveServer2.")
            return c
        except Exception as e:
            derniere_erreur = e
            print(f"[{i}/{tentatives}] HiveServer2 pas encore prêt ({type(e).__name__}) - nouvel essai dans {pause} s...")
            time.sleep(pause)
    raise SystemExit(
        "\nImpossible de se connecter à HiveServer2 (dernière erreur : " + repr(derniere_erreur) + ").\n"
        "Vérifier dans PowerShell :\n"
        "  1) docker ps                                  -> hive-server doit être 'Up'\n"
        "  2) docker logs hive-server --tail 40           -> chercher des erreurs / 'Starting HiveServer2'\n"
        "  3) docker exec hive-server sh -c \"grep -r -A1 server2.authentication /opt/hive/conf /etc/hive\"\n"
        "                                                -> doit afficher NOSASL (sinon : docker compose up -d --force-recreate hive-server)\n"
    )


conn = connecter()


def requete(sql: str, nom: str = None) -> pd.DataFrame:
    """Exécute une requête HiveQL et renvoie un DataFrame pandas.
    Si 'nom' est donné : mesure le temps et exporte le résultat en CSV."""
    t0 = time.time()
    curseur = conn.cursor()
    curseur.execute(sql)
    colonnes = [c[0].split(".")[-1] for c in curseur.description]   # enlève le préfixe "table."
    df = pd.DataFrame(curseur.fetchall(), columns=colonnes)
    curseur.close()
    if nom:
        duree = time.time() - t0
        temps_requetes.append((nom, round(duree, 1)))
        df.to_csv(os.path.join(DOSSIER_RESULTATS, nom + ".csv"), index=False)
    return df


# ----------------------------------------------------------------
# 2. Vérification des tables
# ----------------------------------------------------------------
print("Aperçu de la table transactions :")
print(requete("SELECT * FROM transactions LIMIT 5"))

nb = requete("SELECT COUNT(*) AS nb FROM transactions")
print(f"\nNombre total de transactions : {nb.loc[0, 'nb']}")

nb_small = requete("SELECT COUNT(*) AS nb FROM transactions_small")
print(f"Nombre de transactions (jeu small) : {nb_small.loc[0, 'nb']}")

ca_total = requete("SELECT ROUND(SUM(montant), 2) AS ca_total FROM transactions")
print(f"Chiffre d'affaires total : {ca_total.loc[0, 'ca_total']}")

parts = requete("SHOW PARTITIONS transactions_part")
print(f"Nombre de partitions de transactions_part : {len(parts)}")

# ----------------------------------------------------------------
# 3. Q1 - Chiffre d'affaires mensuel et annuel
# ----------------------------------------------------------------
ca_mois = requete("""
    SELECT annee, mois,
           COUNT(*) AS nb_transactions,
           ROUND(SUM(montant), 2) AS ca_mensuel
    FROM transactions_part
    GROUP BY annee, mois
    ORDER BY annee, mois
""", "Q1a_ca_mensuel")
print("\nChiffre d'affaires mensuel :")
print(ca_mois)

ca_annee = requete("""
    SELECT annee,
           COUNT(*) AS nb_transactions,
           ROUND(SUM(montant), 2) AS ca_annuel
    FROM transactions_part
    GROUP BY annee
    ORDER BY annee
""", "Q1b_ca_annuel")
print("\nChiffre d'affaires annuel :")
print(ca_annee)

etiquettes_mois = ca_mois["annee"].astype(str) + "-" + ca_mois["mois"].astype(str).str.zfill(2)
plt.figure(figsize=(10, 5))
plt.plot(etiquettes_mois, ca_mois["ca_mensuel"], marker="o")
plt.title("Évolution mensuelle du chiffre d'affaires")
plt.ylabel("Chiffre d'affaires (FCFA)")
plt.xticks(rotation=60, ha="right")
plt.tight_layout()
plt.savefig("ca_mensuel.png")
plt.close()

plt.figure(figsize=(6, 5))
plt.bar(ca_annee["annee"].astype(str), ca_annee["ca_annuel"])
plt.title("Chiffre d'affaires annuel")
plt.ylabel("Chiffre d'affaires (FCFA)")
plt.tight_layout()
plt.savefig("ca_annuel.png")
plt.close()

# ----------------------------------------------------------------
# 4. Q2 - Chiffre d'affaires et panier moyen par ville
# ----------------------------------------------------------------
ca_ville = requete("""
    SELECT ville,
           COUNT(*) AS nb_transactions,
           ROUND(SUM(montant), 2) AS ca_total,
           ROUND(AVG(montant), 2) AS panier_moyen
    FROM transactions_part
    GROUP BY ville
    ORDER BY ca_total DESC
""", "Q2_ca_panier_ville")
print("\nChiffre d'affaires et panier moyen par ville :")
print(ca_ville)

plt.figure(figsize=(8, 5))
plt.bar(ca_ville["ville"], ca_ville["ca_total"])
plt.title("Chiffre d'affaires par ville")
plt.ylabel("Chiffre d'affaires (FCFA)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("ca_par_ville.png")
plt.close()

# ----------------------------------------------------------------
# 5. Q3 - Top 10 des produits (par chiffre d'affaires)
# ----------------------------------------------------------------
top_produits = requete("""
    SELECT produit, categorie,
           SUM(quantite) AS quantite_vendue,
           ROUND(SUM(montant), 2) AS ca_total
    FROM transactions_part
    GROUP BY produit, categorie
    ORDER BY ca_total DESC
    LIMIT 10
""", "Q3_top10_produits")
print("\nTop 10 des produits (par chiffre d'affaires) :")
print(top_produits)

plt.figure(figsize=(8, 5))
plt.barh(top_produits["produit"][::-1], top_produits["ca_total"][::-1])
plt.title("Top 10 des produits par chiffre d'affaires")
plt.xlabel("Chiffre d'affaires (FCFA)")
plt.tight_layout()
plt.savefig("top10_produits.png")
plt.close()

# ----------------------------------------------------------------
# 6. Q4 - Ventes par catégorie et mode de paiement
# ----------------------------------------------------------------
cat_paiement = requete("""
    SELECT categorie, mode_paiement,
           COUNT(*) AS nb_transactions,
           SUM(quantite) AS quantite_vendue,
           ROUND(SUM(montant), 2) AS ca_total
    FROM transactions_part
    GROUP BY categorie, mode_paiement
    ORDER BY categorie, ca_total DESC
""", "Q4_ventes_categorie_paiement")
print("\nVentes par catégorie et mode de paiement :")
print(cat_paiement)

pivot = cat_paiement.pivot(index="categorie", columns="mode_paiement", values="ca_total")
pivot.plot(kind="bar", stacked=True, figsize=(9, 5))
plt.title("Chiffre d'affaires par catégorie et mode de paiement")
plt.ylabel("Chiffre d'affaires (FCFA)")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig("ca_categorie_paiement.png")
plt.close()

# ----------------------------------------------------------------
# 7. Q5 - Impact des promotions
# ----------------------------------------------------------------
promos = requete("""
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
    ORDER BY promotion
""", "Q5_impact_promotions")
print("\nImpact des promotions (0 = sans promotion, 1 = avec promotion) :")
print(promos)

fig, axes = plt.subplots(1, 2, figsize=(9, 4))
etiq = promos["promotion"].map({0: "Sans promo", 1: "Avec promo"})
axes[0].bar(etiq, promos["panier_moyen"])
axes[0].set_title("Panier moyen (FCFA)")
axes[1].bar(etiq, promos["quantite_moyenne"])
axes[1].set_title("Quantité moyenne par transaction")
plt.tight_layout()
plt.savefig("impact_promotions.png")
plt.close()

# ----------------------------------------------------------------
# 8. Q6 - Activité par jour de la semaine
# ----------------------------------------------------------------
jours = requete("""
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
    ORDER BY ordre
""", "Q6_activite_jour_semaine")
jours = jours.drop(columns=["ordre"])      # colonne technique de tri, inutile à l'affichage
print("\nActivité par jour de la semaine :")
print(jours)

plt.figure(figsize=(8, 5))
plt.bar(jours["jour_semaine"], jours["nb_transactions"])
plt.title("Nombre de transactions par jour de la semaine")
plt.ylabel("Nombre de transactions")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig("activite_jour_semaine.png")
plt.close()

# ----------------------------------------------------------------
# 9. Q7 - Comparaison Hive / MapReduce
# ----------------------------------------------------------------
# 9.1 Les 5 requêtes Hive équivalentes aux 5 jobs MapReduce du Groupe 3
JOBS = [
    # (nom, requête Hive, colonne clé, colonne valeur)
    ("ref_transactions_par_ville",
     "SELECT ville, COUNT(*) AS nb_transactions FROM transactions GROUP BY ville ORDER BY ville",
     "ville", "nb_transactions"),
    ("ref_ca_par_ville",
     "SELECT ville, ROUND(SUM(quantite * prix_unitaire * (1 - remise)), 2) AS ca "
     "FROM transactions GROUP BY ville ORDER BY ville",
     "ville", "ca"),
    ("ref_quantite_par_produit",
     "SELECT produit, SUM(quantite) AS quantite_vendue FROM transactions GROUP BY produit ORDER BY produit",
     "produit", "quantite_vendue"),
    ("ref_ca_par_categorie",
     "SELECT categorie, ROUND(SUM(quantite * prix_unitaire * (1 - remise)), 2) AS ca "
     "FROM transactions GROUP BY categorie ORDER BY categorie",
     "categorie", "ca"),
    ("ref_transactions_par_paiement",
     "SELECT mode_paiement, COUNT(*) AS nb_transactions FROM transactions GROUP BY mode_paiement ORDER BY mode_paiement",
     "mode_paiement", "nb_transactions"),
]


# 9.2 Référence MapReduce locale : MAP -> SHUFFLE/SORT -> REDUCE
def mapper(ligne, col_cle, col_val):
    q = int(ligne["quantite"])
    p = float(ligne["prix_unitaire"])
    r = float(ligne["remise"])
    valeurs = {
        "nb_transactions": 1,
        "ca": q * p * (1 - r),
        "quantite_vendue": q,
    }
    return [(ligne[col_cle], valeurs[col_val])]


def reference_locale(col_cle, col_val):
    with open(CSV_MEDIUM, newline="", encoding="utf-8") as f:
        paires = [kv for ligne in csv.DictReader(f) for kv in mapper(ligne, col_cle, col_val)]   # MAP
    paires.sort(key=itemgetter(0))                                                                 # SHUFFLE / SORT
    return {k: sum(v for _, v in g) for k, g in groupby(paires, key=itemgetter(0))}                # REDUCE


def charger_reference(nom, col_cle, col_val):
    chemin = os.path.join(DOSSIER_REFERENCE, nom + ".csv")
    if os.path.exists(chemin):                       # CSV des Groupes 3/4
        ref = pd.read_csv(chemin)
        return dict(zip(ref.iloc[:, 0].astype(str), ref[col_val].astype(float))), "Groupes 3/4"
    return reference_locale(col_cle, col_val), "reconstruite localement"


lignes_comparaison = []
for nom, sql, col_cle, col_val in JOBS:
    hive_df = requete(sql, nom.replace("ref_", "Q7_"))
    hive_res = dict(zip(hive_df[col_cle].astype(str), hive_df[col_val].astype(float)))
    ref_res, origine = charger_reference(nom, col_cle, col_val)
    memes_cles = set(hive_res) == set(ref_res)
    ecart_max = max((abs(hive_res[k] - ref_res[k]) for k in set(hive_res) & set(ref_res)), default=float("nan"))
    statut = "PASS" if (memes_cles and ecart_max <= TOLERANCE) else "FAIL"
    lignes_comparaison.append({
        "job": nom.replace("ref_", ""),
        "nb_cles_hive": len(hive_res),
        "nb_cles_reference": len(ref_res),
        "ecart_max": round(ecart_max, 4),
        "origine_reference": origine,
        "statut": statut,
    })

comparaison = pd.DataFrame(lignes_comparaison)
comparaison.to_csv("comparaison_hive_mapreduce.csv", index=False)
print("\nComparaison Hive / MapReduce :")
print(comparaison)

# ----------------------------------------------------------------
# 10. Temps d'exécution des requêtes (benchmark Hive)
# ----------------------------------------------------------------
benchmark = pd.DataFrame(temps_requetes, columns=["requete", "duree_s"])
benchmark.to_csv("benchmark_hive.csv", index=False)
print("\nTemps d'exécution des requêtes Hive :")
print(benchmark)

conn.close()
print("\nTP terminé. Fichiers produits :")
print(" - graphiques : ca_mensuel.png, ca_annuel.png, ca_par_ville.png, top10_produits.png,")
print("                ca_categorie_paiement.png, impact_promotions.png, activite_jour_semaine.png")
print(f" - résultats CSV : dossier {DOSSIER_RESULTATS}/")
print(" - comparaison_hive_mapreduce.csv, benchmark_hive.csv")
