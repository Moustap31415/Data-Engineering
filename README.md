🚀 Projet Data Engineering — Lakehouse PySpark sur Databricks

📌 Contexte académique

Cours : Data Engineering 
Enseignant : Mbaye Babacar Gueye
Type : Capstone Project

🎯 Objectif

--> Construire une plateforme Data Engineering de type Lakehouse en PySpark (batch) sur Databricks, avec :

* ingestion de données volumineuses (≥ 8 Go),
* enrichissement multi-sources réel,
* architecture Bronze / Silver / Gold,
* contrôles de qualité des données,
* optimisations de performance mesurées,
* livrables exploitables (BI, analytics).

---

🧱 Architecture globale (Lakehouse)

```
                        ┌──────────────────────┐
                        │   Données RAW        │
                        │  (Parquet volumineux)│
                        └──────────┬───────────┘
                                   │
                         Ingestion + Idempotence
                                   │
                   ┌───────────────▼───────────────┐
                   │           BRONZE              │
                   │  Données brutes historisées   │
                   │  (Delta, run_date=YYYY-MM-DD) │
                   └───────────────┬───────────────┘
                                   │
                     Nettoyage & Standardisation
                                   │
                   ┌───────────────▼───────────────┐
                   │            SILVER             │
                   │  Données propres & typées     │
                   │     + rejets auditables       │
                   └───────────────┬───────────────┘
                                   │
                        Enrichissement multi-sources
                                   │
                   ┌───────────────▼───────────────┐
                   │             GOLD              │
                   │  Marts analytiques            │
                   │  KPIs journaliers & mensuels  │
                   │  Exports BI-ready             │
                   └───────────────────────────────┘
```

---

📂 Organisation du projet (Unity Catalog Volumes)

```
project/
│
├── data/
│   ├── bronze/
│   │   ├── main/           # Orders bruts
│   │   └── enrich/         # Customers bruts
│   ├── silver/
│   │   ├── main_clean/     # Orders nettoyés
│   │   ├── enrich_clean/   # Customers nettoyés
│   │   ├── joined/         # Orders enrichis
│   │   └── rejects/        # Lignes rejetées (audit)
│   ├── gold/
│   │   ├── marts/          # Table analytique principale
│   │   ├── aggregates/     # KPIs jour / mois
│   │   └── exports/        # Parquet BI-ready
│
├── reports/
│   ├── data_quality/       # Résultats + rapport Markdown
│   └── benchmarks/         # Mesures perf & tailles
│
└── notebooks/              # Notebooks PySpark Databricks
```

---

🗄️ Sources de données

1️⃣ Dataset principal — Orders

* Volume ≥ 8 Go (Parquet partitionné généré avec Python)
* Faits transactionnels : commandes clients

Champs clés :

* order_id, customer_id, order_date, order_time
* quantity, unit_price, total_amount, discount
* product_category, payment_method, order_status

2️⃣ Dataset d’enrichissement — Customers

* Source indépendante
* Une ligne par client

champs clés :

* customer_id, signup_date, age, gender
* city, country, customer_type
* preferred_payment_method, loyalty_points

---

🥉 Bronze — Ingestion & historisation

🎯 Objectifs

* Charger les données sans transformation métier
* Garantir l’idempotence

⚙️ Méthode

* Lecture Parquet RAW
* Ajout :

  * run_id
  * run_date
* Écriture Delta Lake avec :

  ```
  mode("overwrite") + run_date=YYYY-MM-DD
  ```

✅ Résultat

* Rejouer le pipeline ne duplique jamais les données
* Historique par date d’exécution

---

🥈 Silver — Nettoyage, standardisation & audit

🔧 Transformations principales

Orders

* Schéma explicite (cast)
* Normalisation des chaînes (lower, trim)
* Suppression :

  * IDs nuls
  * quantités ≤ 0
  * montants négatifs
  * dates invalides
* Règle métier : année 2025 uniquement
* order_time = 000000 / 00:00:00 → NULL

Customers

* Validation âge : 0 ≤ age ≤ 120
* Standardisation des catégories

### 🧪 Rejets (audit)

Les lignes invalides sont stockées dans :

```
data/silver/rejects/run_date=YYYY-MM-DD/
```

avec :

* données originales
* reject_reason

👉 Traçabilité complète (exigence projet respectée)

---

🔗 Enrichissement multi-sources (Silver Joined)

🔑 Jointure

* orders.customer_id = customers.customer_id
* Broadcast join automatique si customers < 5M lignes

### 🧠 Features générées

* customer_tenure_days
* is_tenure_negative
* preferred_payment_match
* gross_amount
* age_bucket
* customer_found

👉 Données incohérentes conservées mais mesurées (transparence)

---

🥇 Gold — Datasets analytiques

1️⃣ Mart principale

* Table analytique enrichie
* Partitionnée par order_date
* Format : Delta Lake

2️⃣ Agrégations

Journalière

* Nombre de commandes
* Chiffre d’affaires
* Panier moyen
* Clients actifs
* Anomalies tenure

#### Mensuelle

* KPIs par order_month (YYYY-MM)

3️⃣ Export BI-ready

* Format : Parquet optimisé
* Fichiers réduits (coalesce)

---

🧪 Qualité des données (obligatoire)

✔️ Contrôles implémentés (niveau 1)

| Check                  | Description           |
| ---------------------- | --------------------- |
| order_id_not_null_rate | Taux d’IDs non nuls   |
| order_id_unique        | Unicité des commandes |
| customer_id_not_null   | Clé client présente   |
| total_amount ≥ 0       | Montants valides      |
| quantity > 0           | Quantités valides     |
| unit_price ≥ 0         | Prix valides          |
| tenure_negative_rate   | Anomalies temporelles |
| customer_found_rate    | Qualité de jointure   |

📄 Livrables

* Table Delta dq_results
* Rapport Markdown auto-généré

---

⚡ Performance & optimisations

Optimisations appliquées

* Broadcast join conditionnel
* Partitionnement par date
* repartition / coalesce
* Ajustement spark.sql.shuffle.partitions
* Format Delta vs Parquet

Benchmarks

* Durées avant / après optimisation
* Nombre de fichiers
* Taille disque totale

Stockés dans :

```
reports/benchmarks/
```

---
👤 Auteurs

Ibrahima Dia
Aissatou M'bayang Diedhiou
Mouhamadou Moustapha Sow
---