# Market Insight

Plateforme de backtesting de stratégies d'investissement en C++, appliquée sur des données boursières historiques du NASDAQ (33,7M lignes, 11 727 symboles).

---

## ✨ Fonctionnalités

### 🎯 Fonctionnalités principales

- 📊 **Backtest Gave (équipondération)** — Teste la théorie de Charles Gave : le portefeuille est redistribué à parts égales à chaque période de rééquilibrage. Les gagnants sont vendus, les perdants rachetés mécaniquement.
- 📈 **Backtest Buy & Hold** — Stratégie de référence : positions initiales conservées sans rééquilibrage périodique (sauf intégration de nouveaux symboles).
- ⚖️ **Comparaison chiffrée** — Les deux backtests sont lancés sur les mêmes paramètres et leurs résultats comparés côte à côte (rendement, drawdown, surperformance en points).
- 🕐 **Dernier prix connu** — Si un symbole n'a pas de cotation à une date donnée (week-end, jour férié, trou de données), le moteur utilise automatiquement le dernier prix disponible.
- 📥 **Symboles en attente** — Un symbole sans cotation à la date de départ (ex: IPO en cours de période) est mis en attente et intégré automatiquement dès sa première cotation.
- 🛡️ **Validation des entrées** — Dates invalides, période de rééquilibrage nulle et autres erreurs utilisateur sont détectées avec un message explicite, sans crash.

### 🚀 Points techniques

- **Recherche binaire O(log n)** sur les vecteurs de prix triés (`find_price_at_or_before`, `find_exact_price`)
- **Itération calendaire** : le moteur avance par pas de `period_days` jours, indépendant des jours de bourse
- **Fenêtre de simulation automatique** : déduite des données disponibles, pas des dates saisies
- **Architecture modulaire** : fonctions statiques à responsabilité unique, séparation données / logique / interface
- **Requêtes SQL paramétrées** via libpqxx (protection injection SQL)
- **37 tests unitaires + 9 tests d'intégration** sur données réelles

---

## 🛠️ Stack technique

### Backend

| Outil | Rôle |
|-------|------|
| C++23 / g++-14 | Langage du moteur de backtesting |
| libpqxx 7.8.1 | Connexion PostgreSQL depuis C++ |
| PostgreSQL 15 | Stockage des données historiques |
| Docker / Compose | Orchestration de la base de données |
| Google Test | Tests unitaires et d'intégration |

### Pipeline de données

| Outil | Rôle |
|-------|------|
| Python 3.12 | Téléchargement et import des données |
| yfinance | Source des prix historiques NASDAQ |
| PySpark | Traitement distribué des données brutes |

---

## 🎮 Fonctionnement

### La stratégie Gave

La théorie de Charles Gave repose sur l'**équipondération avec rééquilibrage périodique** : à chaque période (ex: tous les 30 ou 90 jours), le portefeuille est ramené à une répartition strictement égale entre tous les actifs.

L'effet mécanique : **vendre ce qui a surperformé, racheter ce qui a sous-performé**. Sur le long terme, cet anti-momentum capture la mean-reversion du marché.

```
Exemple — 2 actions, capital 10 000 $, rééquilibrage mensuel :

Jour 0   : 50 AAPL (5 000 $) + 50 MSFT (5 000 $)
Jour 30  : AAPL vaut 7 500 $, MSFT vaut 4 000 $ → total 11 500 $
           Rééquilibrage : vente AAPL, achat MSFT → 5 750 $ chacun
Jour 60  : nouvelle évaluation et redistribution...
```

### Le moteur de backtest

```
MarketData (PostgreSQL)
       │
       ▼
  Backtest::execute_backtest(portfolio, strategy)
       │
       ├── Jour 0   : distribution initiale sur symboles disponibles
       │
       ├── Boucle   : avance par pas de period_days
       │    ├── Résolution des symboles en attente (pending → available)
       │    ├── Collecte des prix (dernier connu si trou)
       │    ├── Rééquilibrage si strategy.should_rebalance()
       │    └── Enregistrement de la valeur journalière
       │
       └── Snapshot final + calcul des métriques
```

### Métriques produites

| Métrique | Description |
|----------|-------------|
| Valeur finale | Valeur du portefeuille à la date de fin |
| Rendement total | `(final / initial - 1) × 100` |
| Rendement annualisé | Taux annuel composé équivalent |
| Drawdown maximum | Plus grande baisse depuis un pic |
| Nombre de rééquilibrages | Total des redistributions effectuées |

---

## 📁 Structure du projet

```
market_insight/
├── backend/
│   ├── include/
│   │   ├── backtest.hpp             # Moteur de backtesting
│   │   ├── backtest_result.hpp      # Structure de résultats
│   │   ├── buy_and_hold_strategy.hpp
│   │   ├── database.hpp             # Connexion PostgreSQL (RAII)
│   │   ├── date.hpp                 # Alias Date + parse/format
│   │   ├── equal_weight_strategy.hpp
│   │   ├── market_data.hpp          # Chargement des prix
│   │   ├── portfolio.hpp            # Gestion du portefeuille
│   │   ├── price.hpp                # Struct OHLCV
│   │   ├── strategy.hpp             # Interface abstraite Strategy
│   │   ├── transaction.hpp          # Struct Transaction + OrderType
│   │   └── user_instruction.hpp     # Paramètres utilisateur
│   ├── src/
│   │   ├── backtest.cpp
│   │   ├── database.cpp
│   │   ├── date.cpp
│   │   ├── main.cpp                 # CLI interactive
│   │   ├── market_data.cpp
│   │   └── portfolio.cpp
│   ├── tests/
│   │   ├── test_backtest.cpp        # 16 tests unitaires moteur
│   │   ├── test_database.cpp        # 9 tests intégration DB
│   │   ├── test_portfolio.cpp       # 12 tests unitaires portfolio
│   │   └── integration/
│   │       └── test_backtest_integration.cpp  # 9 scénarios données réelles
│   └── Makefile
├── scripts/
│   ├── download_full_data.py        # Téléchargement NASDAQ complet
│   ├── download_test_data.py        # Téléchargement jeu de test
│   ├── get_nasdaq_symbols.py        # Liste des symboles
│   ├── import_to_postgres.py        # Import CSV → PostgreSQL
│   └── validate_data.py             # Validation intégrité des données
├── data/
│   └── test/
│       └── test_symbols.txt         # Symboles réservés aux tests
├── db/
│   └── schema.sql                   # Schéma PostgreSQL
├── docker-compose.yml
└── README.md
```

---

## 🚀 Mise en route

### Prérequis

- Docker et Docker Compose
- g++-14 (C++23 requis — `<print>` non disponible sur GCC 13)
- libpqxx 7.8.1
- libpq-dev
- Google Test
- Python 3.12+ (pour le pipeline de données uniquement)

### 1. Démarrer la base de données

```bash
docker-compose up -d
```

### 2. Compiler le backend

```bash
cd backend
make
```

### 3. Lancer la CLI

Le programme est interactif : il demande les paramètres un à un via stdin.

```bash
./build/market_insight
```

```
Symboles (separes par des espaces, ex: AAPL MSFT GOOGL) : AAPL MSFT
Capital initial ($) : 10000
Date la plus lointaine est 1970-01-02
Date de debut (YYYY-MM-DD) : 2020-01-01
Date de fin (YYYY-MM-DD) : 2024-12-31
Periode de reequilibrage Gave (jours, ex: 30 90 365) : 90

Symboles : AAPL, MSFT
Periode  : 2020-01-01 -> 2024-12-31
Capital  : 10000.00 $
Reequilibrage Gave : tous les 90 jours

--- Gave - Equiponderation ---
  Valeur finale       : 30126.87 $
  Rendement total     : 201.27 %
  Rendement annualise : 24.68 %
  Drawdown maximum    : 25.20 %
  Reequilibrages      : 20

--- Buy & Hold ---
  Valeur finale       : 29659.22 $
  Rendement total     : 196.59 %
  Rendement annualise : 24.29 %
  Drawdown maximum    : 32.66 %
  Reequilibrages      : 0

Surperformance Gave vs B&H : +4.68 points
```

Il est aussi possible de passer les paramètres directement via un pipe :

```bash
echo "AAPL MSFT
10000
2020-01-01
2024-12-31
90" | ./build/market_insight
```

### 4. Lancer les tests

```bash
# Tests unitaires (ne requièrent pas Docker)
make test

# Tests d'intégration sur données réelles (requièrent Docker)
make test-integration

# Les deux
make test-all
```

---

## 🔭 Vision long-terme

Voir [ROADMAP.md](doc_apprentissage/ROADMAP.md) pour le détail complet des phases à venir :

- **Phase 5** : délisting, refactoring moteur, export CSV
- **Phase 6** : parallélisation CPU (std::thread)
- **Phase 7** : parallélisation GPU (CUDA)
- **Phase 8** : API REST C++
- **Phase 9** : frontend React avec graphiques interactifs
- **Phase 10** : déploiement AWS (EC2 GPU, RDS, S3)
