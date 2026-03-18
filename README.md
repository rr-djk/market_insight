# Market Insight

Terrain d'entraînement au **profiling et à l'optimisation C++**, à grande échelle, sur des données boursières historiques du NASDAQ (33,7M lignes, 11 727 symboles).

Le projet fournit un moteur de backtesting et un calculateur d'indicateurs techniques comme prétexte à des opérations CPU et I/O suffisamment lourdes pour que le profiling révèle de vrais goulots d'étranglement. Chaque phase de la roadmap introduit une charge plus importante et un niveau d'optimisation correspondant - de l'accès PostgreSQL jusqu'à la parallélisation GPU.

---

## ✨ Fonctionnalités

- **Backtest Gave (équipondération)** - Teste la théorie de Charles Gave : le portefeuille est redistribué à parts égales à chaque rééquilibrage. Les gagnants sont vendus, les perdants rachetés mécaniquement. Gère le délisting (vente au dernier prix connu, exclusion définitive).
- **Backtest Buy & Hold** - Stratégie de référence : positions initiales conservées sans rééquilibrage périodique.
- **Comparaison chiffrée** - Les deux backtests sont lancés sur les mêmes paramètres et comparés côte à côte (rendement total, rendement annualisé, drawdown maximum).
- **Indicateurs techniques sur 33M lignes** - Calcul batch de SMA, EMA, RSI-14, MACD et Bollinger Bands sur l'ensemble de la base, point d'entrée du profiling CPU.

---

## 🛠️ Stack technique

| Outil | Rôle |
|-------|------|
| C++23 / g++-14 | Langage du moteur |
| libpqxx 7.8.1 | Connexion PostgreSQL depuis C++ |
| PostgreSQL 15 | Stockage des données historiques |
| Docker / Compose | Orchestration de la base de données |
| Google Test | Tests unitaires et d'intégration |
| gprof / perf | Profiling CPU et analyse des goulots |

---

## 🎮 Fonctionnement

### La stratégie Gave

La théorie de Charles Gave repose sur l'**équipondération avec rééquilibrage périodique** : à chaque période (ex: tous les 30 ou 90 jours), le portefeuille est ramené à une répartition strictement égale entre tous les actifs.

L'effet mécanique : **vendre ce qui a surperformé, racheter ce qui a sous-performé**.

```
Exemple — 2 actions, capital 10 000 $, rééquilibrage mensuel :

Jour 0   : 50 AAPL (5 000 $) + 50 MSFT (5 000 $)
Jour 30  : AAPL vaut 7 500 $, MSFT vaut 4 000 $ → total 11 500 $
           Rééquilibrage : vente AAPL, achat MSFT → 5 750 $ chacun
Jour 60  : nouvelle évaluation et redistribution...
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
│   ├── include/          # Headers C++ (moteur, stratégies, données)
│   ├── src/              # Implémentations + CLI interactive
│   ├── tests/
│   │   ├── test_backtest.cpp
│   │   ├── test_database.cpp
│   │   ├── test_portfolio.cpp
│   │   ├── test_technical_indicators.cpp
│   │   └── integration/
│   │       └── test_backtest_integration.cpp
│   ├── profiling/
│   │   ├── profiling_report.md              # Synthèse des sessions de profiling
│   │   ├── profiling_gprof_baseline.txt     # Baseline gprof (Phase 6)
│   │   ├── profiling_gprof_opt2_bulk.txt    # gprof après optimisation bulk query
│   │   ├── profiling_perf_opt2.md           # Analyse perf (cache misses, branches)
│   │   ├── fails.md                         # Optimisations tentées et rejetées
│   │   └── map_vs_unordered_map/
│   │       ├── analyse.md
│   │       ├── perf_map.txt
│   │       ├── perf_unordered_map.txt
│   │       ├── time_map.txt
│   │       └── time_unordered_map.txt
│   └── Makefile
├── scripts/              # Téléchargement et import des données (Python)
├── data/
│   └── test/
│       └── test_symbols.txt   # Symboles réservés aux tests
├── db/
│   └── schema.sql
├── docker-compose.yml
└── README.md
```

---

## ⚠️ Note sur les données

Le jeu de données complet (33,7M lignes, plusieurs Go de CSV + base PostgreSQL) est actuellement stocké en local uniquement et n'est pas inclus dans ce dépôt. Le projet n'est donc pas entièrement reproductible en l'état - une solution pour rendre les données accessibles est à l'étude.

Les **tests unitaires** (`make test`) ne dépendent pas des données et restent pleinement exécutables. Les **tests d'intégration** (`make test-integration`) et le mode indicateurs techniques requièrent la base PostgreSQL locale.

---

## 🚀 Mise en route

### Prérequis

- Docker et Docker Compose
- g++-14 (C++23 requis — `<print>` non disponible sur GCC 13)
- libpqxx 7.8.1
- libpq-dev
- Google Test

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

```bash
./build/market_insight
```

```
=== Market Insight ===
1. Backtest (Gave vs Buy & Hold)
2. Indicateurs techniques (batch sur tous les symboles)
Choix : 1

Symboles (separes par des espaces, ex: AAPL MSFT GOOGL) : AAPL MSFT
Capital initial ($) : 10000
Date de debut (YYYY-MM-DD) : 2020-01-01
Date de fin (YYYY-MM-DD) : 2024-12-31
Periode de reequilibrage Gave (jours, ex: 30 90 365) : 90

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

### 4. Lancer les tests

```bash
# Tests unitaires (ne requièrent pas Docker)
make test

# Tests d'intégration sur données réelles (requièrent Docker)
make test-integration

# Valgrind sur les tests unitaires
make valgrind
```

---

## 🔭 Roadmap

Voir [roadmap.md](roadmap.md) pour le détail complet des phases :

- **Phase 5** ✅ — Délisting, refactoring moteur
- **Phase 6** ✅ — Indicateurs techniques, profiling gprof/perf, optimisations PostgreSQL
- **Phase 7** — Screening / ranking (optimisation SQL + C++)
- **Phase 8** — Backtest massif sur tout le NASDAQ
- **Phase 9** — Grid search (parallélisation CPU, thread pool)
- **Phase 10** — Matrice de corrélations (SIMD + tiling)
- **Phase 11** — Monte Carlo (GPU CUDA)
