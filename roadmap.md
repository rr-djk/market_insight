# Roadmap — Market Insight

## Orientation du projet

Le projet a évolué : l'objectif principal est désormais la **pratique du profiling et de l'optimisation C++** à grande échelle, en utilisant les 33M lignes de données comme terrain de travail réel.

Chaque phase introduit une opération plus lourde sur les données, et un niveau de profiling/optimisation correspondant. La progression va du calcul séquentiel simple jusqu'à la parallélisation GPU.

---

## ✅ Phase 1 — Données

- [x] Télécharger les symboles NASDAQ
- [x] Télécharger les données historiques complètes
- [x] Valider l'intégrité des CSV (colonnes, valeurs aberrantes, périodes couvertes)
- [x] **Résultat** : 11 727 symboles, 33 748 226 lignes de données

---

## ✅ Phase 2 — Base de données PostgreSQL

- [x] PostgreSQL via Docker (`docker-compose.yml`)
- [x] Schéma relationnel : `symbols` + `historical_prices` avec contraintes d'intégrité
- [x] Indexation : (symbol_id, trade_date), trade_date, symbol_id
- [x] Import Python CSV → PostgreSQL via COPY (~14 min, gestion des doublons)

---

## ✅ Phase 3 — Fondations C++

- [x] Build system : Makefile, g++-14, C++23, flags stricts
- [x] Dépendances : libpqxx 7.8.1 (sources), Google Test
- [x] Classe `Database` (RAII, requêtes paramétrées, protection injection SQL)
- [x] Structures de données : `Price`, `Transaction`, `Portfolio`, `UserInstruction`
- [x] Interface `Strategy` : `should_rebalance()`, `should_rebalance_on_new_symbol()`, `get_period_days()`
- [x] 37 tests unitaires (`make test`) : Portfolio (12), Database (9), Backtest (16)
- [x] 9 tests d'intégration sur données réelles (`make test-integration`)

---

## ✅ Phase 4 — MVP Backtest (terminé)

- [x] `EqualWeightStrategy` : équipondération + rééquilibrage périodique (stratégie Gave)
- [x] `BuyAndHoldStrategy` : aucun rééquilibrage périodique (baseline)
- [x] Moteur `Backtest::execute_backtest()` :
  - Itération par pas calendaires (`get_period_days()`)
  - Recherche binaire du dernier prix connu (`find_price_at_or_before`)
  - Symboles en attente (`pending`) : intégrés dès leur première cotation
  - Fenêtre de simulation déduite automatiquement des données
  - Métriques : rendement total, rendement annualisé, drawdown maximum
- [x] CLI interactive : saisie des paramètres, affichage comparatif Gave vs B&H
- [x] **Bug corrigé** : boucle infinie si `period_days = 0` → validation input + garde-fou moteur
- [x] **Bug corrigé** : UB dans `sell_all_positions` (itération sur map modifiée simultanément)

---

## 🔧 Phase 5 — Améliorations moteur (en cours)

Prérequis avant de passer aux opérations massives.

- [x] **Refactoring `execute_backtest()`** : decompose en fonctions modulaires (classify_symbols_by_availability, run_initial_distribution, run_simulation_loop, capture_final_snapshot) — chaque fonction <30 lignes
- [ ] **Délisting** : détecter quand un symbole a cessé de coter, vendre au dernier prix connu, exclure définitivement de la simulation, redistribuer le cash au prochain rééquilibrage
- [ ] **Export CSV** : écriture des valeurs journalières dans un fichier pour graphe externe

---

## 📊 Phase 6 — Indicateurs techniques (profiling de base)

**Opération** : calculer RSI, MACD, moyennes mobiles et Bollinger Bands sur les 33M lignes.
**Pourquoi** : première vraie charge CPU — boucles denses, arithmétique flottante, accès séquentiel à de gros volumes. Idéal pour apprendre à lire un profil gprof/perf.

- [ ] Implémenter les indicateurs en C++ (`TechnicalIndicators`) : SMA, EMA, RSI, MACD, Bollinger
- [ ] Calcul sur l'intégralité des symboles NASDAQ
- [ ] **Profiling gprof** : identifier les fonctions qui consomment le plus de temps CPU
- [ ] **Profiling perf** : identifier les cache misses, branch mispredictions
- [ ] Optimisation basée sur les résultats (loop unrolling, layout mémoire)
- [ ] Mesure avant/après : temps d'exécution et métriques CPU

---

## 🔍 Phase 7 — Screening / ranking (optimisation SQL + C++)

**Opération** : trouver les N meilleures actions selon des critères (Sharpe ratio, momentum, volatilité minimale) en scannant toute la base.
**Pourquoi** : mix SQL lourd + calcul C++. Profiling révèle la frontière entre goulot I/O (PostgreSQL) et goulot calcul.

- [ ] Implémenter un `Screener` : calcul de métriques par symbole (Sharpe, volatilité, momentum)
- [ ] Requêtes SQL batch optimisées (chargement par chunks vs tout en mémoire)
- [ ] Ranking et tri des 11 727 symboles
- [ ] **Profiling** : mesurer la part I/O vs calcul
- [ ] Optimisation : connection pooling, requêtes pipelinées, cache en mémoire
- [ ] Mesure avant/après

---

## 🏃 Phase 8 — Backtest massif sur tout le NASDAQ (charge réelle)

**Opération** : lancer la stratégie Gave sur les 11 727 symboles — ou générer des milliers de paniers aléatoires et les backtester tous.
**Pourquoi** : le moteur existant tourne sur 3 symboles en millisecondes. Sur 11 727, les goulots réels apparaissent (allocations, copies, accès DB répétés).

- [ ] Mode batch : lancer N backtests depuis un fichier de configuration
- [ ] Chargement bulk de toutes les données en mémoire (éviter N×requêtes PostgreSQL)
- [ ] **Profiling callgrind / valgrind** : visualisation dans KCachegrind
- [ ] Identifier et corriger les allocations inutiles, les copies de vecteurs
- [ ] Mesure avant/après : nb de backtests/seconde

---

## ⚙️ Phase 9 — Grid search de paramètres (parallélisation CPU)

**Opération** : tester toutes les combinaisons de paramètres (période de rééquilibrage, univers de symboles, dates) — dizaines de milliers de backtests.
**Pourquoi** : workload parfaitement parallélisable. Introduction de `std::thread` et thread pool.

- [ ] Générateur de combinaisons de paramètres (`GridSearch`)
- [ ] Thread pool maison (file de tâches, workers, collecte des résultats)
- [ ] Parallélisation des backtests : 1 tâche = 1 backtest
- [ ] **Profiling** : identifier les contentions (locks, faux partage de cache)
- [ ] Optimisation : réduire les sections critiques, aligner les structures sur les lignes de cache
- [ ] Benchmarking mono-thread vs multi-thread (speedup mesuré)

---

## 🔗 Phase 10 — Matrice de corrélations (SIMD + tiling)

**Opération** : calculer les corrélations entre tous les symboles sur une période donnée — 11 727 × 11 727 = ~68M paires.
**Pourquoi** : O(n²) pur, révèle les limites de la mémoire et de la bande passante. Introduit la vectorisation SIMD et le tiling de cache.

- [ ] Calcul naïf de la matrice de corrélation (baseline)
- [ ] **Profiling** : mesurer la bande passante mémoire, les cache misses L2/L3
- [ ] Optimisation tiling : découper la matrice en blocs qui tiennent dans le cache L2
- [ ] Vectorisation SIMD (AVX2 / AVX-512) : calcul de produits scalaires sur registres 256/512 bits
- [ ] Mesure avant/après : GFLOPS atteints vs peak théorique

---

## ⚡ Phase 11 — Simulation Monte Carlo (GPU CUDA)

**Opération** : générer des milliers de trajectoires aléatoires pour estimer la distribution des rendements futurs d'une stratégie.
**Pourquoi** : CPU-bound pur après chargement initial. Des milliers de simulations indépendantes → archétype du problème GPU.

- [ ] Implémentation CPU de Monte Carlo (baseline)
- [ ] Setup CUDA (instances AWS g4dn.xlarge ou Google Colab)
- [ ] Portage sur GPU : kernel de simulation, génération de nombres aléatoires (cuRAND)
- [ ] Optimisations mémoire GPU : coalescing des accès, shared memory
- [ ] **Profiling GPU** : Nsight Compute, mesure de l'occupancy, de la bande passante
- [ ] Benchmarking CPU vs GPU : speedup mesuré

---

## 🌐 Phase 12 — API REST C++

- [ ] Framework HTTP : Crow ou Pistache
- [ ] Endpoints : `POST /api/simulate`, `POST /api/screen`, `POST /api/grid-search`, `GET /api/symbols`
- [ ] File de tâches pour simulations asynchrones (résultats lourds)

---

## 🎨 Phase 13 — Frontend React

- [ ] Interface de configuration (panier, dates, fréquence, stratégie)
- [ ] Graphiques interactifs (évolution Gave vs B&H, drawdown, corrélations)
- [ ] Résultats de screening et grid search
- [ ] Export CSV / PDF

---

## ☁️ Phase 14 — Déploiement AWS

- [ ] EC2 GPU (p3/g4dn), RDS PostgreSQL, S3, CloudFront
- [ ] CI/CD avec GitHub Actions
- [ ] Monitoring (CloudWatch), mise à jour quotidienne des données (cron + UPSERT)

---

## Prochaine session

**Priorités immédiates (Phase 5) :**
1. Implémenter le **délisting** dans le moteur + test unitaire correspondant
2. ~~Refactorer `execute_backtest()`~~ : ✅ fait
3. (optionnel) Export CSV des valeurs journalières
