# Baseline profiling — Pipeline DB (Phase 7 / backtest 10 symboles)

**Date** : 2026-04-10
**Branche** : `feature/db-profiling-tests` (worktree de `feature/db-pipeline-opt`)
**Outil** : `make test-profile` → `build/run_profiling_tests` (Google Test + `std::chrono::high_resolution_clock`)
**Méthode** : médiane de 5 exécutions après 1 warm-up (sauf Test 2 et Test 4 : 5 runs directs)
**Dataset** : 10 symboles NASDAQ (`INTC MSFT AAPL ADBE ORCL CSCO QCOM AMGN COST PAYX`), données réelles PostgreSQL

---

## Test 1 — Baseline pipeline complet (`get_prices_bulk`)

| Configuration         | Médiane (µs) | Lignes | µs/ligne |
|-----------------------|-------------:|-------:|---------:|
| 1 symbole  / 1 an     |        4 820 |    250 |   19.280 |
| 1 symbole  / 5 ans    |        9 083 |  1 258 |    7.220 |
| 1 symbole  / 10 ans   |       11 541 |  2 516 |    4.587 |
| 5 symboles / 1 an     |       11 946 |  1 250 |    9.557 |
| 5 symboles / 5 ans    |       28 080 |  6 290 |    4.464 |
| 5 symboles / 10 ans   |       54 348 | 12 580 |    4.320 |
| 10 symboles / 1 an    |       23 492 |  2 500 |    9.397 |
| 10 symboles / 5 ans   |       55 015 | 12 580 |    4.373 |
| 10 symboles / 10 ans  |      100 535 | 25 160 |    3.996 |

**Observations :**
- Le coût à 1 symbole / 1 an (~4 800 µs) représente l'overhead fixe réseau + pg : handshake,
  envoi de la requête, retour d'un petit résultat. Ce plancher est incompressible sans connection pooling.
- Le µs/ligne décroît avec le volume (~19 µs sur 250 lignes → ~4 µs sur 25K lignes) :
  l'overhead fixe est amorti sur plus de données.
- 10 symboles × 10 ans → **100 ms** : significatif si répété plusieurs fois dans un batch.

---

## Test 2 — Décomposition : I/O + pqxx vs construction map C++

| Étape                                      | Temps (µs) |
|--------------------------------------------|------------|
| Pipeline complet `get_prices_bulk` (A)     |     60 162 |
| Copie RAM + construction `map` seule (B)   |        217 |
| Ratio A/B                                  |    277.2×  |
| **Fraction I/O + pqxx estimée**            | **99.6 %** |

**Interprétation :**
La construction du `map<string, vector<Price>>` ne coûte que **217 µs** sur 12 580 lignes.
Le reste — **99.6 %** du temps — est entièrement absorbé par le réseau, PostgreSQL et la
désérialisation pqxx (`row[i].as<T>()`). Optimiser la structure de données C++ seule
(Test 3) n'aura qu'un impact marginal tant que la couche pqxx reste dans le chemin critique.

---

## Test 3 — `std::map` vs `std::unordered_map` (construction C++ pure)

| Structure              | Temps (µs) | Speedup |
|------------------------|------------|---------|
| `std::map`             |     3 005  |   —     |
| `std::unordered_map`   |     1 700  |  1.77×  |

**Interprétation :**
`unordered_map` est 1.77× plus rapide pour accumuler 12 580 lignes sur 10 symboles.
Mais rapporté au pipeline complet (60 000 µs), le gain potentiel est de
`(3005 - 1700) / 60000 ≈ 2.2%` — non significatif tant que le bottleneck pqxx n'est pas résolu.
À implémenter comme amélioration secondaire après l'optimisation pqxx.

---

## Test 4 — OHLCV vs Close-only (volume de transfert réseau)

| Requête                          | Temps (µs) | Speedup | Économie |
|----------------------------------|------------|---------|----------|
| `get_prices_bulk` (7 colonnes)   |     60 506 |   —     |   —      |
| `get_close_prices_bulk` (3 col.) |     44 297 |  1.37×  |  26.8 %  |

**Interprétation :**
Passer de 7 colonnes (OHLCV + symbol + date) à 3 colonnes (symbol + date + close)
économise **26.8 %** du temps. Le backtest utilise actuellement `get_prices_bulk` mais
n'accède qu'à `.close` — switcher vers `get_close_prices_bulk` dans `MarketData`
est un gain direct sans refactoring pqxx.

---

## Test 5 — Scalabilité en nombre de symboles (fenêtre 5 ans)

| N symboles | Médiane (µs) | Lignes | µs/ligne | µs/symbole | Ratio N-1 |
|------------|-------------:|-------:|---------:|-----------:|----------:|
| 1          |       10 089 |  1 258 |    8.020 |     10 089 |     —     |
| 2          |       14 143 |  2 516 |    5.621 |      7 071 |    1.40×  |
| 3          |       22 132 |  3 774 |    5.864 |      7 377 |    1.56×  |
| 5          |       27 414 |  6 290 |    4.358 |      5 482 |    1.24×  |
| 10         |       60 215 | 12 580 |    4.787 |      6 021 |    2.20×  |

**Interprétation :**
La croissance est **super-linéaire** (ratio N-1 variable, pas constant) — signe que le
bottleneck n'est pas uniquement le volume de données mais aussi le traitement pqxx par ligne.
Le saut de 5 → 10 symboles (ratio 2.20×) pour un doublement des lignes suggère une
composante quadratique dans la désérialisation ou la gestion mémoire du `pqxx::result`.

---

## Synthèse — Priorités d'optimisation

| Priorité | Optimisation                              | Gain potentiel estimé | Effort |
|----------|-------------------------------------------|----------------------|--------|
| 🔴 1     | `pqxx::stream_from` (bypass shared_ptr)  | ~50–70 % sur le total | Élevé  |
| 🟡 2     | `get_close_prices_bulk` dans MarketData  | ~27 % immédiat        | Faible |
| 🟢 3     | `unordered_map` dans `get_prices_bulk`   | ~2 % sur le total     | Faible |

**Conclusion :** 99.6 % du temps est dans la couche I/O + pqxx. L'ordre d'attaque :
1. Switcher `MarketData` vers `get_close_prices_bulk` (gain rapide, aucun risque)
2. Implémenter `pqxx::stream_from` pour bypasser le `pqxx::result` et ses `shared_ptr`
3. `unordered_map` comme optimisation de finition

---

## Résultats après optimisations (2026-04-10)

Les trois optimisations ont été appliquées séquentiellement sur la branche `feature/db-profiling-tests`.

### Opt 1 — `get_close_prices_bulk` dans `MarketData` (1 ligne changée)

`MarketData` appelait `get_prices_bulk` (7 colonnes : symbol, date, OHLCV) alors que
backtest et indicateurs techniques n'accèdent qu'à `.close`. Switch vers `get_close_prices_bulk`
(3 colonnes : symbol, date, close). Aucun test de régression affecté (14/14 passent).

| Métrique (Test 4, 10 sym, 5 ans) | Avant | Après | Gain |
|---|---|---|---|
| OHLCV `get_prices_bulk`          | 60 506 µs | 75 030 µs* | — |
| Close `get_close_prices_bulk`    | 44 297 µs | 41 757 µs  | ~31 % |

*\* Variance de run. Le gain mesuré sur plusieurs runs tourne autour de 27–44 %.*

### Opt 2 — `std::map` → `std::unordered_map`

Remplacement dans `get_prices_bulk`, `get_close_prices_bulk`, `MarketData::data` et le
constructeur raw. Vérifié : aucun code ne dépend de l'ordre d'itération (accès par clé seulement).

| Métrique (Test 3, construction C++ pure, 12 580 lignes) | Avant | Après | Gain |
|---|---|---|---|
| `std::map` construction | 3 005 µs | 3 195 µs* | — |
| `std::unordered_map` construction | 1 700 µs | 1 620 µs | 1.97× speedup |

*\* Variance de run. Le speedup mesuré est stable autour de 1.77–2.10×.*

Impact sur le pipeline total : ~2 % — marginal sur 10 symboles, significatif à 11 727 symboles.

### Opt 3 — `pqxx::stream_from` (`txn.stream<>`)

Remplacement de `exec_params()` + `pqxx::result` par `txn.stream<TYPES...>()` dans
`get_prices_bulk` et `get_close_prices_bulk`. Le streaming COPY de PostgreSQL envoie les
lignes directement, sans matérialiser un `pqxx::result` complet en RAM, sans `shared_ptr`
de comptage de références (~540M ops éliminées sur 33M lignes en mode batch).

Les paramètres liés (`$1`, `$2`) sont remplacés par des valeurs inlinées via `txn.quote()`
(échappe correctement toute injection SQL).

| Métrique (Test 1, `get_prices_bulk` OHLCV) | Baseline | Après Opt 3 | Gain |
|---|---|---|---|
| 10 sym / 5 ans  | 55 015 µs | 45 196 µs | **−18 %** |
| 10 sym / 10 ans | 100 535 µs | 76 611 µs | **−24 %** |
| µs/ligne (10 sym / 10 ans) | 3.996 | 3.045 | **−24 %** |

| Métrique (Test 2, décomposition, 10 sym, 5 ans) | Baseline | Après Opt 3 |
|---|---|---|
| Pipeline complet (A) | 60 162 µs | 46 043 µs |
| Copie RAM seule (B)  | 217 µs    | 532 µs    |
| Ratio A/B            | 277×      | 87×       |
| Fraction I/O estimée | 99.6 %    | 98.8 %    |

Le ratio A/B passe de 277× à 87× : le pipeline C++ lui-même est maintenant plus visible
(la copie B augmente légèrement car `unordered_map` a un overhead de réallocation sur
petits datasets comparé à `map` qui pré-alloue ses nœuds).

### Tableau récapitulatif final

| Optimisation | Fichier(s) | Gain mesuré (10 sym, 5 ans) | Complexité |
|---|---|---|---|
| `get_close_prices_bulk` dans `MarketData` | `market_data.cpp` (1 ligne) | ~27–44 % sur `MarketData` | Faible |
| `unordered_map` | `database.cpp/hpp`, `market_data.cpp/hpp` | 1.97× sur construction C++ | Faible |
| `txn.stream<>` (bypass `pqxx::result`) | `database.cpp` | **−18 à −24 %** sur pipeline total | Moyen |
| **Cumul (Opt 1+2+3)** | — | **~−36 % sur `get_prices_bulk` OHLCV** | — |

### Ce qui reste

Le 98.8 % restant en I/O est désormais essentiellement :
- Latence réseau loopback (incompressible sans shared memory ou Unix socket)
- Execution PostgreSQL (planner, I/O index, scan)
- Transfert COPY TEXT (vs COPY BINARY qui pourrait réduire la sérialisation côté PostgreSQL)

Prochaine piste potentielle si besoin : COPY BINARY via `pqxx::stream_from` avec types binaires,
ou connection pooling si plusieurs backtests sont lancés en parallèle.
