# Tentatives d'optimisation échouées

## Tentative 1 — `fill_n` limité à la chauffe

**Date** : 2026-03-17

**Idée**
Remplacer `vector<double>(n, NaN)` (fill de n éléments) par `vector<double>(n)` +
`fill_n(begin, period-1, NaN)` pour ne remplir que la période de chauffe.

**Ce qu'on a fait**
```cpp
// Avant
std::vector<double> result(n, NaN);

// Après
std::vector<double> result(n);
std::fill_n(result.begin(), period - 1, NaN);
```

**Résultat mesuré**
| Version | compute_all() |
|---|---|
| Baseline | 3 057 ms |
| fill_n chauffe | 5 617 ms |

**Pourquoi ça a échoué**
`vector<double>(n)` zero-initialise quand même tous les n éléments via le constructeur.
On fait donc n writes à zéro + (period-1) writes NaN = plus de travail qu'avant.
L'initialisation à NaN n'est pas évitée, juste remplacée par une initialisation à 0.

---

## Tentative 2 — `reserve` + `push_back`

**Date** : 2026-03-17

**Idée**
`reserve` alloue sans initialiser. Chaque élément écrit une seule fois via `push_back`.

**Ce qu'on a fait**
```cpp
std::vector<double> result;
result.reserve(n);
for (size_t i = 0; i < period - 1; ++i)
    result.push_back(NaN);
result.push_back(first_valid_value);
for (size_t i = period; i < n; ++i)
    result.push_back(computed_value);
```

**Résultat mesuré**
| Version | compute_all() |
|---|---|
| Baseline | 3 057 ms |
| push_back | 11 786 ms |

**Pourquoi ça a échoué**
`push_back` met à jour le compteur de taille à chaque appel et n'est pas vectorisable
par le compilateur. L'accès indexé (`result[i] = ...`) sur un vecteur de taille connue
permet au compilateur d'auto-vectoriser les boucles avec SIMD. `push_back` casse cette
optimisation.

---

## Conclusion (tentatives 1 et 2)

Ces deux tentatives ciblaient `std::fill` qui ne pesait que **2.54%** du CPU selon gprof.
Même si l'optimisation avait fonctionné, le gain maximal théorique aurait été négligeable.

**Leçon** : toujours optimiser ce que le profiler indique comme goulot réel.
Le vrai goulot est la désérialisation PostgreSQL (~35% CPU) — `parse_date` × 33M
et `shared_ptr` pqxx × 472M. C'est là qu'il faut concentrer les efforts.

---

## Tentative 3 — `unordered_map` à la place de `map`

**Date** : 2026-03-18

**Idée**

Le profiling gprof après l'Opt 2 (bulk query) montrait `string::compare` à **4.78% du CPU**
avec 761 millions d'appels. Source : `map<string, vector<Price>>` est implémenté comme un
arbre binaire trié. Chaque recherche d'un symbole traverse l'arbre avec ~14 comparaisons de
strings (log₂ de 11 727 symboles). Sur 33.7M lignes chargées, ça s'accumule.

Idée naturelle : remplacer par `unordered_map`, qui calcule un hash du symbole et accède
directement à la bonne case — O(1) au lieu de O(log n).

**Ce qu'on a fait**

Remplacement de `std::map` par `std::unordered_map` dans `MarketData`, `Database::get_prices_bulk`,
et les tests.

**Résultat mesuré — session initiale (2026-03-18)**

| Métrique | Opt 2 (map) | Tentative 3 (unordered_map) | Δ |
|---|---|---|---|
| `compute_all()` | 2 643 ms | **4 215 ms** | **+1 572 ms (+59%)** |
| Elapsed | 75.85 s | 97.74 s | +21.89 s (+29%) |
| CPU % | 72% | 60% | −12% |

**Résultat mesuré — re-expérimentation (2026-03-18, `time` + `perf stat`)**

Re-implémentation de la même tentative pour valider la cause (cache misses) avec
`perf stat -e cache-misses,cache-references`. Voir `map_vs_unordered_map/` pour
les fichiers bruts.

| Métrique | map | unordered_map | Δ |
|---|---|---|---|
| `compute_all()` | 4 215 ms | 4 248 ms | +33 ms (~0%) |
| Elapsed | 125.98 s | 97.34 s | −28.64 s (−23%) |
| User time | 81.94 s | 53.59 s | −28.35 s (−35%) |
| CPU % | 69% | 61% | −8% |
| cpu_core cache-miss rate | 46.83% | 47.12% | +0.29% |
| cpu_atom cache-references | 1 948 M | 1 365 M | −30% |

La régression +59% sur `compute_all()` n'est **pas reproduite** : les deux versions
sont quasi identiques sur cette phase. En revanche, l'elapsed total est plus court
avec `unordered_map` (97 s vs 125 s) — l'inverse de la session initiale.

**Analyse**

La re-expérimentation révèle deux choses :

1. **Les conditions de mesure dominent** : le workload est I/O-bound (PostgreSQL).
   La charge système, l'état du cache OS, et la fréquence CPU entre deux sessions
   font varier les résultats autant que le choix de la structure de données. La
   régression de la session initiale était probablement amplifiée par des conditions
   défavorables sur le run `unordered_map`.

2. **L'hypothèse cache misses n'est pas confirmée** : les taux de cache miss sont
   indistinguables (~47% pour les deux). Ce n'est pas la dispersion mémoire de
   `unordered_map` qui explique la lenteur observée initialement. La vraie différence
   vient de la **phase de construction** : insérer 33M lignes dans un `map` ordonné
   coûte plus cher (O(log n) par clé nouvelle) qu'en `unordered_map` (O(1)), ce que
   confirme la différence de user time sur l'elapsed total.

**Leçon révisée**

La complexité algorithmique ne reflète pas toujours les performances réelles — mais
la conclusion inverse est tout aussi vraie : un run unique dans des conditions non
contrôlées ne suffit pas à établir une causalité. La cause documentée initialement
(cache misses) n'était qu'une hypothèse plausible, pas une mesure directe. Il faut
plusieurs runs, des conditions stables, et mesurer la phase exacte concernée.
