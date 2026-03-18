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

**Résultat mesuré**

| Métrique | Opt 2 (map) | Tentative 3 (unordered_map) | Δ |
|---|---|---|---|
| `compute_all()` | 2 643 ms | **4 215 ms** | **+1 572 ms (+59%)** |
| Elapsed | 75.85 s | 97.74 s | +21.89 s (+29%) |
| CPU % | 72% | 60% | −12% |

**Pourquoi ça a échoué**

Deux facteurs combinés :

1. **Les strings sont très courtes** : les noms de symboles font 2 à 5 caractères ("AAPL",
   "T", "MSFT"). Comparer deux strings de 4 caractères revient à comparer 4 octets — c'est
   quasi-instantané pour le CPU. Les 761M comparaisons coûtaient donc très peu individuellement.

2. **Cache misses** : `unordered_map` disperse ses entrées en mémoire selon le hash — les
   cases ne sont pas contiguës. À l'inverse, `map` est un arbre dont les nœuds, une fois
   construits, tendent à rester proches en mémoire et dans le cache CPU. Sur 33M accès
   répétés aux mêmes 11 727 entrées, `map` bénéficie du cache là où `unordered_map` génère
   des défauts de cache. C'est ce qu'indique le CPU% qui baisse de 72% à 60% : le processus
   passe plus de temps à attendre la RAM.

**Leçon**

La complexité algorithmique (O(log n) vs O(1)) ne reflète pas toujours les performances
réelles. Sur de petits ensembles avec des clés courtes et des accès répétés, le comportement
cache peut inverser le résultat attendu. Il faut mesurer, pas supposer.
