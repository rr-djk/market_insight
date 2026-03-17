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

## Conclusion

Ces deux tentatives ciblaient `std::fill` qui ne pesait que **2.54%** du CPU selon gprof.
Même si l'optimisation avait fonctionné, le gain maximal théorique aurait été négligeable.

**Leçon** : toujours optimiser ce que le profiler indique comme goulot réel.
Le vrai goulot est la désérialisation PostgreSQL (~35% CPU) — `parse_date` × 33M
et `shared_ptr` pqxx × 472M. C'est là qu'il faut concentrer les efforts.
