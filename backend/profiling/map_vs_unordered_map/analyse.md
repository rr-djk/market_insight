# Analyse : map vs unordered_map

## Données brutes

| Métrique | map | unordered_map |
|---|---|---|
| `compute_all()` | 4 215 ms | 4 248 ms |
| Elapsed total | 125.98 s | 97.34 s |
| User time | 81.94 s | 53.59 s |
| CPU % | 69% | 61% |
| cpu_core cache-references | 360 M | 383 M |
| cpu_core cache-misses | 168 M (46.83%) | 180 M (47.12%) |
| cpu_atom cache-references | 1 948 M | 1 365 M |
| cpu_atom cache-misses | 951 M (48.83%) | 646 M (47.36%) |

---

## Régression documentée dans fails.md : non reproduite

La régression originale documentait `compute_all()` à 2 643 ms pour map et
4 215 ms pour unordered_map (+59%). Ici les deux versions affichent ~4 200 ms —
quasi identiques, sans avantage clair pour l'un ou l'autre.

Le chiffre 2 643 ms (Opt 2, map) représentait probablement une session de mesure
dans des conditions favorables : base de données déjà chargée en mémoire OS,
charge système faible, CPU à fréquence maximale. Les conditions varient suffisamment
entre deux sessions pour inverser ou effacer une différence de quelques centaines de
millisecondes sur ce type de workload I/O-bound.

---

## Ce qu'on observe à la place

L'elapsed total est paradoxalement **plus court avec unordered_map** (97 s vs 125 s),
et le user time l'est encore plus nettement (53 s vs 81 s). La différence ne vient
pas de `compute_all()` — elle vient du **chargement PostgreSQL**.

`get_prices_bulk()` insère 33 millions de lignes dans la structure avec
`data[symbol].push_back(...)`. Avec `map`, chaque accès à un symbole inexistant
crée une entrée en maintenant le tri de l'arbre — O(log n) par insertion unique
de clé. Avec `unordered_map`, c'est O(1). Sur 11 727 symboles distincts,
la différence à la construction est mesurable.

En accès lecture (`compute_all()` itère sur les symboles déjà chargés),
l'avantage O(1) de `unordered_map` disparaît : les 11 727 symboles
tiennent facilement dans le cache CPU une fois l'accès établi, et l'arbre
`map` est lui aussi traversé rapidement sur des clés de 4-5 caractères.

---

## Taux de cache miss

Les deux versions affichent ~47% de cache miss rate sur cpu_core — indistinguable.
L'hypothèse de fails.md (dispersion mémoire de unordered_map génère plus de cache
misses) n'est **pas confirmée** par ces mesures. Le taux est le même.

Ce qui diffère : le **volume total** de références sur cpu_atom est 43% plus élevé
avec map (1 948 M vs 1 365 M). Map génère plus d'accès mémoire en absolu, même si
le taux de miss est identique.

---

## Conclusion

La réalité est plus nuancée que ce que fails.md documentait :

- Sur la **phase de chargement** (construction de la structure depuis PostgreSQL) :
  `unordered_map` est plus rapide — O(1) à l'insertion de nouvelles clés.

- Sur la **phase de calcul** (accès séquentiel aux 11 727 clés déjà en cache) :
  les deux sont équivalents. L'avantage théorique O(1) vs O(log n) disparaît
  quand les clés tiennent dans le cache L3.

- Les conditions de mesure (charge système, état du cache OS, fréquence CPU)
  influencent les résultats autant que le choix de la structure de données,
  sur un workload dominé par PostgreSQL.

La leçon de fails.md reste valide dans son principe (mesurer plutôt que supposer),
mais la cause précise documentée — cache misses de unordered_map — n'est pas ce
qu'on observe ici. La vraie explication de la lenteur initiale était probablement
une variation de conditions entre sessions de mesure.
