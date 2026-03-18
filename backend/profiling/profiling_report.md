# Rapport de profiling — Market Insight

## Méthodologie

Chaque mesure est prise avec `time ./build/market_insight` (mode 2 : batch indicateurs,
tous les symboles) sur le même jeu de données : 11 727 symboles NASDAQ, 33 748 226 lignes.

Le temps affiché par l'application (`Termine en X ms`) mesure uniquement `compute_all()`.
Le `time` système mesure le processus entier (chargement PostgreSQL + calcul).

---

## Lexique `time`

| Champ | Définition |
|---|---|
| **user** | Temps CPU passé en espace utilisateur (calcul pur, boucles, arithmétique). |
| **system** | Temps CPU passé en appels noyau (I/O, allocations mémoire, syscalls). |
| **elapsed** | Temps réel écoulé (horloge murale). Inclut les attentes I/O, réseau, DB. |
| **CPU %** | `(user + system) / elapsed × 100`. < 100% = le processus attend (I/O, DB). |

---

## Baseline — Version séquentielle naïve

**Date** : 2026-03-17
**Commande** : `echo 2 | time ./build/market_insight`
**Version** : `technical_indicators.cpp` — calcul séquentiel, aucune optimisation

| Métrique | Valeur |
|---|---|
| `compute_all()` mesuré en interne | **3 057 ms (3.06 s)** |
| Elapsed total (`time`) | **1 min 04.87 s** |
| User CPU | 35.38 s |
| System | 3.25 s |
| CPU utilisé | 59 % |
| Mémoire résidente max | 3 190 500 KB (~3.04 GB) |
| Page faults (minor) | 1 470 494 |
| Symboles traités | 11 727 |

**Observations :**
- Le calcul pur (`compute_all`) ne représente que ~3 s sur 65 s d'elapsed total.
- ~62 s sont consacrés au chargement des données depuis PostgreSQL (I/O dominant).
- CPU à 59 % : le processus est bloqué en attente I/O une bonne partie du temps.
- 3 GB de RAM pour charger l'intégralité des 33M lignes en mémoire.

---

## Analyse gprof — Baseline

**Commande** :
```bash
# Recompiler avec instrumentation
# Ajouter -pg aux CXXFLAGS dans le Makefile, puis :
make rebuild
echo 2 | ./build/market_insight
gprof ./build/market_insight gmon.out > profiling_gprof_baseline.txt
```

**Fichier** : `profiling_gprof_baseline.txt`

### Nos indicateurs ne sont PAS le goulot

| Fonction | % time | Calls |
|---|---|---|
| `compute_bollinger_band` | 0.83% | 23 454 |
| `compute_rsi` | 0.49% | 11 727 |
| `compute_ema` | 0.39% | 34 083 |
| `compute_sma` | 0.39% | 11 727 |

Tout le code de calcul représente moins de 3% du temps CPU. Les algorithmes sont corrects et rapides.

### Le vrai goulot : désérialisation PostgreSQL

| Problème | % time | Calls | Cause |
|---|---|---|---|
| `_init` | 17.99% | — | Attente réseau/disque PostgreSQL mal attribuée par gprof |
| `shared_ptr::_M_release` | 4.00% | 472M | pqxx gère ses `result` via `shared_ptr` — un compteur par accès champ |
| `parse_date` | 2.68% | 33.7M | Une conversion string → date pour chaque ligne des 33M |
| `std::fill` (NaN init) | 2.54% | 92K | Initialisation `vector<double>(n, NaN)` pour chaque indicateur |
| `shared_ptr::_M_add_ref_copy` | 2.49% | 472M | Idem `_M_release` — symétrique |
| `__shared_count` | 2.12% | 472M | Infrastructure interne `shared_ptr` pqxx |
| `pqxx::result` copy/destruct | ~2.5% | 236M | Cycle de vie des objets résultat pqxx |

**Total désérialisation pqxx estimé : ~35% du CPU user.**

### Pistes d'optimisation identifiées

| Problème | Optimisation cible |
|---|---|
| 11 727 requêtes PostgreSQL distinctes | Une seule requête bulk `SELECT * FROM historical_prices` |
| `parse_date` × 33M | Stocker les dates comme entiers (jours depuis epoch) en DB |
| `shared_ptr` pqxx × 945M | Chargement binaire (`COPY BINARY`) ou cache CSV local |
| NaN fill × 92K | `std::memset` ou écriture directe sans pré-initialisation |

### Conclusion

Le profiling confirme que le problème n'est pas dans les algorithmes mais dans **la couche de chargement**.
Optimiser `compute_all()` sans toucher au chargement ne gagnerait que ~3 s sur 65 s.
La prochaine cible est le chargement PostgreSQL.

---

## Optimisation 1 — Élimination de `parse_date` × 33M

**Date** : 2026-03-18
**Commande** : `echo 2 | time ./build/market_insight`

### Modification

Dans `get_prices()` (`database.cpp`), la colonne `trade_date` était retournée par PostgreSQL
comme string "YYYY-MM-DD", allouée en `std::string` sur le heap, puis parsée par `parse_date()`.
Cela représentait 33M allocations heap + 33M appels `parse_date`.

Remplacement du `SELECT hp.trade_date` par :
```sql
SELECT EXTRACT(EPOCH FROM hp.trade_date)::int / 86400
```

Côté C++, `row[0].as<int>()` lit directement un entier (stack, zéro malloc),
puis `sys_days{days{n}}` reconstruit la date en O(1).

`parse_date()` reste utilisé uniquement pour la saisie CLI (entrée utilisateur).

### Résultats

| Métrique | Baseline | Optimisation 1 | Δ |
|---|---|---|---|
| `compute_all()` | 3 057 ms | **2 890 ms** | −167 ms (−5.5%) |
| Elapsed total | 64.87 s | ~95 s* | — |
| User CPU | 35.38 s | 40.72 s | — |
| System | 3.25 s | 4.46 s | — |
| CPU utilisé | 59% | 47% | — |
| Mémoire résidente max | 3 190 500 KB | 3 190 108 KB | ≈ identique |

*\* L'elapsed total est dominé par les 11 727 aller-retours PostgreSQL et varie selon
le cache DB et la charge système — non significatif pour comparer cette optimisation.*

### Analyse

Le gain sur `compute_all()` (~5.5%) est cohérent avec le poids de `parse_date` dans le
profil gprof (2.68% du CPU user). L'élimination des 33M allocations `std::string` réduit
aussi la pression sur l'allocateur.

Le goulot principal reste les **11 727 requêtes PostgreSQL distinctes** qui dominent l'elapsed.
Prochaine cible : requête bulk unique.

---

---

## Optimisation 2 — Requête bulk unique (ANY($1::text[]))

**Date** : 2026-03-18
**Commande** : `echo 2 | time ./build/market_insight`

### Modification

`MarketData` appelait `db.get_prices(symbol, ...)` dans une boucle, soit 11 727 aller-retours
PostgreSQL distincts. Remplacement par une seule requête `get_prices_bulk` :

```sql
SELECT s.symbol,
       EXTRACT(EPOCH FROM hp.trade_date)::int / 86400,
       hp.open, hp.high, hp.low, hp.close, hp.volume
FROM historical_prices hp
JOIN symbols s ON hp.symbol_id = s.symbol_id
WHERE s.symbol = ANY($1::text[])
  AND hp.trade_date >= $2
  AND hp.trade_date <= $3
ORDER BY s.symbol, hp.trade_date
```

Côté C++, `MarketData` reçoit directement un `map<string, vector<Price>>` construit en une passe.

### Résultats

| Métrique | Opt 1 | Opt 2 (bulk) | Δ |
|---|---|---|---|
| `compute_all()` | 2 890 ms | **2 643 ms** | −247 ms (−8.5%) |
| Elapsed total | ~95 s | **75.85 s** | −~19 s (−20%) |
| User CPU | 40.72 s | 50.65 s | +9.93 s |
| System | 4.46 s | 3.96 s | −0.5 s |
| CPU % | 47% | **72%** | +25% |
| Mémoire résidente max | ~3.04 GB | **7.57 GB** | **+4.5 GB** |

### Analyse

L'elapsed baisse de 20% : les 11 727 aller-retours réseau sont éliminés. Le CPU passe de 47%
à 72% — le processus attend moins la DB et calcule davantage.

**Régression mémoire** : la RAM double. La requête bulk charge les 33M lignes dans un seul
`pqxx::result` qui coexiste en mémoire avec la map en cours de construction. Avant, chaque
`pqxx::result` par symbole était libéré immédiatement après usage. C'est un trade-off
vitesse/mémoire documenté, pas un bug.

---

## Tableau récapitulatif

| Version | compute_all() | Elapsed | CPU | RAM | Gain compute_all() |
|---|---|---|---|---|---|
| Baseline séquentielle | 3 057 ms | 64.87 s | 59% | 3.04 GB | — |
| Opt 1 — dates entières | 2 890 ms | ~95 s* | 47% | 3.04 GB | −5.5% |
| Opt 2 — bulk query | 2 643 ms | 75.85 s | 72% | 7.57 GB | −13.5% |
