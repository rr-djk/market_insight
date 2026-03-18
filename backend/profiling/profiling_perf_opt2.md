# Profiling perf — Opt 2 (bulk query + map)

**Date** : 2026-03-18
**Outil** : `perf stat` + `perf stat -e cache-references,cache-misses,L1-dcache-load-misses,LLC-load-misses`
**Commande** : `echo 2 | perf stat [-e ...] ./build/market_insight`
**Version** : bulk query (`ANY($1::text[])`), `std::map<string, vector<Price>>`

> ⚠️ `perf stat` ajoute ~30 s de overhead CPU (81 s user vs 50 s sans perf).
> Le `compute_all()` interne affiche ~4 280 ms au lieu de 2 643 ms.
> Les chiffres perf restent valides pour comparer les compteurs entre eux,
> mais ne reflètent pas les performances réelles du binaire.

---

## Prérequis

```bash
sudo sh -c 'echo -1 > /proc/sys/kernel/perf_event_paranoid'
```

---

## Résultats bruts

### perf stat (compteurs généraux)

```
   688,533,264,992      cpu_core/instructions/    #  3.29  insn per cycle
   209,358,679,707      cpu_core/cycles/          #  2.317 GHz
   114,936,893,431      cpu_core/branches/        #  1.272 G/sec
       162,306,524      cpu_core/branch-misses/   #  0.14% of all branches

 #     54.5 %  tma_retiring
 #     23.9 %  tma_frontend_bound
 #     19.2 %  tma_backend_bound
 #      2.4 %  tma_bad_speculation
```

### perf stat -e cache (compteurs cache)

```
       364,207,470      cpu_core/cache-references/
       168,601,146      cpu_core/cache-misses/         #  46.29% of cache refs
       143,825,066      cpu_core/L1-dcache-load-misses/
         2,122,411      cpu_core/LLC-load-misses/
```

---

## Analyse

### Branch mispredictions : négligeable

0.14% des branches sont mal prédites. Le CPU prédit correctement presque toutes les
conditions (`if`, boucles). Ce n'est pas un goulot.

### Cache misses : alarmant en surface, bénin en réalité

Le taux de 46% de cache misses peut faire peur, mais il faut regarder **où les données
sont retrouvées** :

- **L1** (quelques Ko) : rate souvent — normal, 7.5 GB de données ne peuvent pas y tenir
- **L2/L3** : la grande majorité des misses L1 sont rattrapées ici
- **LLC (L3) misses : seulement 2.1 millions** — c'est peu

Concrètement : quand le CPU cherche une donnée et ne la trouve pas en L1, il la trouve
presque toujours en L2 ou L3 avant d'aller en RAM. Le dataset tient dans le L3. La
bande passante mémoire (RAM) n'est **pas** le goulot.

### TMA — où passe le temps CPU

| Catégorie | % | Signification |
|---|---|---|
| **Retiring** | 54.5% | Travail utile effectué — correct |
| **Frontend bound** | 23.9% | Le CPU n'arrive pas à charger les instructions assez vite |
| **Backend bound** | 19.2% | Le CPU attend des données (latence mémoire / heap) |
| **Bad speculation** | 2.4% | Négligeable |

**Frontend bound (23.9%)** : le CPU passe trop de temps à chercher quelles instructions
exécuter. Cause probable : pqxx instancie des dizaines de fonctions template différentes
(une par type de champ, par type de résultat...). Le code machine généré est très gros et
dépasse la capacité du cache d'instructions (L1i). Le CPU doit constamment recharger des
instructions depuis L2/L3.

**Backend bound (19.2%)** : le CPU attend des données. Source principale : les allocations
`shared_ptr` de pqxx sont éparpillées sur le heap — chaque accès à un compteur de référence
peut pointer vers une adresse différente en mémoire, provoquant de petites latences répétées.

### Conclusion

| Goulot potentiel | Verdict |
|---|---|
| Branch mispredictions | ✅ Pas un problème (0.14%) |
| Cache misses RAM (LLC) | ✅ Pas un problème (2.1M misses seulement) |
| Instruction cache (frontend) | 🟡 23.9% — pqxx template bloat |
| Latence heap pqxx (backend) | 🟡 19.2% — shared_ptr éparpillés |

Le profiling perf confirme ce que gprof indiquait : le vrai goulot est la **couche
pqxx** (désérialisation + shared_ptr), pas les algorithmes ni les accès mémoire au sens
large. Optimiser les accès à la map ou le calcul des indicateurs ne donnerait rien de
significatif tant que pqxx reste dans la boucle critique.

**Prochaine piste** : `pqxx::stream_from` pour bypasser le `pqxx::result` et ses
`shared_ptr` — ou passer à la Phase 7 du roadmap.
