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

## Mesures suivantes

*(à compléter au fil des optimisations)*

| Version | compute_all() | Elapsed | CPU | RAM | Gain |
|---|---|---|---|---|---|
| Baseline séquentielle | 3 057 ms | 64.87 s | 59% | 3.04 GB | — |
