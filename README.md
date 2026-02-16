# Market Insight

Plateforme de backtesting de stratégies d'investissement en C++, appliquée sur des données boursières historiques du NASDAQ (~34M lignes, 11 727 symboles).

## Focus MVP

Afin d'accélérer la mise en production, le développement se concentre d'abord sur un **MVP centré sur la théorie de Charles Gaves** : équipondération du portefeuille avec rééquilibrage périodique.

L'utilisateur choisit un panier d'actions, un budget de départ, une période et une fréquence de rééquilibrage. Le MVP produit un backtest comparatif :
- **Avec rééquilibrage** (stratégie Gaves) : le portefeuille est redistribué à parts égales à chaque période
- **Sans rééquilibrage** (buy & hold) : le portefeuille conserve ses positions initiales

Résultat : graphe d'évolution comparative + métriques (rendement, drawdown, nombre de rééquilibrages).

## Vision long-terme

Le projet vise à devenir une plateforme complète avec parallélisation (CPU/GPU CUDA), API REST, frontend React et déploiement AWS. Ces étapes seront abordées après le MVP.

## Avancement

- **Données** : acquisition, validation et import terminés
- **Base de données** : PostgreSQL opérationnel (schéma, indexation, 33.7M lignes)
- **Backend C++** : fondations en place (connexion BDD, structures de données, moteur de backtesting)
- **Stratégies** : EqualWeightStrategy (Gave) et BuyAndHoldStrategy implémentées
- **En cours** : tests des stratégies, interface utilisateur et graphe comparatif

## Prérequis

- Docker et Docker Compose
- Python 3.12+
- g++ avec support C++20
- libpqxx 7.8.1 (compilée depuis les sources avec C++20)
- libpq-dev

## Mise en route rapide

```bash
# Démarrer PostgreSQL
docker-compose up -d

# Compiler le backend
cd backend && make

# Exécuter
make run
```
