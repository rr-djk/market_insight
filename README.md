# Market Insight

Plateforme de backtesting qui permet de tester des stratégies d'investissement en recueillant les règles de l'utilisateur (seuils d'achat/vente, sélection d'actions) et en les appliquant sur des données boursières historiques du NASDAQ (~34M lignes, 11 727 symboles).

Projet à but académique visant à :
- comprendre l'architecture d'une application complète (frontend, backend, base de données, déploiement)
- approfondir le C++ dans un contexte concret
- implémenter du parallélisme CPU et GPU (CUDA)
- manipuler des bases de données avec de gros volumes de données
- découvrir le déploiement cloud (AWS)

## Avancement

- **Données** : acquisition, validation et import terminés
- **Base de données** : PostgreSQL opérationnel (schéma, indexation, 33.7M lignes)
- **Backend C++** : fondations en place (connexion BDD, structures de données, interface stratégies)
- **En cours** : moteur de backtesting séquentiel

## Prérequis

- Docker et Docker Compose
- Python 3.12+
- g++ avec support C++20
- libpqxx 7.8.1 (compilée depuis les sources avec C++20)
- libpq-dev

## Quickstart

```bash
# Démarrer PostgreSQL
docker-compose up -d

# Compiler le backend
cd backend && make

# Exécuter
make run
```

## Licence

Projet personnel à but pédagogique.
