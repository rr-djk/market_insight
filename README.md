# Market Insight

Plateforme de backtesting de stratégies d'investissement avec backend C++, parallélisation CPU/GPU (CUDA), et déploiement AWS.

## Objectif

Tester des stratégies d'investissement paramétrables sur des données boursières historiques (NASDAQ), en mettant l'accent sur la performance backend et le traitement de gros volumes de données.

## État actuel

### Phase 1 : Données
- [x] Téléchargement des symboles NASDAQ (12 231 symboles)
- [x] Téléchargement des données historiques (11 727 fichiers)
- [x] Validation des données

### Phase 2 : Base de données
- [x] PostgreSQL via Docker
- [x] Schéma de base de données (`symbols`, `historical_prices`)
- [x] Import des données : **33.7M lignes**

### Phase 3-10 : À venir
- [ ] Backend C++ avec connexion PostgreSQL
- [ ] Moteur de backtesting (CPU)
- [ ] Parallélisation multi-thread
- [ ] Accélération GPU (CUDA)
- [ ] API REST
- [ ] Frontend React
- [ ] Déploiement AWS

## Stack technique

| Composant | Technologie |
|-----------|-------------|
| Données | Python, pandas, yfinance |
| Base de données | PostgreSQL 16 |
| Backend | C++20 (à venir) |
| Parallélisation | std::thread, CUDA (à venir) |
| Frontend | React + TypeScript (à venir) |
| Infrastructure | Docker, Docker Compose |

## Installation

### Prérequis
- Docker et Docker Compose
- Python 3.12+
- (À venir) Compilateur C++20, CUDA Toolkit

## Mise en route rapide

```bash
# Démarrer PostgreSQL et pgAdmin
docker-compose up -d

# Vérifier que les services tournent
docker ps
