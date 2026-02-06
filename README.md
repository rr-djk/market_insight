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

### Lancer les services

```bash
# Démarrer PostgreSQL et pgAdmin
docker-compose up -d

# Vérifier que les services tournent
docker ps
```

### Accès pgAdmin
- URL : http://localhost:5050
- Credentials : voir fichier `.env`

## Structure du projet

```
market_insight/
├── data/
│   ├── raw/                 # Données CSV (11 727 fichiers)
│   └── nasdaq_symbols.csv   # Liste des symboles
├── db/
│   └── schema.sql           # Schéma PostgreSQL
├── scripts/
│   ├── get_nasdaq_symbols.py
│   ├── download_full_data.py
│   ├── validate_data.py
│   └── import_to_postgres.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Base de données

### Schéma

```sql
-- Symboles boursiers
CREATE TABLE symbols (
    symbol_id   SERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL UNIQUE
);

-- Données historiques OHLCV
CREATE TABLE historical_prices (
    price_id    BIGSERIAL PRIMARY KEY,
    symbol_id   INTEGER REFERENCES symbols(symbol_id),
    trade_date  DATE NOT NULL,
    open        NUMERIC(18, 6),
    high        NUMERIC(18, 6),
    low         NUMERIC(18, 6),
    close       NUMERIC(18, 6),
    volume      BIGINT,
    UNIQUE (symbol_id, trade_date)
);
```

### Statistiques
- Symboles : 11 727
- Lignes : 33 748 226
- Période : 1980 - 2025

## Licence

Projet personnel à but pédagogique.
