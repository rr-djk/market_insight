-- =============================================================================
-- Market Insight - Schéma de base de données PostgreSQL
-- =============================================================================

-- Suppression des tables existantes (si elles existent)
DROP TABLE IF EXISTS historical_prices CASCADE;
DROP TABLE IF EXISTS symbols CASCADE;

-- =============================================================================
-- Table: symbols
-- =============================================================================
CREATE TABLE symbols (
    symbol_id   SERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL UNIQUE
);

-- =============================================================================
-- Table: historical_prices
-- =============================================================================
CREATE TABLE historical_prices (
    price_id    BIGSERIAL PRIMARY KEY,
    symbol_id   INTEGER NOT NULL REFERENCES symbols(symbol_id) ON DELETE CASCADE,
    trade_date  DATE NOT NULL,
    open        NUMERIC(18, 6) NOT NULL,
    high        NUMERIC(18, 6) NOT NULL,
    low         NUMERIC(18, 6) NOT NULL,
    close       NUMERIC(18, 6) NOT NULL,
    volume      BIGINT NOT NULL,

    CONSTRAINT unique_symbol_date UNIQUE (symbol_id, trade_date)
);

-- =============================================================================
-- Index pour optimisation des requêtes
-- =============================================================================
CREATE INDEX idx_prices_trade_date ON historical_prices(trade_date);
CREATE INDEX idx_prices_symbol_id ON historical_prices(symbol_id);
CREATE INDEX idx_prices_symbol_date ON historical_prices(symbol_id, trade_date);

-- =============================================================================
-- Vue pour requêtes simplifiées (prix + symbole)
-- =============================================================================
CREATE OR REPLACE VIEW v_prices AS
SELECT
    s.symbol,
    hp.trade_date,
    hp.open,
    hp.high,
    hp.low,
    hp.close,
    hp.volume
FROM historical_prices hp
JOIN symbols s ON hp.symbol_id = s.symbol_id;
