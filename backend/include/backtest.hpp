#ifndef BACKTEST_HPP
#define BACKTEST_HPP

#include <string>
#include <vector>
#include "backtest_result.hpp"
#include "market_data.hpp"
#include "portfolio.hpp"
#include "strategy.hpp"

class Backtest {
public:
    /**
     * Cree un moteur de backtesting avec un contexte de marche et les symboles a simuler.
     * @param market Donnees de marche contenant les prix historiques.
     * @param symbols Liste des tickers a simuler (ex: {"AAPL", "MSFT"}).
     */
    Backtest(const MarketData& market,
             const std::vector<std::string>& symbols);

    /**
     * Simule une strategie sur les symboles configures et retourne les resultats.
     * @param portfolio Portefeuille a utiliser pour la simulation (cash, positions).
     * @param strategy Strategie de trading a appliquer.
     * @return Resultats du backtest (rendement, drawdown, etc.).
     */
    BacktestResult execute_backtest(Portfolio& portfolio, Strategy& strategy);

private:
    const MarketData& market;
    std::vector<std::string> symbols;
};

#endif
