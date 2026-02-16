#ifndef BACKTEST_RESULT_HPP
#define BACKTEST_RESULT_HPP

#include <vector>

/**
 * Resultats d'une simulation de backtest.
 */
struct BacktestResult {
    double final_value;              // Valeur finale du portefeuille.
    double total_return_pct;         // Rendement total en pourcentage.
    double annualized_return_pct;    // Rendement annualise en pourcentage.
    double max_drawdown_pct;         // Drawdown maximum en pourcentage.
    double total_rebalances;
    std::vector<double> daily_portfolio_values;  // Valeur du portefeuille a chaque jour simule.
};

#endif
