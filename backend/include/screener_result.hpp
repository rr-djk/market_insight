#ifndef SCREENER_RESULT_HPP
#define SCREENER_RESULT_HPP

#include <string>

/**
 * Metriques calculees pour un symbole par le Screener.
 * Les valeurs NaN indiquent que l'historique est insuffisant pour le calcul.
 */
struct ScreenerResult {
    /// Ticker du symbole (ex: "AAPL").
    std::string symbol;

    /// Sharpe ratio annualise (taux sans risque nul).
    /// NaN si moins de 2 jours d'historique ou si la volatilite est nulle.
    double sharpe_ratio;

    /// Volatilite annualisee (ecart-type des rendements log * sqrt(252)).
    /// NaN si moins de 2 jours d'historique.
    double annualized_volatility;

    /// Rendement cumulatif sur les 252 derniers jours disponibles (momentum 12 mois).
    /// NaN si moins de 253 jours d'historique.
    double momentum_12m;

    /// Nombre de jours de donnees disponibles pour ce symbole.
    unsigned int data_points;
};

#endif
