#include "technical_indicators.hpp"
#include <cmath>
#include <limits>
#include <vector>
#include <string>
#include <unordered_map>

static constexpr double NaN = std::numeric_limits<double>::quiet_NaN();

std::vector<double> compute_sma(const std::vector<Price>& prices, unsigned int period)
{
    const size_t n = prices.size();
    std::vector<double> result(n, NaN);

    if (period == 0 || n < period) return result;

    double sum = 0.0;
    for (unsigned int i = 0; i < period; ++i)
        sum += prices[i].close;

    result[period - 1] = sum / static_cast<double>(period);

    for (size_t i = period; i < n; ++i) {
        sum += prices[i].close - prices[i - period].close;
        result[i] = sum / static_cast<double>(period);
    }

    return result;
}

std::vector<double> compute_ema(const std::vector<Price>& prices, unsigned int period)
{
    const size_t n = prices.size();
    std::vector<double> result(n, NaN);

    if (period == 0 || n < period) return result;

    double sum = 0.0;
    for (unsigned int i = 0; i < period; ++i)
        sum += prices[i].close;

    result[period - 1] = sum / static_cast<double>(period);

    const double k = 2.0 / (static_cast<double>(period) + 1.0);
    for (size_t i = period; i < n; ++i)
        result[i] = prices[i].close * k + result[i - 1] * (1.0 - k);

    return result;
}

std::vector<double> compute_rsi(const std::vector<Price>& prices, unsigned int period)
{
    const size_t n = prices.size();
    std::vector<double> result(n, NaN);

    if (period == 0 || n <= period) return result;

    double avg_gain = 0.0, avg_loss = 0.0;
    for (unsigned int i = 1; i <= period; ++i) {
        double change = prices[i].close - prices[i - 1].close;
        if (change > 0.0) avg_gain += change;
        else              avg_loss += -change;
    }
    avg_gain /= static_cast<double>(period);
    avg_loss /= static_cast<double>(period);

    result[period] = (avg_loss == 0.0) ? 100.0
                                       : 100.0 - 100.0 / (1.0 + avg_gain / avg_loss);

    for (size_t i = period + 1; i < n; ++i) {
        double change = prices[i].close - prices[i - 1].close;
        double gain   = (change > 0.0) ? change  : 0.0;
        double loss   = (change < 0.0) ? -change : 0.0;

        avg_gain = (avg_gain * static_cast<double>(period - 1) + gain)
                   / static_cast<double>(period);
        avg_loss = (avg_loss * static_cast<double>(period - 1) + loss)
                   / static_cast<double>(period);

        result[i] = (avg_loss == 0.0) ? 100.0
                                      : 100.0 - 100.0 / (1.0 + avg_gain / avg_loss);
    }

    return result;
}

std::vector<double> compute_macd(const std::vector<Price>& prices,
                             unsigned int fast,
                             unsigned int slow)
{
    const size_t n = prices.size();
    std::vector<double> result(n, NaN);

    if (fast == 0 || slow == 0 || fast >= slow || n < slow) return result;

    auto ema_fast = compute_ema(prices, fast);
    auto ema_slow = compute_ema(prices, slow);

    for (size_t i = slow - 1; i < n; ++i)
        result[i] = ema_fast[i] - ema_slow[i];

    return result;
}

/**
 * Calcule une bande de Bollinger (superieure ou inferieure) en un seul passage.
 * Utilise la formule var = E[x²] - E[x]² pour eviter une double iteration.
 * @param prices Historique de prix.
 * @param period Taille de la fenetre.
 * @param std_dev Multiplicateur de l'ecart-type.
 * @param upper Si true, retourne la bande superieure ; sinon la bande inferieure.
 * @return Vecteur de taille prices.size(). Les period-1 premiers elements sont NaN.
 * @note La variance est plafonnee a 0 pour absorber les erreurs d'arrondi flottant.
 */
static std::vector<double> compute_bollinger_band(const std::vector<Price>& prices,
                                              unsigned int period,
                                              double std_dev,
                                              bool upper)
{
    const size_t n = prices.size();
    std::vector<double> result(n, NaN);

    if (period == 0 || n < period) return result;

    double sum = 0.0, sum_sq = 0.0;
    for (unsigned int i = 0; i < period; ++i) {
        sum    += prices[i].close;
        sum_sq += prices[i].close * prices[i].close;
    }

    auto compute_band = [&]() {
        double mean     = sum / static_cast<double>(period);
        double variance = sum_sq / static_cast<double>(period) - mean * mean;
        double sigma    = std::sqrt(std::max(0.0, variance));
        return upper ? mean + std_dev * sigma : mean - std_dev * sigma;
    };

    result[period - 1] = compute_band();

    for (size_t i = period; i < n; ++i) {
        sum    += prices[i].close - prices[i - period].close;
        sum_sq += prices[i].close          * prices[i].close
                - prices[i - period].close * prices[i - period].close;
        result[i] = compute_band();
    }

    return result;
}

std::vector<double> compute_bollinger_upper(const std::vector<Price>& prices,
                                        unsigned int period,
                                        double std_dev)
{
    return compute_bollinger_band(prices, period, std_dev, true);
}

std::vector<double> compute_bollinger_lower(const std::vector<Price>& prices,
                                        unsigned int period,
                                        double std_dev)
{
    return compute_bollinger_band(prices, period, std_dev, false);
}

std::unordered_map<std::string, IndicatorSet>
compute_all(const MarketData& market, const std::vector<std::string>& symbols)
{
    std::unordered_map<std::string, IndicatorSet> results;
    results.reserve(symbols.size());

    for (const auto& symbol : symbols) {
        const auto& prices = market.get_prices(symbol);
        IndicatorSet set;
        set.sma             = compute_sma(prices);
        set.ema             = compute_ema(prices);
        set.rsi             = compute_rsi(prices);
        set.macd            = compute_macd(prices);
        set.bollinger_upper = compute_bollinger_upper(prices);
        set.bollinger_lower = compute_bollinger_lower(prices);
        results[symbol]     = std::move(set);
    }

    return results;
}
