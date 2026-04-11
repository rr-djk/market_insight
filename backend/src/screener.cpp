#include "screener.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>

Screener::Screener(Database& database,
                   unsigned int chunk,
                   const std::string& start,
                   const std::string& end)
    : db(database)
    , chunk_size(chunk == 0 ? 1 : chunk)
    , start_date(start)
    , end_date(end)
{}

// =============================================================================
// Helpers privés
// =============================================================================

std::vector<double> Screener::compute_log_returns(const std::vector<Price>& prices) const
{
    if (prices.size() < 2)
        return {};

    std::vector<double> returns;
    returns.reserve(prices.size() - 1);

    for (std::size_t i = 1; i < prices.size(); ++i) {
        if (prices[i - 1].close <= 0.0 || prices[i].close <= 0.0)
            return {};
        returns.push_back(std::log(prices[i].close / prices[i - 1].close));
    }

    return returns;
}

ScreenerResult Screener::compute_metrics(const std::string& symbol,
                                          const std::vector<Price>& prices) const
{
    const double nan = std::numeric_limits<double>::quiet_NaN();
    ScreenerResult result{symbol, nan, nan, nan,
                          static_cast<unsigned int>(prices.size())};

    if (prices.size() < 2)
        return result;

    const auto log_returns = compute_log_returns(prices);
    if (log_returns.size() < 2)
        return result;

    const double n = static_cast<double>(log_returns.size());

    // Moyenne et écart-type des rendements log (variance non biaisée, N-1)
    const double mean = std::accumulate(log_returns.begin(), log_returns.end(), 0.0) / n;

    double variance = 0.0;
    for (double r : log_returns)
        variance += (r - mean) * (r - mean);
    variance /= (n - 1.0);

    const double std_dev = std::sqrt(variance);

    result.annualized_volatility = std_dev * std::sqrt(252.0);

    // Sharpe annualisé (taux sans risque nul)
    if (std_dev > 0.0)
        result.sharpe_ratio = (mean * 252.0) / (std_dev * std::sqrt(252.0));
    // Si std_dev == 0, sharpe reste NaN

    // Momentum 12 mois : rendement sur les 252 derniers jours disponibles
    if (prices.size() >= 253) {
        const std::size_t last = prices.size() - 1;
        const std::size_t first = last - 252;
        result.momentum_12m = (prices[last].close / prices[first].close) - 1.0;
    }

    return result;
}

// =============================================================================
// Interface publique
// =============================================================================

std::vector<ScreenerResult> Screener::run(const std::vector<std::string>& symbols)
{
    std::vector<ScreenerResult> all_results;
    all_results.reserve(symbols.size());

    for (std::size_t offset = 0; offset < symbols.size(); offset += chunk_size) {
        const std::size_t end = std::min(offset + chunk_size, symbols.size());
        const std::vector<std::string> chunk(symbols.begin() + static_cast<std::ptrdiff_t>(offset),
                                             symbols.begin() + static_cast<std::ptrdiff_t>(end));

        auto chunk_data = db.get_close_prices_bulk(chunk, start_date, end_date);

        for (const auto& sym : chunk) {
            auto it = chunk_data.find(sym);
            if (it == chunk_data.end()) {
                all_results.push_back({sym,
                                       std::numeric_limits<double>::quiet_NaN(),
                                       std::numeric_limits<double>::quiet_NaN(),
                                       std::numeric_limits<double>::quiet_NaN(),
                                       0u});
            } else {
                all_results.push_back(compute_metrics(sym, it->second));
            }
        }
    }

    return all_results;
}

std::vector<ScreenerResult>
Screener::rank_by_sharpe(std::vector<ScreenerResult> results, unsigned int top_n) const
{
    results.erase(
        std::remove_if(results.begin(), results.end(),
                       [](const ScreenerResult& r) { return std::isnan(r.sharpe_ratio); }),
        results.end());

    std::sort(results.begin(), results.end(),
              [](const ScreenerResult& a, const ScreenerResult& b) {
                  return a.sharpe_ratio > b.sharpe_ratio;
              });

    if (static_cast<std::size_t>(top_n) < results.size())
        results.resize(static_cast<std::size_t>(top_n));

    return results;
}

std::vector<ScreenerResult>
Screener::rank_by_momentum(std::vector<ScreenerResult> results, unsigned int top_n) const
{
    results.erase(
        std::remove_if(results.begin(), results.end(),
                       [](const ScreenerResult& r) { return std::isnan(r.momentum_12m); }),
        results.end());

    std::sort(results.begin(), results.end(),
              [](const ScreenerResult& a, const ScreenerResult& b) {
                  return a.momentum_12m > b.momentum_12m;
              });

    if (static_cast<std::size_t>(top_n) < results.size())
        results.resize(static_cast<std::size_t>(top_n));

    return results;
}
