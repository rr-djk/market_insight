#include "backtest.hpp"
#include "date.hpp"
#include <algorithm>
#include <cmath>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

Backtest::Backtest(const MarketData& market_data,
                   const std::vector<std::string>& symbol_list)
    : market(market_data), symbols(symbol_list) {}

// =============================================================================
// Recherche binaire sur les prix
// =============================================================================

/**
 * Retourne le prix de cloture a la date exactement egale a target.
 * Retourne nullopt si aucun prix ne correspond.
 */
static std::optional<double> find_exact_price(const std::vector<Price>& prices,
                                              Date target)
{
    auto it = std::lower_bound(prices.begin(), prices.end(), target,
                               [](const Price& p, Date d) { return p.date < d; });

    if (it == prices.end() || it->date != target) return std::nullopt;
    return it->close;
}

/**
 * Retourne le prix de cloture a la date la plus recente inferieure ou egale a target.
 * Retourne nullopt si toutes les dates du symbole sont posterieures a target.
 */
static std::optional<double> find_price_at_or_before(const std::vector<Price>& prices,
                                                      Date target)
{
    // upper_bound trouve le premier element dont la date > target
    auto it = std::upper_bound(prices.begin(), prices.end(), target,
                               [](Date d, const Price& p) { return d < p.date; });

    if (it == prices.begin()) return std::nullopt;
    --it;
    return it->close;
}

// =============================================================================
// Collecte des prix et gestion des symboles
// =============================================================================

/**
 * Collecte les prix disponibles pour chaque symbole a la date donnee.
 * Si exact_only, seule une correspondance exacte est acceptee.
 * Les symboles sans prix disponible sont simplement ignores.
 */
static std::unordered_map<std::string, double> collect_prices(
        const MarketData& market,
        const std::vector<std::string>& symbols,
        Date date,
        bool exact_only)
{
    std::unordered_map<std::string, double> prices;
    for (const auto& symbol : symbols) {
        const auto& price_vec = market.get_prices(symbol);
        std::optional<double> price = exact_only
            ? find_exact_price(price_vec, date)
            : find_price_at_or_before(price_vec, date);
        if (price) prices[symbol] = *price;
    }
    return prices;
}

/**
 * Verifie si des symboles en attente ont maintenant des donnees a la date donnee.
 * Les symboles devenus disponibles sont transferes vers available et retires de pending.
 * Retourne true si au moins un nouveau symbole est devenu disponible.
 */
static bool resolve_pending_symbols(std::vector<std::string>& pending,
                                    std::vector<std::string>& available,
                                    const MarketData& market,
                                    Date date)
{
    bool found_new = false;
    std::vector<std::string> still_pending;

    for (const auto& symbol : pending) {
        if (find_price_at_or_before(market.get_prices(symbol), date)) {
            available.push_back(symbol);
            found_new = true;
        } else {
            still_pending.push_back(symbol);
        }
    }

    pending = std::move(still_pending);
    return found_new;
}

/**
 * Deduit la fenetre de simulation depuis les donnees de marche disponibles.
 * Retourne nullopt si aucun symbole n'a de donnees.
 */
static std::optional<std::pair<Date, Date>> derive_date_range(
        const MarketData& market,
        const std::vector<std::string>& symbols)
{
    std::optional<Date> start, end;

    for (const auto& symbol : symbols) {
        const auto& prices = market.get_prices(symbol);
        if (prices.empty()) continue;
        if (!start || prices.front().date < *start) start = prices.front().date;
        if (!end   || prices.back().date  > *end)   end   = prices.back().date;
    }

    if (!start) return std::nullopt;
    return std::make_pair(*start, *end);
}

// =============================================================================
// Transactions et repartition du capital
// =============================================================================

/**
 * Vend toutes les positions du portefeuille aux prix donnes.
 */
static void sell_all_positions(Portfolio& portfolio,
                               const std::unordered_map<std::string, double>& current_prices,
                               Date date)
{
    std::string date_str = format_date(date);
    for (const auto& [symbol, qty] : portfolio.get_stock_positions()) {
        if (qty > 0) {
            portfolio.sell_stock(symbol, qty, current_prices.at(symbol), date_str);
        }
    }
}

/**
 * Repartit le cash disponible a parts egales entre les symboles ayant un prix connu.
 */
static void distribute_equally(Portfolio& portfolio,
                               const std::vector<std::string>& target_symbols,
                               const std::unordered_map<std::string, double>& current_prices,
                               Date date)
{
    if (target_symbols.empty()) return;

    std::string date_str = format_date(date);
    double cash_per_symbol =
        portfolio.get_available_cash() / static_cast<double>(target_symbols.size());

    for (const auto& symbol : target_symbols) {
        auto it = current_prices.find(symbol);
        if (it == current_prices.end()) continue;

        unsigned int shares = static_cast<unsigned int>(cash_per_symbol / it->second);
        if (shares > 0) {
            portfolio.buy_stock(symbol, shares, it->second, date_str);
        }
    }
}

// =============================================================================
// Metriques
// =============================================================================

/**
 * Met a jour le pic de valeur et le drawdown maximum de facon incrementale.
 */
static void update_drawdown(double value, double& peak, double& max_drawdown)
{
    if (value > peak) peak = value;
    double drawdown = (peak - value) / peak;
    if (drawdown > max_drawdown) max_drawdown = drawdown;
}

/**
 * Calcule le rendement annualise a partir du ratio final/initial
 * et du nombre de jours calendaires total.
 */
static double compute_annualized_return(double initial_value,
                                        double final_value,
                                        unsigned int calendar_days)
{
    if (calendar_days == 0) return 0.0;
    double ratio = final_value / initial_value;
    return (std::pow(ratio, 365.0 / static_cast<double>(calendar_days)) - 1.0) * 100.0;
}

// =============================================================================
// Moteur principal
// =============================================================================

BacktestResult Backtest::execute_backtest(Portfolio& portfolio, Strategy& strategy)
{
    BacktestResult result{};
    double initial_value = portfolio.get_initial_cash();
    result.final_value = initial_value;

    auto date_range = derive_date_range(market, symbols);
    if (!date_range) return result;

    auto [start_date, end_date] = *date_range;
    unsigned int period_days = strategy.get_period_days();

    double peak_value  = initial_value;
    double max_drawdown = 0.0;

    // -------------------------------------------------------------------------
    // Premiere iteration : date exacte de depart obligatoire.
    // Les symboles sans donnees a cette date exacte sont mis en attente.
    // -------------------------------------------------------------------------
    std::vector<std::string> available, pending;
    for (const auto& symbol : symbols) {
        if (find_exact_price(market.get_prices(symbol), start_date)) {
            available.push_back(symbol);
        } else {
            pending.push_back(symbol);
        }
    }

    if (available.empty()) return result;

    auto initial_prices = collect_prices(market, available, start_date, true);
    sell_all_positions(portfolio, initial_prices, start_date);
    distribute_equally(portfolio, available, initial_prices, start_date);

    double value = portfolio.compute_total_value(initial_prices);
    result.daily_portfolio_values.push_back(value);
    update_drawdown(value, peak_value, max_drawdown);

    Date last_rebalance_date = start_date;

    // -------------------------------------------------------------------------
    // Iterations suivantes : on avance par pas de period_days.
    // A chaque etape on verifie si des symboles en attente sont devenus
    // disponibles, puis on reequilibre selon la strategie.
    // -------------------------------------------------------------------------
    Date current_date = start_date + std::chrono::days(period_days);

    while (current_date < end_date) {
        bool new_symbols = !pending.empty() &&
            resolve_pending_symbols(pending, available, market, current_date);

        auto current_prices = collect_prices(market, available, current_date, false);

        bool should_rebalance = false;
        if (new_symbols && strategy.should_rebalance_on_new_symbol())
            should_rebalance = true;

        unsigned int days_since = static_cast<unsigned int>(
            (current_date - last_rebalance_date).count());
        if (strategy.should_rebalance(days_since))
            should_rebalance = true;

        if (should_rebalance) {
            sell_all_positions(portfolio, current_prices, current_date);
            distribute_equally(portfolio, available, current_prices, current_date);
            last_rebalance_date = current_date;
            result.total_rebalances++;
        }

        value = portfolio.compute_total_value(current_prices);
        result.daily_portfolio_values.push_back(value);
        update_drawdown(value, peak_value, max_drawdown);

        current_date = current_date + std::chrono::days(period_days);
    }

    // -------------------------------------------------------------------------
    // Instantane final a end_date : valeur reelle au dernier jour de simulation.
    // Pas de reequilibrage — on enregistre uniquement la valeur de marche.
    // -------------------------------------------------------------------------
    if (!pending.empty())
        resolve_pending_symbols(pending, available, market, end_date);

    auto final_prices = collect_prices(market, available, end_date, false);
    value = portfolio.compute_total_value(final_prices);
    result.daily_portfolio_values.push_back(value);
    update_drawdown(value, peak_value, max_drawdown);

    // -------------------------------------------------------------------------
    // Calcul des metriques finales
    // -------------------------------------------------------------------------
    result.final_value         = result.daily_portfolio_values.back();
    result.total_return_pct    = (result.final_value / initial_value - 1.0) * 100.0;
    result.max_drawdown_pct    = max_drawdown * 100.0;

    unsigned int total_days = static_cast<unsigned int>((end_date - start_date).count());
    result.annualized_return_pct = compute_annualized_return(
        initial_value, result.final_value, total_days);

    return result;
}
