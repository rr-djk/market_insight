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

/**
 * Recherche le prix de cloture exact a une date donnee par recherche binaire.
 * @param prices Vecteur de prix tries par date croissante.
 * @param target Date exacte recherchee.
 * @return Le prix de cloture si la date existe, nullopt sinon.
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
 * Recherche le dernier prix de cloture connu a une date ou avant celle-ci.
 * Utile pour les jours feries ou weekends ou aucun prix n'est disponible.
 * @param prices Vecteur de prix tries par date croissante.
 * @param target Date cible (incluse).
 * @return Le prix de cloture le plus recent, nullopt si aucun prix n'existe avant target.
 */
static std::optional<double> find_price_at_or_before(const std::vector<Price>& prices,
                                                      Date target)
{
    auto it = std::upper_bound(prices.begin(), prices.end(), target,
                               [](Date d, const Price& p) { return d < p.date; });

    if (it == prices.begin()) return std::nullopt;
    --it;
    return it->close;
}

/**
 * Collecte les prix de cloture de plusieurs symboles a une date donnee.
 * Les symboles sans prix disponible sont silencieusement exclus du resultat.
 * @param market Donnees de marche contenant les historiques.
 * @param symbols Liste des tickers a interroger.
 * @param date Date cible.
 * @param exact_only Si true, exige un prix exact a cette date ; sinon accepte le dernier prix connu.
 * @return Map symbole -> prix de cloture pour les symboles avec une donnee disponible.
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
 * Tente d'integrer les symboles en attente dans la liste des symboles disponibles.
 * Un symbole passe de pending a available des qu'un prix existe a ou avant la date courante.
 * @param pending Liste des symboles sans prix au demarrage (modifiee en place).
 * @param available Liste des symboles actifs (modifiee en place si de nouveaux symboles arrivent).
 * @param market Donnees de marche.
 * @param date Date courante de la simulation.
 * @return true si au moins un symbole a ete transfere de pending vers available.
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
 * Deduit la fenetre temporelle de simulation a partir des donnees disponibles.
 * La date de debut est la premiere cotation toutes series confondues,
 * la date de fin est la derniere.
 * @param market Donnees de marche.
 * @param symbols Liste des tickers a considerer.
 * @return Paire (start_date, end_date), ou nullopt si toutes les series sont vides.
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

/**
 * Vend la totalite des positions detenues dans le portefeuille aux prix fournis.
 * @param portfolio Portefeuille dont les positions seront liquidees.
 * @param current_prices Prix de vente par symbole (doit couvrir tous les symboles detenus).
 * @param date Date de la transaction.
 */
static void sell_all_positions(Portfolio& portfolio,
                               const std::unordered_map<std::string, double>& current_prices,
                               Date date)
{
    auto positions = portfolio.get_stock_positions();
    std::string date_str = format_date(date);
    for (const auto& [symbol, qty] : positions) {
        if (qty > 0) {
            portfolio.sell_stock(symbol, qty, current_prices.at(symbol), date_str);
        }
    }
}

/**
 * Repartit le cash disponible du portefeuille a parts egales entre les symboles cibles.
 * Les symboles sans prix dans current_prices sont ignores.
 * Le cash residuel (du aux arrondis en nombre entier d'actions) reste en liquidites.
 * @param portfolio Portefeuille dont le cash sera investi.
 * @param target_symbols Symboles a ponderer egalement.
 * @param current_prices Prix d'achat par symbole.
 * @param date Date de la transaction.
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

/**
 * Met a jour le pic de valeur et le drawdown maximum observes.
 * @param value Valeur courante du portefeuille.
 * @param peak Pic de valeur historique (mis a jour si value > peak).
 * @param max_drawdown Drawdown maximum observe (mis a jour si le drawdown courant le depasse).
 */
static void update_drawdown(double value, double& peak, double& max_drawdown)
{
    if (value > peak) peak = value;
    double drawdown = (peak - value) / peak;
    if (drawdown > max_drawdown) max_drawdown = drawdown;
}

/**
 * Calcule le rendement annualise a partir du rendement total sur une periode.
 * @param initial_value Valeur initiale du portefeuille.
 * @param final_value Valeur finale du portefeuille.
 * @param calendar_days Duree totale de la simulation en jours calendaires.
 * @return Rendement annualise en pourcentage, ou 0 si calendar_days vaut 0.
 */
static double compute_annualized_return(double initial_value,
                                         double final_value,
                                         unsigned int calendar_days)
{
    if (calendar_days == 0) return 0.0;
    double ratio = final_value / initial_value;
    return (std::pow(ratio, 365.0 / static_cast<double>(calendar_days)) - 1.0) * 100.0;
}

/**
 * Separe les symboles en deux groupes selon leur disponibilite a la date de depart.
 * Un symbole est disponible s'il possede un prix exact a start_date ; sinon il est en attente.
 * @param market Donnees de marche.
 * @param symbols Liste complete des tickers a classifier.
 * @param start_date Date de debut de la simulation.
 * @return Paire (available, pending).
 */
static std::pair<std::vector<std::string>, std::vector<std::string>>
classify_symbols_by_availability(const MarketData& market,
                                 const std::vector<std::string>& symbols,
                                 Date start_date)
{
    std::vector<std::string> available, pending;
    for (const auto& symbol : symbols) {
        if (find_exact_price(market.get_prices(symbol), start_date)) {
            available.push_back(symbol);
        } else {
            pending.push_back(symbol);
        }
    }
    return {available, pending};
}

/**
 * Effectue l'achat initial : distribue le cash egalement entre les symboles disponibles
 * et enregistre la valeur du portefeuille au point de depart.
 * @param portfolio Portefeuille a initialiser.
 * @param available Symboles disponibles a la date de depart.
 * @param market Donnees de marche.
 * @param start_date Date de debut de la simulation.
 * @param result Structure de resultats (daily_portfolio_values mis a jour).
 * @param peak_value Pic de valeur courant (mis a jour).
 * @param max_drawdown Drawdown maximum courant (mis a jour).
 */
static void run_initial_distribution(Portfolio& portfolio,
                                    const std::vector<std::string>& available,
                                    const MarketData& market,
                                    Date start_date,
                                    BacktestResult& result,
                                    double& peak_value,
                                    double& max_drawdown)
{
    auto prices = collect_prices(market, available, start_date, true);
    sell_all_positions(portfolio, prices, start_date);
    distribute_equally(portfolio, available, prices, start_date);

    double value = portfolio.compute_total_value(prices);
    result.daily_portfolio_values.push_back(value);
    update_drawdown(value, peak_value, max_drawdown);
}

/**
 * Determine si un reequilibrage doit avoir lieu au pas courant.
 * Priorite au reequilibrage declenche par un nouveau symbole, puis au reequilibrage periodique.
 * @param strategy Strategie interrogee.
 * @param new_symbols_available true si au moins un symbole vient d'etre integre.
 * @param current_date Date courante de la simulation.
 * @param last_rebalance_date Date du dernier reequilibrage effectue.
 * @return true si un reequilibrage doit etre execute.
 */
static bool evaluate_rebalance_needs(Strategy& strategy,
                                    bool new_symbols_available,
                                    Date current_date,
                                    Date last_rebalance_date)
{
    if (new_symbols_available && strategy.should_rebalance_on_new_symbol())
        return true;

    unsigned int days_since = static_cast<unsigned int>(
        (current_date - last_rebalance_date).count());
    return strategy.should_rebalance(days_since);
}

/**
 * Liquide toutes les positions et redistribue le capital egalement entre les symboles disponibles.
 * Incremente le compteur de reequilibrages dans result.
 * @param portfolio Portefeuille a reequilibrer.
 * @param available Symboles actifs au moment du reequilibrage.
 * @param market Donnees de marche.
 * @param current_date Date du reequilibrage.
 * @param result Structure de resultats (total_rebalances incremente).
 */
static void perform_rebalance(Portfolio& portfolio,
                              const std::vector<std::string>& available,
                              const MarketData& market,
                              Date current_date,
                              BacktestResult& result)
{
    auto prices = collect_prices(market, available, current_date, false);
    sell_all_positions(portfolio, prices, current_date);
    distribute_equally(portfolio, available, prices, current_date);
    result.total_rebalances++;
}

/**
 * Evalue la valeur courante du portefeuille et l'ajoute a l'historique.
 * @param portfolio Portefeuille a valoriser.
 * @param market Donnees de marche.
 * @param available Symboles actifs a cette date.
 * @param current_date Date d'evaluation.
 * @param result Structure de resultats (daily_portfolio_values mis a jour).
 * @param peak_value Pic de valeur courant (mis a jour).
 * @param max_drawdown Drawdown maximum courant (mis a jour).
 */
static void record_daily_value(Portfolio& portfolio,
                               const MarketData& market,
                               const std::vector<std::string>& available,
                               Date current_date,
                               BacktestResult& result,
                               double& peak_value,
                               double& max_drawdown)
{
    auto prices = collect_prices(market, available, current_date, false);
    double value = portfolio.compute_total_value(prices);
    result.daily_portfolio_values.push_back(value);
    update_drawdown(value, peak_value, max_drawdown);
}

/**
 * Execute la boucle principale de simulation entre start_date et end_date.
 * Avance par pas de strategy.get_period_days(), resout les symboles en attente,
 * declenche les reequilibrages si necessaire, et enregistre la valeur a chaque pas.
 * @param portfolio Portefeuille de la simulation.
 * @param strategy Strategie appliquee.
 * @param market Donnees de marche.
 * @param available Symboles disponibles a start_date (mis a jour en cours de simulation).
 * @param pending Symboles en attente a start_date (mis a jour en cours de simulation).
 * @param start_date Date de debut.
 * @param end_date Date de fin (exclue de la boucle, traitee par capture_final_snapshot).
 * @param result Structure de resultats alimentee a chaque pas.
 * @param peak_value Pic de valeur courant (mis a jour).
 * @param max_drawdown Drawdown maximum courant (mis a jour).
 */
static void run_simulation_loop(Portfolio& portfolio,
                                Strategy& strategy,
                                const MarketData& market,
                                std::vector<std::string>& available,
                                std::vector<std::string>& pending,
                                Date start_date,
                                Date end_date,
                                BacktestResult& result,
                                double& peak_value,
                                double& max_drawdown)
{
    if (available.empty()) return;

    run_initial_distribution(portfolio, available, market, start_date,
                              result, peak_value, max_drawdown);

    Date last_rebalance_date = start_date;
    unsigned int period_days = strategy.get_period_days();
    Date current_date = start_date + std::chrono::days(period_days);

    while (current_date < end_date) {
        bool new_symbols = !pending.empty() &&
            resolve_pending_symbols(pending, available, market, current_date);

        bool needs_rebalance = evaluate_rebalance_needs(
            strategy, new_symbols, current_date, last_rebalance_date);

        if (needs_rebalance) {
            perform_rebalance(portfolio, available, market, current_date, result);
            last_rebalance_date = current_date;
        }

        record_daily_value(portfolio, market, available, current_date,
                          result, peak_value, max_drawdown);

        current_date = current_date + std::chrono::days(period_days);
    }
}

/**
 * Capture l'etat final du portefeuille a end_date et calcule les metriques de rendement.
 * Resout les eventuels symboles encore en attente avant la valorisation finale.
 * @param portfolio Portefeuille en fin de simulation.
 * @param market Donnees de marche.
 * @param available Symboles actifs en fin de simulation (mis a jour si pending non vide).
 * @param pending Symboles encore en attente (mis a jour).
 * @param end_date Date de fin de la simulation.
 * @param result Structure de resultats (final_value, total_return_pct, max_drawdown_pct remplis).
 * @param initial_value Capital de depart, utilise pour calculer le rendement total.
 * @param peak_value Pic de valeur courant (mis a jour).
 * @param max_drawdown Drawdown maximum courant (mis a jour).
 */
static void capture_final_snapshot(Portfolio& portfolio,
                                   const MarketData& market,
                                   std::vector<std::string>& available,
                                   std::vector<std::string>& pending,
                                   Date end_date,
                                   BacktestResult& result,
                                   double initial_value,
                                   double& peak_value,
                                   double& max_drawdown)
{
    if (!pending.empty())
        resolve_pending_symbols(pending, available, market, end_date);

    record_daily_value(portfolio, market, available, end_date,
                      result, peak_value, max_drawdown);

    result.final_value = result.daily_portfolio_values.back();
    result.total_return_pct = (result.final_value / initial_value - 1.0) * 100.0;
    result.max_drawdown_pct = max_drawdown * 100.0;
}

/**
 * Calcule le rendement annualise et le stocke dans result.
 * @param result Structure de resultats (annualized_return_pct rempli).
 * @param start_date Date de debut de la simulation.
 * @param end_date Date de fin de la simulation.
 * @param initial_value Capital de depart.
 */
static void compute_calendar_metrics(BacktestResult& result,
                                    Date start_date,
                                    Date end_date,
                                    double initial_value)
{
    unsigned int total_days = static_cast<unsigned int>((end_date - start_date).count());
    result.annualized_return_pct = compute_annualized_return(
        initial_value, result.final_value, total_days);
}

BacktestResult Backtest::execute_backtest(Portfolio& portfolio, Strategy& strategy)
{
    BacktestResult result{};
    double initial_value = portfolio.get_initial_cash();
    result.final_value = initial_value;

    auto date_range = derive_date_range(market, symbols);
    if (!date_range) return result;

    auto [start_date, end_date] = *date_range;
    unsigned int period_days = strategy.get_period_days();
    if (period_days == 0) return result;

    double peak_value = initial_value;
    double max_drawdown = 0.0;

    auto [available, pending] = classify_symbols_by_availability(market, symbols, start_date);

    run_simulation_loop(portfolio, strategy, market, available, pending,
                        start_date, end_date, result, peak_value, max_drawdown);

    if (result.daily_portfolio_values.empty()) return result;

    capture_final_snapshot(portfolio, market, available, pending,
                           end_date, result, initial_value, peak_value, max_drawdown);

    compute_calendar_metrics(result, start_date, end_date, initial_value);

    return result;
}
