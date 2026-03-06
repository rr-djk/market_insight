#include <gtest/gtest.h>
#include "backtest.hpp"
#include "buy_and_hold_strategy.hpp"
#include "equal_weight_strategy.hpp"

// =============================================================================
// Helpers
// =============================================================================

static Price make_price(const std::string& date, double close) {
    return {*parse_date(date), close, close, close, close, 1000};
}

static MarketData make_single_market(const std::string& symbol,
                                     const std::vector<Price>& prices) {
    std::map<std::string, std::vector<Price>> data;
    data[symbol] = prices;
    return MarketData(data);
}

static MarketData make_dual_market(const std::string& symbol_a,
                                   const std::vector<Price>& prices_a,
                                   const std::string& symbol_b,
                                   const std::vector<Price>& prices_b) {
    std::map<std::string, std::vector<Price>> data;
    data[symbol_a] = prices_a;
    data[symbol_b] = prices_b;
    return MarketData(data);
}

static MarketData make_triple_market(const std::string& sym_a,
                                     const std::vector<Price>& pa,
                                     const std::string& sym_b,
                                     const std::vector<Price>& pb,
                                     const std::string& sym_c,
                                     const std::vector<Price>& pc) {
    std::map<std::string, std::vector<Price>> data;
    data[sym_a] = pa;
    data[sym_b] = pb;
    data[sym_c] = pc;
    return MarketData(data);
}

// =============================================================================
// BuyAndHoldStrategy
// =============================================================================

TEST(BuyAndHoldTest, SingleStockProfit) {
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-02", 150.0)
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // 10000 / 100 = 100 actions. Jour 1 : 100 * 150 = 15000
    EXPECT_DOUBLE_EQ(result.final_value, 15000.0);
    EXPECT_DOUBLE_EQ(result.total_return_pct, 50.0);
    EXPECT_EQ(result.total_rebalances, 0u);
}

TEST(BuyAndHoldTest, SingleStockLoss) {
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-02", 80.0)
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // 100 actions, jour 1 : 100 * 80 = 8000
    EXPECT_DOUBLE_EQ(result.final_value, 8000.0);
    EXPECT_DOUBLE_EQ(result.total_return_pct, -20.0);
    EXPECT_EQ(result.total_rebalances, 0u);
}

TEST(BuyAndHoldTest, TwoStocksNoRebalance) {
    BuyAndHoldStrategy strategy;

    // Deux actions : A monte, B baisse, puis retour au depart
    std::vector<Price> prices_a = {
        make_price("2024-01-01", 100.0),
        make_price("2024-02-01", 150.0),
        make_price("2024-03-03", 100.0)
    };
    std::vector<Price> prices_b = {
        make_price("2024-01-01", 100.0),
        make_price("2024-02-01", 50.0),
        make_price("2024-03-03", 100.0)
    };

    MarketData market = make_dual_market("A", prices_a, "B", prices_b);
    Backtest bt(market, {"A", "B"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Jour 0 : 50 actions A a 100 (5000) + 50 actions B a 100 (5000). Cash = 0.
    // Jour 2 : 50*100 + 50*100 = 10000
    EXPECT_DOUBLE_EQ(result.final_value, 10000.0);
    EXPECT_DOUBLE_EQ(result.total_return_pct, 0.0);
    EXPECT_EQ(result.total_rebalances, 0u);
}

TEST(BuyAndHoldTest, DailyValuesTracked) {
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-02", 110.0),
        make_price("2024-01-03", 120.0)
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // 100 actions achetees jour 0
    ASSERT_EQ(result.daily_portfolio_values.size(), 3);
    EXPECT_DOUBLE_EQ(result.daily_portfolio_values[0], 10000.0);
    EXPECT_DOUBLE_EQ(result.daily_portfolio_values[1], 11000.0);
    EXPECT_DOUBLE_EQ(result.daily_portfolio_values[2], 12000.0);
}

// =============================================================================
// EqualWeightStrategy
// =============================================================================

TEST(EqualWeightTest, TwoStocksRebalanceBenefit) {
    // Scenario mean-reversion : A monte puis redescend, B baisse puis remonte.
    // Le reequilibrage vend le gagnant et achete le perdant → surperformance.
    EqualWeightStrategy strategy(30u);

    std::vector<Price> prices_a = {
        make_price("2024-01-01", 100.0),
        make_price("2024-02-01", 150.0),   // +50%
        make_price("2024-03-03", 100.0)    // retour
    };
    std::vector<Price> prices_b = {
        make_price("2024-01-01", 100.0),
        make_price("2024-02-01", 50.0),    // -50%
        make_price("2024-03-03", 100.0)    // retour
    };

    MarketData market = make_dual_market("A", prices_a, "B", prices_b);
    Backtest bt(market, {"A", "B"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Jour 0 : 50 A (5000) + 50 B (5000). Cash = 0.
    // Jour 1 (31 jours, >= 30) : reequilibrage.
    //   Vente : 50*150 + 50*50 = 10000 cash.
    //   Achat : 5000/150 = 33 A (4950) + 5000/50 = 100 B (5000). Cash = 50.
    // Jour 2 (31 jours, >= 30) : reequilibrage.
    //   Vente : 33*100 + 100*100 = 13300 + 50 cash = 13350.
    //   Achat : 6675/100 = 66 A (6600) + 6675/100 = 66 B (6600). Cash = 150.
    //   Valeur : 66*100 + 66*100 + 150 = 13350.
    EXPECT_DOUBLE_EQ(result.final_value, 13350.0);
    EXPECT_DOUBLE_EQ(result.total_return_pct, 33.5);
    EXPECT_EQ(result.total_rebalances, 2u);
}

TEST(EqualWeightTest, NoRebalanceBeforePeriod) {
    // Periode de 90 jours : aucun reequilibrage ne se declenche si les dates
    // sont espacees de moins de 90 jours.
    EqualWeightStrategy strategy(90u);

    std::vector<Price> prices_a = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-15", 150.0),   // 14 jours
        make_price("2024-01-30", 120.0)    // 29 jours depuis debut
    };
    std::vector<Price> prices_b = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-15", 80.0),
        make_price("2024-01-30", 90.0)
    };

    MarketData market = make_dual_market("A", prices_a, "B", prices_b);
    Backtest bt(market, {"A", "B"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Aucun reequilibrage : positions restent a 50 A + 50 B
    // Valeur jour 2 : 50*120 + 50*90 = 6000 + 4500 = 10500
    EXPECT_DOUBLE_EQ(result.final_value, 10500.0);
    EXPECT_EQ(result.total_rebalances, 0u);
}

// =============================================================================
// Tests generaux du moteur
// =============================================================================

TEST(BacktestTest, EmptyPricesReturnsInitialCash) {
    BuyAndHoldStrategy strategy;

    MarketData market = make_single_market("AAPL", {});
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_DOUBLE_EQ(result.final_value, 10000.0);
    EXPECT_TRUE(result.daily_portfolio_values.empty());
    EXPECT_EQ(result.total_rebalances, 0u);
}

TEST(BacktestTest, InsufficientCashNoInitialBuy) {
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-02", 110.0)
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(50.0);  // Pas assez pour acheter 1 action a 100

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_DOUBLE_EQ(result.final_value, 50.0);
}

TEST(BacktestTest, MaxDrawdownCalculation) {
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),   // achat 100 actions
        make_price("2024-01-02", 200.0),   // valeur = 20000 (peak)
        make_price("2024-01-03", 150.0),   // valeur = 15000, drawdown = 25%
        make_price("2024-01-04", 180.0)    // valeur = 18000
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_DOUBLE_EQ(result.max_drawdown_pct, 25.0);
    EXPECT_DOUBLE_EQ(result.final_value, 18000.0);
}

TEST(BacktestTest, AnnualizedReturnOverOneYear) {
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),
        make_price("2024-12-31", 110.0)
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // 100 actions a 100, valeur jour 1 = 11000 → rendement = 10%
    // 365 jours → rendement annualise ≈ 10%
    EXPECT_DOUBLE_EQ(result.final_value, 11000.0);
    EXPECT_NEAR(result.annualized_return_pct, 10.0, 0.5);
}

// =============================================================================
// Symboles en attente (pending)
// =============================================================================

TEST(PendingSymbolTest, BuyAndHoldRebalancesOnNewSymbol) {
    // B n'a pas de donnees au premier jour : il est mis en attente.
    // Quand il apparait au jour 1, BuyAndHold declenche un reequilibrage immediat.
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices_a = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-02", 100.0),
        make_price("2024-01-03", 100.0)
    };
    std::vector<Price> prices_b = {
        // Pas de prix au 2024-01-01 : B demarre en attente.
        make_price("2024-01-02", 100.0),
        make_price("2024-01-03", 100.0)
    };

    MarketData market = make_dual_market("A", prices_a, "B", prices_b);
    Backtest bt(market, {"A", "B"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Jour 0 : seulement A disponible, achat 100 A a 100. Cash = 0.
    // Jour 1 : B apparait → B&H reequilibre.
    //   Vente 100 A a 100 = 10000. Achat 50 A + 50 B a 100 = 10000. Cash = 0.
    // Final (jour 2) : 50*100 + 50*100 = 10000. Aucun reequilibrage supplementaire.
    EXPECT_DOUBLE_EQ(result.final_value, 10000.0);
    EXPECT_EQ(result.total_rebalances, 1u);
}

TEST(PendingSymbolTest, EqualWeightIgnoresNewSymbolTrigger) {
    // B n'a pas de donnees au premier jour : il est mis en attente.
    // EqualWeight ne reequilibre pas a l'apparition d'un nouveau symbole.
    // La periode de 90 jours n'est jamais atteinte → aucun reequilibrage,
    // B n'est jamais achete.
    EqualWeightStrategy strategy(90u);

    std::vector<Price> prices_a = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-16", 100.0),
        make_price("2024-01-31", 100.0)
    };
    std::vector<Price> prices_b = {
        // Pas de prix au 2024-01-01 : B demarre en attente.
        make_price("2024-01-16", 100.0),
        make_price("2024-01-31", 100.0)
    };

    MarketData market = make_dual_market("A", prices_a, "B", prices_b);
    Backtest bt(market, {"A", "B"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Jour 0 : 100 A a 100. Cash = 0.
    // Pas de boucle (period=90 > duree sim=30 jours).
    // B resolu au snapshot final mais n'est jamais achete (aucun reequilibrage).
    // Valeur finale : 100 * 100 = 10000.
    EXPECT_DOUBLE_EQ(result.final_value, 10000.0);
    EXPECT_EQ(result.total_rebalances, 0u);
}

// =============================================================================
// Prix manquants (trous dans les donnees)
// =============================================================================

TEST(BacktestTest, PriceGap_LastKnownPriceUsed) {
    // Un symbole sans cotation a une date intermediaire.
    // Le moteur doit utiliser le dernier prix connu (find_price_at_or_before).
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),
        // Pas de prix au 2024-01-02.
        make_price("2024-01-03", 120.0)
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Jour 0 : 100 actions a 100. Cash = 0. Valeur = 10000.
    // Jour 1 (2024-01-02) : pas de prix → dernier connu = 100. Valeur = 10000.
    // Final (2024-01-03) : prix = 120. Valeur = 100 * 120 = 12000.
    ASSERT_EQ(result.daily_portfolio_values.size(), 3u);
    EXPECT_DOUBLE_EQ(result.daily_portfolio_values[1], 10000.0);
    EXPECT_DOUBLE_EQ(result.final_value, 12000.0);
}

// =============================================================================
// Trois symboles
// =============================================================================

TEST(BacktestTest, ThreeSymbols_EqualDistributionWithResidualCash) {
    // Avec 10000 repartis sur 3 symboles a 100 : 33 actions chacun = 9900.
    // Le cash residuel (100) reste en portefeuille et est compte dans la valeur.
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-02", 100.0)
    };

    MarketData market = make_triple_market("A", prices, "B", prices, "C", prices);
    Backtest bt(market, {"A", "B", "C"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // 10000 / 3 = 3333.33 par symbole. floor(3333.33 / 100) = 33 actions chacun.
    // Cout total : 33 * 100 * 3 = 9900. Cash residuel = 100.
    // Valeur = 9900 + 100 = 10000.
    EXPECT_DOUBLE_EQ(result.final_value, 10000.0);
    EXPECT_DOUBLE_EQ(result.total_return_pct, 0.0);
    EXPECT_EQ(result.total_rebalances, 0u);
}

TEST(BacktestTest, ThreeSymbols_RebalanceWithResidualCash) {
    // Reequilibrage sur 3 symboles avec prix divergents, puis A continue de monter.
    // Le cash residuel des arrondis (100) est conserve a travers le reequilibrage.
    EqualWeightStrategy strategy(30u);

    std::vector<Price> prices_a = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-31", 150.0),
        make_price("2024-02-01", 200.0)
    };
    std::vector<Price> prices_b = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-31", 100.0),
        make_price("2024-02-01", 100.0)
    };
    std::vector<Price> prices_c = {
        make_price("2024-01-01", 100.0),
        make_price("2024-01-31",  50.0),
        make_price("2024-02-01",  50.0)
    };

    MarketData market = make_triple_market("A", prices_a, "B", prices_b, "C", prices_c);
    Backtest bt(market, {"A", "B", "C"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Jour 0 : 33 A (3300) + 33 B (3300) + 33 C (3300) + 100 cash = 10000.
    // Jour 30 (2024-01-31) : reequilibrage.
    //   Vente : 33*150 + 33*100 + 33*50 + 100 cash = 10000.
    //   Achat : floor(3333.33/150)=22 A + floor(3333.33/100)=33 B + floor(3333.33/50)=66 C.
    //   Cout : 3300+3300+3300 = 9900. Cash residuel = 100. Rebalances = 1.
    // Final (2024-02-01) : 22*200 + 33*100 + 66*50 + 100 = 4400+3300+3300+100 = 11100.
    EXPECT_EQ(result.total_rebalances, 1u);
    EXPECT_DOUBLE_EQ(result.final_value, 11100.0);
}

// =============================================================================
// Cas limites
// =============================================================================

TEST(BacktestTest, SingleDate_LoopNeverRuns_ValuePreserved) {
    // Un seul prix par symbole : start_date = end_date.
    // La boucle de simulation ne s'execute pas.
    // La valeur finale est identique a la valeur initiale.
    BuyAndHoldStrategy strategy;

    std::vector<Price> prices = {
        make_price("2024-01-01", 100.0)
    };

    MarketData market = make_single_market("AAPL", prices);
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(10000.0);

    auto result = bt.execute_backtest(portfolio, strategy);

    // start = end = 2024-01-01. Achat 100 actions a 100.
    // Boucle : 2024-01-02 < 2024-01-01 → faux, aucune iteration.
    // Snapshot final a 2024-01-01 : valeur = 100 * 100 = 10000.
    // daily_portfolio_values = [valeur_initiale, snapshot_final] → taille 2.
    EXPECT_EQ(result.total_rebalances, 0u);
    EXPECT_DOUBLE_EQ(result.final_value, 10000.0);
    ASSERT_EQ(result.daily_portfolio_values.size(), 2u);
}
