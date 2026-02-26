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
