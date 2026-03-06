// Tests d'integration : requierent PostgreSQL en cours d'execution.
// Lancer avec : make test-integration (depuis le repertoire backend/)
// Prerequis : docker-compose up (depuis la racine du projet)
//
// Les symboles utilises sont exclusivement ceux de data/test/test_symbols.txt :
// INTC, MSFT, AAPL, ADBE, ORCL, CSCO, QCOM, AMGN, COST, PAYX

#include <gtest/gtest.h>
#include "backtest.hpp"
#include "database.hpp"
#include "market_data.hpp"
#include "buy_and_hold_strategy.hpp"
#include "equal_weight_strategy.hpp"

// =============================================================================
// Fixture commune
// =============================================================================

class BacktestIntegrationTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        try {
            db = std::make_unique<Database>("../.env");
        } catch (const std::exception& e) {
            db_unavailable = true;
        }
    }

    static void TearDownTestSuite() {
        db.reset();
    }

    void SetUp() override {
        if (db_unavailable) {
            GTEST_SKIP() << "PostgreSQL indisponible, tests d'integration ignores";
        }
    }

    // Charge les donnees de marche depuis la base pour la periode donnee.
    static MarketData load(const std::vector<std::string>& symbols,
                           const std::string& start,
                           const std::string& end) {
        return MarketData(*db, symbols, start, end);
    }

    static std::unique_ptr<Database> db;
    static bool db_unavailable;

    static const std::vector<std::string> ALL_SYMBOLS;
    static const std::vector<std::string> PAIR;
};

std::unique_ptr<Database> BacktestIntegrationTest::db;
bool BacktestIntegrationTest::db_unavailable = false;

const std::vector<std::string> BacktestIntegrationTest::ALL_SYMBOLS = {
    "INTC", "MSFT", "AAPL", "ADBE", "ORCL",
    "CSCO", "QCOM", "AMGN", "COST", "PAYX"
};

const std::vector<std::string> BacktestIntegrationTest::PAIR = {"AAPL", "MSFT"};

// =============================================================================
// Scenario 1 : Buy & Hold, action unique, 5 ans
// Verifie que les metriques de base sont dans des plages coherentes.
// =============================================================================

TEST_F(BacktestIntegrationTest, BuyAndHold_SingleStock_FiveYears_PlausibleMetrics) {
    MarketData market = load({"AAPL"}, "2015-01-01", "2020-01-01");
    Backtest bt(market, {"AAPL"});
    Portfolio portfolio(100000.0);
    BuyAndHoldStrategy strategy;

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_GT(result.final_value, 0.0);
    EXPECT_GE(result.max_drawdown_pct, 0.0);
    EXPECT_LE(result.max_drawdown_pct, 100.0);
    EXPECT_GT(result.total_return_pct, -99.0);
    EXPECT_FALSE(result.daily_portfolio_values.empty());
    EXPECT_DOUBLE_EQ(result.final_value, result.daily_portfolio_values.back());

    // B&H ne reequilibre jamais (AAPL existait avant 2015, aucun symbole en attente).
    EXPECT_EQ(result.total_rebalances, 0u);
}

// =============================================================================
// Scenario 2 : Buy & Hold, deux actions etablies, aucun reequilibrage attendu
// =============================================================================

TEST_F(BacktestIntegrationTest, BuyAndHold_TwoStocks_FiveYears_ZeroRebalances) {
    MarketData market = load(PAIR, "2015-01-01", "2020-01-01");
    Backtest bt(market, PAIR);
    Portfolio portfolio(100000.0);
    BuyAndHoldStrategy strategy;

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_GT(result.final_value, 0.0);
    // AAPL et MSFT existent tous deux bien avant 2015 : aucun symbole en attente,
    // donc aucun reequilibrage.
    EXPECT_EQ(result.total_rebalances, 0u);
    EXPECT_FALSE(result.daily_portfolio_values.empty());
}

// =============================================================================
// Scenario 3 : EqualWeight mensuel, 2 actions, 5 ans
// Verifie que le nombre de reequilibrages correspond a la frequence mensuelle.
// =============================================================================

TEST_F(BacktestIntegrationTest, EqualWeight_Monthly_TwoStocks_FiveYears_RebalanceCount) {
    MarketData market = load(PAIR, "2015-01-01", "2020-01-01");
    Backtest bt(market, PAIR);
    Portfolio portfolio(100000.0);
    EqualWeightStrategy strategy(30u);  // Mensuel

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_GT(result.final_value, 0.0);
    EXPECT_FALSE(result.daily_portfolio_values.empty());

    // 5 ans = ~1826 jours calendaires. floor(1826 / 30) = 60 iterations.
    // Chaque iteration reequilibre (30 >= 30). Plage tolerante : 55-65.
    EXPECT_GE(result.total_rebalances, 55u);
    EXPECT_LE(result.total_rebalances, 65u);
}

// =============================================================================
// Scenario 4 : EqualWeight trimestriel, 10 actions, 10 ans
// Verifie le comptage sur un panier large et une longue periode.
// =============================================================================

TEST_F(BacktestIntegrationTest, EqualWeight_Quarterly_AllSymbols_TenYears_RebalanceCount) {
    MarketData market = load(ALL_SYMBOLS, "2010-01-01", "2020-01-01");
    Backtest bt(market, ALL_SYMBOLS);
    Portfolio portfolio(100000.0);
    EqualWeightStrategy strategy(91u);  // Trimestriel

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_GT(result.final_value, 0.0);

    // 10 ans = ~3652 jours. floor(3652 / 91) = 40 iterations.
    // Plage tolerante : 35-45.
    EXPECT_GE(result.total_rebalances, 35u);
    EXPECT_LE(result.total_rebalances, 45u);
}

// =============================================================================
// Scenario 5 : EqualWeight annuel, 10 actions, 5 ans
// Periode longue → peu de reequilibrages.
// =============================================================================

TEST_F(BacktestIntegrationTest, EqualWeight_Annual_AllSymbols_FiveYears_RebalanceCount) {
    MarketData market = load(ALL_SYMBOLS, "2015-01-01", "2020-01-01");
    Backtest bt(market, ALL_SYMBOLS);
    Portfolio portfolio(100000.0);
    EqualWeightStrategy strategy(365u);  // Annuel

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_GT(result.final_value, 0.0);

    // 5 ans = ~1826 jours. floor(1826 / 365) = 5 iterations.
    // Plage tolerante : 4-6.
    EXPECT_GE(result.total_rebalances, 4u);
    EXPECT_LE(result.total_rebalances, 6u);
}

// =============================================================================
// Scenario 6 : Periode superieure a la duree de simulation → 0 reequilibrages
// =============================================================================

TEST_F(BacktestIntegrationTest, EqualWeight_PeriodExceedsSimulation_ZeroRebalances) {
    // Simulation de 1 an, periode de reequilibrage de 1000 jours.
    // La boucle ne reequilibre jamais car 1000 > ~365 jours de simulation.
    MarketData market = load(PAIR, "2019-01-01", "2020-01-01");
    Backtest bt(market, PAIR);
    Portfolio portfolio(100000.0);
    EqualWeightStrategy strategy(1000u);

    auto result = bt.execute_backtest(portfolio, strategy);

    EXPECT_GT(result.final_value, 0.0);
    EXPECT_EQ(result.total_rebalances, 0u);
}

// =============================================================================
// Scenario 7 : Determinisme — deux executions identiques donnent le meme resultat
// =============================================================================

TEST_F(BacktestIntegrationTest, BuyAndHold_Deterministic_SameResultTwice) {
    MarketData market = load(PAIR, "2018-01-01", "2019-01-01");

    Backtest bt1(market, PAIR);
    Portfolio p1(50000.0);
    BuyAndHoldStrategy s1;
    auto result1 = bt1.execute_backtest(p1, s1);

    Backtest bt2(market, PAIR);
    Portfolio p2(50000.0);
    BuyAndHoldStrategy s2;
    auto result2 = bt2.execute_backtest(p2, s2);

    EXPECT_DOUBLE_EQ(result1.final_value, result2.final_value);
    EXPECT_DOUBLE_EQ(result1.total_return_pct, result2.total_return_pct);
    EXPECT_DOUBLE_EQ(result1.max_drawdown_pct, result2.max_drawdown_pct);
    EXPECT_EQ(result1.total_rebalances, result2.total_rebalances);
    ASSERT_EQ(result1.daily_portfolio_values.size(),
              result2.daily_portfolio_values.size());
}

// =============================================================================
// Scenario 8 : Validation exhaustive de toutes les metriques
// Un long backtest sur panier diversifie doit produire des valeurs coherentes.
// =============================================================================

TEST_F(BacktestIntegrationTest, AllMetrics_TenYears_AllSymbols_ValidRanges) {
    MarketData market = load(ALL_SYMBOLS, "2010-01-01", "2020-01-01");
    Backtest bt(market, ALL_SYMBOLS);
    Portfolio portfolio(100000.0);
    EqualWeightStrategy strategy(91u);

    auto result = bt.execute_backtest(portfolio, strategy);

    // Valeur finale > 0.
    EXPECT_GT(result.final_value, 0.0);

    // Rendement total dans une plage large (-99% a +2000%).
    EXPECT_GT(result.total_return_pct, -99.0);
    EXPECT_LT(result.total_return_pct, 2000.0);

    // Rendement annualise dans une plage raisonnable.
    EXPECT_GT(result.annualized_return_pct, -50.0);
    EXPECT_LT(result.annualized_return_pct, 200.0);

    // Drawdown non negatif, au plus 100%.
    EXPECT_GE(result.max_drawdown_pct, 0.0);
    EXPECT_LE(result.max_drawdown_pct, 100.0);

    // Valeurs journalieres coherentes avec la valeur finale.
    EXPECT_FALSE(result.daily_portfolio_values.empty());
    EXPECT_DOUBLE_EQ(result.final_value, result.daily_portfolio_values.back());

    // Toutes les valeurs journalieres sont positives.
    for (double v : result.daily_portfolio_values) {
        EXPECT_GT(v, 0.0);
    }
}

// =============================================================================
// Scenario 9 : EqualWeight vs Buy & Hold sur le meme panier
// EW produit des reequilibrages, B&H n'en produit aucun.
// Les deux strategies donnent des resultats differents.
// =============================================================================

TEST_F(BacktestIntegrationTest, EqualWeight_vs_BuyAndHold_DifferentRebalanceCounts) {
    MarketData market = load(PAIR, "2015-01-01", "2020-01-01");

    Backtest bt_ew(market, PAIR);
    Portfolio p_ew(100000.0);
    EqualWeightStrategy strat_ew(30u);
    auto result_ew = bt_ew.execute_backtest(p_ew, strat_ew);

    Backtest bt_bh(market, PAIR);
    Portfolio p_bh(100000.0);
    BuyAndHoldStrategy strat_bh;
    auto result_bh = bt_bh.execute_backtest(p_bh, strat_bh);

    // EW reequilibre periodiquement, B&H ne reequilibre jamais.
    EXPECT_GT(result_ew.total_rebalances, 0u);
    EXPECT_EQ(result_bh.total_rebalances, 0u);

    // Les deux strategies produisent des resultats valides.
    EXPECT_GT(result_ew.final_value, 0.0);
    EXPECT_GT(result_bh.final_value, 0.0);

    // Les resultats sont differents (strategies differentes sur meme panier).
    EXPECT_NE(result_ew.final_value, result_bh.final_value);
}
