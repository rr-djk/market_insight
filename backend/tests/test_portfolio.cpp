#include <gtest/gtest.h>
#include "portfolio.hpp"

// =============================================================================
// buy_stock
// =============================================================================

TEST(PortfolioTest, BuyStockSuccess) {
    Portfolio p(10000.0);

    bool result = p.buy_stock("AAPL", 10, 150.0, "2024-01-15");

    EXPECT_TRUE(result);
    EXPECT_DOUBLE_EQ(p.get_available_cash(), 8500.0);
    EXPECT_EQ(p.get_stock_positions().at("AAPL"), 10);
    EXPECT_EQ(p.get_transaction_history().size(), 1);
}

TEST(PortfolioTest, BuyStockInsufficientCash) {
    Portfolio p(1000.0);

    bool result = p.buy_stock("AAPL", 10, 150.0, "2024-01-15");

    EXPECT_FALSE(result);
    EXPECT_DOUBLE_EQ(p.get_available_cash(), 1000.0);
    EXPECT_TRUE(p.get_stock_positions().empty());
    EXPECT_TRUE(p.get_transaction_history().empty());
}

TEST(PortfolioTest, BuyStockExactCash) {
    Portfolio p(1500.0);

    bool result = p.buy_stock("AAPL", 10, 150.0, "2024-01-15");

    EXPECT_TRUE(result);
    EXPECT_DOUBLE_EQ(p.get_available_cash(), 0.0);
    EXPECT_EQ(p.get_stock_positions().at("AAPL"), 10);
}

TEST(PortfolioTest, BuyStockMultipleSymbols) {
    Portfolio p(10000.0);

    p.buy_stock("AAPL", 5, 150.0, "2024-01-15");
    p.buy_stock("MSFT", 10, 400.0, "2024-01-15");

    EXPECT_DOUBLE_EQ(p.get_available_cash(), 10000.0 - 750.0 - 4000.0);
    EXPECT_EQ(p.get_stock_positions().size(), 2);
    EXPECT_EQ(p.get_stock_positions().at("AAPL"), 5);
    EXPECT_EQ(p.get_stock_positions().at("MSFT"), 10);
    EXPECT_EQ(p.get_transaction_history().size(), 2);
}

TEST(PortfolioTest, BuyStockSameSymbolTwice) {
    Portfolio p(10000.0);

    p.buy_stock("AAPL", 5, 150.0, "2024-01-15");
    p.buy_stock("AAPL", 3, 155.0, "2024-01-16");

    EXPECT_EQ(p.get_stock_positions().at("AAPL"), 8);
    EXPECT_EQ(p.get_transaction_history().size(), 2);
}

// =============================================================================
// sell_stock
// =============================================================================

TEST(PortfolioTest, SellStockSuccess) {
    Portfolio p(10000.0);
    p.buy_stock("AAPL", 10, 150.0, "2024-01-15");

    bool result = p.sell_stock("AAPL", 5, 160.0, "2024-02-01");

    EXPECT_TRUE(result);
    EXPECT_DOUBLE_EQ(p.get_available_cash(), 8500.0 + 800.0);
    EXPECT_EQ(p.get_stock_positions().at("AAPL"), 5);
    EXPECT_EQ(p.get_transaction_history().size(), 2);
}

TEST(PortfolioTest, SellStockAllShares) {
    Portfolio p(10000.0);
    p.buy_stock("AAPL", 10, 150.0, "2024-01-15");

    bool result = p.sell_stock("AAPL", 10, 160.0, "2024-02-01");

    EXPECT_TRUE(result);
    EXPECT_DOUBLE_EQ(p.get_available_cash(), 8500.0 + 1600.0);
    // Le symbole doit etre retire de la map quand quantite = 0
    EXPECT_EQ(p.get_stock_positions().count("AAPL"), 0);
}

TEST(PortfolioTest, SellStockNoPosition) {
    Portfolio p(10000.0);

    bool result = p.sell_stock("AAPL", 5, 160.0, "2024-02-01");

    EXPECT_FALSE(result);
    EXPECT_DOUBLE_EQ(p.get_available_cash(), 10000.0);
    EXPECT_TRUE(p.get_transaction_history().empty());
}

TEST(PortfolioTest, SellStockInsufficientShares) {
    Portfolio p(10000.0);
    p.buy_stock("AAPL", 5, 150.0, "2024-01-15");

    bool result = p.sell_stock("AAPL", 10, 160.0, "2024-02-01");

    EXPECT_FALSE(result);
    // Position inchangee
    EXPECT_EQ(p.get_stock_positions().at("AAPL"), 5);
    // Seule la transaction d'achat
    EXPECT_EQ(p.get_transaction_history().size(), 1);
}

// =============================================================================
// compute_total_value
// =============================================================================

TEST(PortfolioTest, ComputeTotalValueCashOnly) {
    Portfolio p(10000.0);

    std::unordered_map<std::string, double> prices;
    EXPECT_DOUBLE_EQ(p.compute_total_value(prices), 10000.0);
}

TEST(PortfolioTest, ComputeTotalValueWithPositions) {
    Portfolio p(10000.0);
    p.buy_stock("AAPL", 10, 150.0, "2024-01-15");
    p.buy_stock("MSFT", 5, 400.0, "2024-01-15");

    // cash restant = 10000 - 1500 - 2000 = 6500
    std::unordered_map<std::string, double> current_prices = {
        {"AAPL", 160.0},
        {"MSFT", 420.0}
    };

    // 6500 + (10 * 160) + (5 * 420) = 6500 + 1600 + 2100 = 10200
    EXPECT_DOUBLE_EQ(p.compute_total_value(current_prices), 10200.0);
}

// =============================================================================
// transaction_history : verification du contenu
// =============================================================================

TEST(PortfolioTest, TransactionHistoryContent) {
    Portfolio p(10000.0);
    p.buy_stock("AAPL", 10, 150.0, "2024-01-15");
    p.sell_stock("AAPL", 5, 160.0, "2024-02-01");

    const auto& history = p.get_transaction_history();
    ASSERT_EQ(history.size(), 2);

    EXPECT_EQ(history[0].symbol, "AAPL");
    EXPECT_EQ(history[0].order_type, OrderType::BUY);
    EXPECT_EQ(history[0].quantity, 10);
    EXPECT_DOUBLE_EQ(history[0].unit_price, 150.0);
    EXPECT_EQ(history[0].date, "2024-01-15");

    EXPECT_EQ(history[1].symbol, "AAPL");
    EXPECT_EQ(history[1].order_type, OrderType::SELL);
    EXPECT_EQ(history[1].quantity, 5);
    EXPECT_DOUBLE_EQ(history[1].unit_price, 160.0);
    EXPECT_EQ(history[1].date, "2024-02-01");
}
