#include <gtest/gtest.h>
#include "database.hpp"
#include "date.hpp"

// =============================================================================
// Tests d'integration - necessitent PostgreSQL lance (docker-compose up)
// =============================================================================

class DatabaseTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        db = std::make_unique<Database>("../.env");
    }

    static void TearDownTestSuite() {
        db.reset();
    }

    static std::unique_ptr<Database> db;
};

std::unique_ptr<Database> DatabaseTest::db = nullptr;

// =============================================================================
// get_symbols
// =============================================================================

TEST_F(DatabaseTest, GetSymbolsReturnsNonEmpty) {
    auto symbols = db->get_symbols();
    EXPECT_GT(symbols.size(), 0);
}

TEST_F(DatabaseTest, GetSymbolsContainsAAPL) {
    auto symbols = db->get_symbols();
    auto it = std::find(symbols.begin(), symbols.end(), "AAPL");
    EXPECT_NE(it, symbols.end());
}

TEST_F(DatabaseTest, GetSymbolsSortedAlphabetically) {
    auto symbols = db->get_symbols();
    EXPECT_TRUE(std::is_sorted(symbols.begin(), symbols.end()));
}

// =============================================================================
// get_prices
// =============================================================================

TEST_F(DatabaseTest, GetPricesReturnsData) {
    auto prices = db->get_prices("AAPL", "2024-01-01", "2024-01-31");
    EXPECT_GT(prices.size(), 0);
}

TEST_F(DatabaseTest, GetPricesSortedByDate) {
    auto prices = db->get_prices("AAPL", "2024-01-01", "2024-12-31");
    for (size_t i = 1; i < prices.size(); ++i) {
        EXPECT_LE(prices[i - 1].date, prices[i].date);
    }
}

TEST_F(DatabaseTest, GetPricesPositiveValues) {
    auto prices = db->get_prices("AAPL", "2024-01-01", "2024-01-31");
    for (const auto& p : prices) {
        EXPECT_GT(p.open, 0.0);
        EXPECT_GT(p.high, 0.0);
        EXPECT_GT(p.low, 0.0);
        EXPECT_GT(p.close, 0.0);
        EXPECT_GE(p.volume, 0);
    }
}

TEST_F(DatabaseTest, GetPricesDateFilter) {
    auto prices = db->get_prices("AAPL", "2024-06-01", "2024-06-30");
    Date start = *parse_date("2024-06-01");
    Date end   = *parse_date("2024-06-30");
    for (const auto& p : prices) {
        EXPECT_GE(p.date, start);
        EXPECT_LE(p.date, end);
    }
}

TEST_F(DatabaseTest, GetPricesUnknownSymbolReturnsEmpty) {
    auto prices = db->get_prices("ZZZZZZZ_FAKE", "2024-01-01", "2024-12-31");
    EXPECT_TRUE(prices.empty());
}

TEST_F(DatabaseTest, GetPricesNoDatesReturnsAll) {
    auto all_prices = db->get_prices("AAPL");
    auto jan_prices = db->get_prices("AAPL", "2024-01-01", "2024-01-31");
    EXPECT_GT(all_prices.size(), jan_prices.size());
}
