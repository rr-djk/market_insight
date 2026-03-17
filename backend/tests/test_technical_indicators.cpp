#include <gtest/gtest.h>
#include <cmath>
#include "technical_indicators.hpp"

using std::vector;

// =============================================================================
// Helpers
// =============================================================================

static Price make_price(double close)
{
    return {*parse_date("2024-01-01"), close, close, close, close, 1000};
}

static vector<Price> make_prices(const vector<double>& closes)
{
    vector<Price> prices;
    for (double c : closes)
        prices.push_back(make_price(c));
    return prices;
}

// =============================================================================
// SMA
// =============================================================================

TEST(SmaTest, CorrectValues)
{
    // SMA(3) sur [100, 102, 101, 105, 103]
    // index 2 : (100+102+101)/3 = 101.0
    // index 3 : (102+101+105)/3 = 102.666...
    // index 4 : (101+105+103)/3 = 103.0
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0, 103.0});
    auto result = compute_sma(prices, 3);

    ASSERT_EQ(result.size(), 5u);
    EXPECT_NEAR(result[2], 101.0,       1e-9);
    EXPECT_NEAR(result[3], 102.666667,  1e-5);
    EXPECT_NEAR(result[4], 103.0,       1e-9);
}

TEST(SmaTest, WarmupIsNaN)
{
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0});
    auto result = compute_sma(prices, 3);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_TRUE(std::isnan(result[1]));
    EXPECT_FALSE(std::isnan(result[2]));
}

TEST(SmaTest, PeriodLargerThanData)
{
    auto prices = make_prices({100.0, 102.0});
    auto result = compute_sma(prices, 5);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_TRUE(std::isnan(result[1]));
}

// =============================================================================
// EMA
// =============================================================================

TEST(EmaTest, FirstValueEqualsSma)
{
    // EMA(3) : premiere valeur valide = SMA des 3 premiers prix
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0});
    auto sma    = compute_sma(prices, 3);
    auto ema    = compute_ema(prices, 3);

    EXPECT_NEAR(ema[2], sma[2], 1e-9);
}

TEST(EmaTest, CorrectValues)
{
    // EMA(3) sur [100, 102, 101, 105, 103]
    // k = 2/(3+1) = 0.5
    // index 2 (amorçage SMA) : 101.0
    // index 3 : 105*0.5 + 101.0*0.5 = 103.0
    // index 4 : 103*0.5 + 103.0*0.5 = 103.0
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0, 103.0});
    auto result = compute_ema(prices, 3);

    EXPECT_NEAR(result[2], 101.0, 1e-9);
    EXPECT_NEAR(result[3], 103.0, 1e-9);
    EXPECT_NEAR(result[4], 103.0, 1e-9);
}

TEST(EmaTest, WarmupIsNaN)
{
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0});
    auto result = compute_ema(prices, 3);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_TRUE(std::isnan(result[1]));
    EXPECT_FALSE(std::isnan(result[2]));
}

// =============================================================================
// RSI
// =============================================================================

TEST(RsiTest, WarmupIsNaN)
{
    // RSI(3) : les 3 premiers elements doivent etre NaN
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0, 103.0});
    auto result = compute_rsi(prices, 3);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_TRUE(std::isnan(result[1]));
    EXPECT_TRUE(std::isnan(result[2]));
    EXPECT_FALSE(std::isnan(result[3]));
}

TEST(RsiTest, AlwaysInRange)
{
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0, 103.0, 107.0, 104.0});
    auto result = compute_rsi(prices, 3);

    for (size_t i = 3; i < result.size(); ++i) {
        EXPECT_GE(result[i], 0.0);
        EXPECT_LE(result[i], 100.0);
    }
}

TEST(RsiTest, AllGainsGivesHundred)
{
    // Que des hausses → avg_loss = 0 → RSI = 100
    auto prices = make_prices({100.0, 101.0, 102.0, 103.0, 104.0});
    auto result = compute_rsi(prices, 3);

    EXPECT_DOUBLE_EQ(result[3], 100.0);
    EXPECT_DOUBLE_EQ(result[4], 100.0);
}

// =============================================================================
// MACD
// =============================================================================

TEST(MacdTest, WarmupIsNaN)
{
    // MACD(3, 5) : les 4 premiers elements (slow-1) doivent etre NaN
    auto prices = make_prices({100.0, 101.0, 102.0, 103.0, 104.0, 105.0});
    auto result = compute_macd(prices, 3, 5);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_TRUE(std::isnan(result[1]));
    EXPECT_TRUE(std::isnan(result[2]));
    EXPECT_TRUE(std::isnan(result[3]));
    EXPECT_FALSE(std::isnan(result[4]));
}

TEST(MacdTest, FastGreaterOrEqualSlowReturnsAllNaN)
{
    auto prices = make_prices({100.0, 101.0, 102.0, 103.0, 104.0});
    auto result = compute_macd(prices, 5, 3);

    for (const auto& v : result)
        EXPECT_TRUE(std::isnan(v));
}

TEST(MacdTest, TrendingUpGivesPositiveMacd)
{
    // Prix strictement croissants : EMA rapide > EMA lente → MACD > 0
    auto prices = make_prices({100.0, 101.0, 102.0, 103.0, 104.0,
                                105.0, 106.0, 107.0, 108.0, 109.0});
    auto result = compute_macd(prices, 3, 5);

    for (size_t i = 4; i < result.size(); ++i)
        EXPECT_GT(result[i], 0.0);
}

// =============================================================================
// Bollinger
// =============================================================================

TEST(BollingerTest, WarmupIsNaN)
{
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0});
    auto upper  = compute_bollinger_upper(prices, 3);
    auto lower  = compute_bollinger_lower(prices, 3);

    EXPECT_TRUE(std::isnan(upper[0]));
    EXPECT_TRUE(std::isnan(upper[1]));
    EXPECT_FALSE(std::isnan(upper[2]));
    EXPECT_TRUE(std::isnan(lower[0]));
    EXPECT_TRUE(std::isnan(lower[1]));
    EXPECT_FALSE(std::isnan(lower[2]));
}

TEST(BollingerTest, UpperAlwaysGreaterThanLower)
{
    auto prices = make_prices({100.0, 102.0, 101.0, 105.0, 103.0, 98.0, 107.0});
    auto upper  = compute_bollinger_upper(prices, 3);
    auto lower  = compute_bollinger_lower(prices, 3);

    for (size_t i = 2; i < upper.size(); ++i)
        EXPECT_GT(upper[i], lower[i]);
}

TEST(BollingerTest, ConstantPricesGiveZeroBandWidth)
{
    // Prix constants → ecart-type = 0 → upper = lower = prix
    auto prices = make_prices({100.0, 100.0, 100.0, 100.0, 100.0});
    auto upper  = compute_bollinger_upper(prices, 3);
    auto lower  = compute_bollinger_lower(prices, 3);

    for (size_t i = 2; i < upper.size(); ++i) {
        EXPECT_NEAR(upper[i], 100.0, 1e-9);
        EXPECT_NEAR(lower[i], 100.0, 1e-9);
    }
}
