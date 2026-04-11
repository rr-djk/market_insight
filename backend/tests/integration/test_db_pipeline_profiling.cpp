// Benchmarks du pipeline get_prices_bulk() — metriques de reference AVANT optimisation.
// Lancer avec : make test-profile (depuis le repertoire backend/)
// Prerequis : docker-compose up (depuis la racine du projet)
//
// Ces tests ne verifient pas la correction des donnees : ils mesurent le temps
// d'execution de chaque phase du pipeline et affichent les metriques.
// Tous les tests se terminent par SUCCEED().

#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "database.hpp"
#include "price.hpp"

// =============================================================================
// Fixture commune
// =============================================================================

class DbPipelineProfilingTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        try {
            db = std::make_unique<Database>("../.env");
        } catch (const std::exception&) {
            db_unavailable = true;
        }
    }

    static void TearDownTestSuite() { db.reset(); }

    void SetUp() override {
        if (db_unavailable) {
            GTEST_SKIP() << "PostgreSQL indisponible, benchmarks ignores";
        }
    }

    /**
     * Executes @p fn five times after one warm-up and returns the median
     * duration in microseconds alongside the row count from the last run.
     */
    template <typename Fn>
    static std::pair<long long, std::size_t> measure_median_us(Fn&& fn) {
        std::size_t row_count = fn();  // warm-up

        std::vector<long long> samples;
        samples.reserve(5);
        for (int i = 0; i < 5; ++i) {
            auto t0 = std::chrono::high_resolution_clock::now();
            row_count = fn();
            auto t1 = std::chrono::high_resolution_clock::now();
            samples.push_back(
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
        }
        std::sort(samples.begin(), samples.end());
        return {samples[2], row_count};  // mediane de 5
    }

    static void print_row(const std::string& label,
                          long long          total_us,
                          std::size_t        rows) {
        double us_per_row = (rows > 0)
            ? static_cast<double>(total_us) / static_cast<double>(rows)
            : 0.0;
        std::cout << "  " << std::left  << std::setw(40) << label
                  << std::right << std::setw(10) << total_us << " us"
                  << std::setw(12) << rows << " lignes"
                  << std::fixed << std::setprecision(3)
                  << std::setw(12) << us_per_row << " us/ligne\n";
    }

    static std::unique_ptr<Database> db;
    static bool db_unavailable;

    static const std::vector<std::string> ALL_SYMBOLS;
};

std::unique_ptr<Database> DbPipelineProfilingTest::db;
bool DbPipelineProfilingTest::db_unavailable = false;

const std::vector<std::string> DbPipelineProfilingTest::ALL_SYMBOLS = {
    "INTC", "MSFT", "AAPL", "ADBE", "ORCL",
    "CSCO", "QCOM", "AMGN", "COST", "PAYX"
};

// =============================================================================
// Test 1 — Baseline pipeline complet
// Mesure du temps total de get_prices_bulk() pour differentes tailles de
// panier et fenetres temporelles. Mediane sur 5 runs apres 1 warm-up.
// =============================================================================

TEST_F(DbPipelineProfilingTest, Test1_Baseline_Pipeline_Complet) {
    std::cout << "\n=== Test 1 : Baseline pipeline complet (get_prices_bulk) ===\n";
    std::cout << "  " << std::left  << std::setw(40) << "Configuration"
              << std::right << std::setw(12) << "Mediane"
              << std::setw(13) << "Lignes"
              << std::setw(14) << "us/ligne" << "\n";
    std::cout << "  " << std::string(75, '-') << "\n";

    const std::vector<std::string> five_symbols = {
        "INTC", "MSFT", "AAPL", "ADBE", "ORCL"
    };

    struct Config {
        std::vector<std::string> symbols;
        std::string              start;
        std::string              end;
        std::string              label;
    };

    std::vector<Config> configs = {
        { {"AAPL"},      "2023-01-01", "2024-01-01", "1 symbole  / 1 an"    },
        { {"AAPL"},      "2019-01-01", "2024-01-01", "1 symbole  / 5 ans"   },
        { {"AAPL"},      "2014-01-01", "2024-01-01", "1 symbole  / 10 ans"  },
        { five_symbols,  "2023-01-01", "2024-01-01", "5 symboles / 1 an"    },
        { five_symbols,  "2019-01-01", "2024-01-01", "5 symboles / 5 ans"   },
        { five_symbols,  "2014-01-01", "2024-01-01", "5 symboles / 10 ans"  },
        { ALL_SYMBOLS,   "2023-01-01", "2024-01-01", "10 symboles / 1 an"   },
        { ALL_SYMBOLS,   "2019-01-01", "2024-01-01", "10 symboles / 5 ans"  },
        { ALL_SYMBOLS,   "2014-01-01", "2024-01-01", "10 symboles / 10 ans" },
    };

    for (const auto& cfg : configs) {
        auto [median_us, rows] = measure_median_us([&]() -> std::size_t {
            auto data = db->get_prices_bulk(cfg.symbols, cfg.start, cfg.end);
            std::size_t count = 0;
            for (const auto& [sym, prices] : data) {
                count += prices.size();
            }
            return count;
        });
        print_row(cfg.label, median_us, rows);
    }

    SUCCEED();
}

// =============================================================================
// Test 2 — Decomposition : deserialization pqxx vs construction map
// Mesure A : get_prices_bulk() complet (I/O + pqxx + map).
// Mesure B : iteration + copie des donnees deja en RAM (map sans requete SQL).
// Ratio A/B indique la fraction du temps imputable a I/O+pqxx vs C++ pur.
// =============================================================================

TEST_F(DbPipelineProfilingTest, Test2_Decomposition_IO_vs_Construction_Map) {
    std::cout << "\n=== Test 2 : Decomposition I/O+pqxx vs construction map"
                 " (5 ans, 10 symboles) ===\n";

    const std::string start = "2019-01-01";
    const std::string end   = "2024-01-01";

    // Mesure A : pipeline complet.
    auto [median_a_us, total_rows] = measure_median_us([&]() -> std::size_t {
        auto data = db->get_prices_bulk(ALL_SYMBOLS, start, end);
        std::size_t count = 0;
        for (const auto& [sym, prices] : data) {
            count += prices.size();
        }
        return count;
    });

    // Chargement unique des donnees en RAM pour la mesure B.
    auto reference_data = db->get_prices_bulk(ALL_SYMBOLS, start, end);

    // Mesure B : reconstruction d'une map identique depuis des donnees en RAM.
    // Simule le cout de construction sans appel SQL ni deserialisation pqxx.
    auto [median_b_us, rows_b] = measure_median_us([&]() -> std::size_t {
        std::map<std::string, std::vector<Price>> rebuilt;
        for (const auto& [sym, prices] : reference_data) {
            rebuilt[sym] = prices;
        }
        std::size_t count = 0;
        for (const auto& [sym, prices] : rebuilt) {
            count += prices.size();
        }
        return count;
    });

    double ratio = (median_b_us > 0)
        ? static_cast<double>(median_a_us) / static_cast<double>(median_b_us)
        : 0.0;
    double io_fraction_pct = (median_a_us > 0)
        ? 100.0 * static_cast<double>(median_a_us - median_b_us)
              / static_cast<double>(median_a_us)
        : 0.0;

    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  Mesure A (pipeline complet)     : " << median_a_us << " us"
              << " (" << total_rows << " lignes)\n";
    std::cout << "  Mesure B (copie RAM, map seule) : " << median_b_us << " us"
              << " (" << rows_b << " lignes)\n";
    std::cout << "  Ratio A/B                       : "
              << std::setprecision(1) << ratio << "x\n";
    std::cout << "  Fraction I/O+pqxx estimee       : "
              << std::setprecision(1) << io_fraction_pct << "%\n";

    SUCCEED();
}

// =============================================================================
// Test 3 — map vs unordered_map (cout de la structure d'accumulation)
// Compare la vitesse de construction a partir des memes donnees en RAM.
// Goulot #2 documente : ~15% CPU en comparaisons de strings dans std::map.
// =============================================================================

TEST_F(DbPipelineProfilingTest, Test3_Map_vs_UnorderedMap) {
    std::cout << "\n=== Test 3 : map vs unordered_map"
                 " (construction depuis donnees RAM, 10 symboles, 5 ans) ===\n";

    const std::string start = "2019-01-01";
    const std::string end   = "2024-01-01";

    // Dataset plat (paires {symbole, Price}) simulant une iteration ligne par
    // ligne comme le fait get_prices_bulk() sur le pqxx::result.
    auto reference_data = db->get_prices_bulk(ALL_SYMBOLS, start, end);
    std::vector<std::pair<std::string, Price>> flat_rows;
    for (const auto& [sym, prices] : reference_data) {
        for (const auto& p : prices) {
            flat_rows.emplace_back(sym, p);
        }
    }
    std::size_t total_rows = flat_rows.size();

    // Mesure map.
    auto [map_us, rows_map] = measure_median_us([&]() -> std::size_t {
        std::map<std::string, std::vector<Price>> m;
        for (const auto& [sym, p] : flat_rows) {
            m[sym].push_back(p);
        }
        std::size_t count = 0;
        for (const auto& [sym, prices] : m) {
            count += prices.size();
        }
        return count;
    });

    // Mesure unordered_map.
    auto [umap_us, rows_umap] = measure_median_us([&]() -> std::size_t {
        std::unordered_map<std::string, std::vector<Price>> m;
        for (const auto& [sym, p] : flat_rows) {
            m[sym].push_back(p);
        }
        std::size_t count = 0;
        for (const auto& [sym, prices] : m) {
            count += prices.size();
        }
        return count;
    });

    double speedup = (umap_us > 0)
        ? static_cast<double>(map_us) / static_cast<double>(umap_us)
        : 0.0;

    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  Lignes dans le dataset plat     : " << total_rows << "\n";
    std::cout << "  std::map construction           : " << map_us  << " us"
              << " (" << rows_map  << " lignes verifiees)\n";
    std::cout << "  std::unordered_map construction : " << umap_us << " us"
              << " (" << rows_umap << " lignes verifiees)\n";
    std::cout << "  Speedup unordered_map vs map    : "
              << std::setprecision(2) << speedup << "x\n";

    SUCCEED();
}

// =============================================================================
// Test 4 — OHLCV vs Close-only (volume de transfert reseau)
// Compare get_prices_bulk() (7 colonnes) vs get_close_prices_bulk() (3 colonnes).
// Le backtest utilise OHLCV mais n'accede qu'a .close — economie potentielle ~57%.
// =============================================================================

TEST_F(DbPipelineProfilingTest, Test4_OHLCV_vs_CloseOnly) {
    std::cout << "\n=== Test 4 : OHLCV vs Close-only (10 symboles, 5 ans) ===\n";

    const std::string start = "2019-01-01";
    const std::string end   = "2024-01-01";

    // Mesure OHLCV (7 colonnes : symbol, date, open, high, low, close, volume).
    auto [ohlcv_us, rows_ohlcv] = measure_median_us([&]() -> std::size_t {
        auto data = db->get_prices_bulk(ALL_SYMBOLS, start, end);
        std::size_t count = 0;
        for (const auto& [sym, prices] : data) {
            count += prices.size();
        }
        return count;
    });

    // Mesure Close-only (3 colonnes : symbol, date, close).
    auto [close_us, rows_close] = measure_median_us([&]() -> std::size_t {
        auto data = db->get_close_prices_bulk(ALL_SYMBOLS, start, end);
        std::size_t count = 0;
        for (const auto& [sym, prices] : data) {
            count += prices.size();
        }
        return count;
    });

    double speedup = (close_us > 0)
        ? static_cast<double>(ohlcv_us) / static_cast<double>(close_us)
        : 0.0;
    double saving_pct = (ohlcv_us > 0)
        ? 100.0 * static_cast<double>(ohlcv_us - close_us)
              / static_cast<double>(ohlcv_us)
        : 0.0;

    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  get_prices_bulk (OHLCV)         : " << ohlcv_us << " us"
              << " (" << rows_ohlcv << " lignes)\n";
    std::cout << "  get_close_prices_bulk (close)   : " << close_us << " us"
              << " (" << rows_close << " lignes)\n";
    std::cout << "  Speedup close vs OHLCV          : "
              << std::setprecision(2) << speedup << "x\n";
    std::cout << "  Economie de temps               : "
              << std::setprecision(1) << saving_pct << "%\n";

    SUCCEED();
}

// =============================================================================
// Test 5 — Scalabilite : N symboles (fenetre fixe 5 ans)
// Verifie si la croissance est lineaire (bottleneck = transfert de donnees)
// ou sous-lineaire (benefice de l'overhead fixe reseau absorbe par le volume).
// =============================================================================

TEST_F(DbPipelineProfilingTest, Test5_Scalabilite_N_Symboles) {
    std::cout << "\n=== Test 5 : Scalabilite N symboles (fenetre 5 ans fixe) ===\n";
    std::cout << "  " << std::left  << std::setw(14) << "N symboles"
              << std::right << std::setw(12) << "Mediane"
              << std::setw(13) << "Lignes"
              << std::setw(14) << "us/ligne"
              << std::setw(18) << "us/symbole" << "\n";
    std::cout << "  " << std::string(70, '-') << "\n";

    const std::string start = "2019-01-01";
    const std::string end   = "2024-01-01";

    const std::vector<std::size_t> sizes = {1, 2, 3, 5, 10};

    long long prev_us = 0;
    for (std::size_t n : sizes) {
        std::vector<std::string> subset(
            ALL_SYMBOLS.begin(),
            ALL_SYMBOLS.begin() + static_cast<std::ptrdiff_t>(n));

        auto [median_us, rows] = measure_median_us([&]() -> std::size_t {
            auto data = db->get_prices_bulk(subset, start, end);
            std::size_t count = 0;
            for (const auto& [sym, prices] : data) {
                count += prices.size();
            }
            return count;
        });

        double us_per_row = (rows > 0)
            ? static_cast<double>(median_us) / static_cast<double>(rows)
            : 0.0;
        long long us_per_sym = static_cast<long long>(
            static_cast<double>(median_us) / static_cast<double>(n));

        std::cout << "  " << std::left  << std::setw(14) << n
                  << std::right << std::setw(10) << median_us << " us"
                  << std::setw(12) << rows << " lignes"
                  << std::fixed << std::setprecision(3)
                  << std::setw(12) << us_per_row << " us/ligne"
                  << std::setw(14) << us_per_sym << " us/symbole";

        if (prev_us > 0) {
            double ratio = static_cast<double>(median_us) / static_cast<double>(prev_us);
            std::cout << "  (ratio N-1: " << std::setprecision(2) << ratio << "x)";
        }
        std::cout << "\n";
        prev_us = median_us;
    }

    SUCCEED();
}
