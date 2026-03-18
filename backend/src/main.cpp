#include "backtest.hpp"
#include "buy_and_hold_strategy.hpp"
#include "database.hpp"
#include "date.hpp"
#include "equal_weight_strategy.hpp"
#include "market_data.hpp"
#include "portfolio.hpp"
#include "technical_indicators.hpp"
#include "user_instruction.hpp"
#include <chrono>
#include <iostream>
#include <print>
#include <sstream>

// =============================================================================
// Mode 1 — Backtest Gave vs Buy & Hold
// =============================================================================

static void print_result(const std::string& label, const BacktestResult& result)
{
    std::println("\n--- {} ---", label);
    std::println("  Valeur finale       : {:.2f} $", result.final_value);
    std::println("  Rendement total     : {:.2f} %", result.total_return_pct);
    std::println("  Rendement annualise : {:.2f} %", result.annualized_return_pct);
    std::println("  Drawdown maximum    : {:.2f} %", result.max_drawdown_pct);
    std::println("  Reequilibrages      : {}", result.total_rebalances);
}

static UserInstruction read_user_instruction()
{
    UserInstruction instruction{};

    std::print("Symboles (separes par des espaces, ex: AAPL MSFT GOOGL) : ");
    std::string symbols_line;
    std::getline(std::cin, symbols_line);
    std::istringstream iss(symbols_line);
    std::string symbol;
    while (iss >> symbol)
        instruction.symbols.push_back(symbol);

    std::print("Capital initial ($) : ");
    std::cin >> instruction.initial_cash;

    std::string start_str, end_str;
    std::println("Date la plus lointaine est 1970-01-02");
    std::print("Date de debut (YYYY-MM-DD) : ");
    std::cin >> start_str;
    auto start = parse_date(start_str);
    if (!start)
        throw std::invalid_argument("Date de debut invalide : " + start_str);
    instruction.start_date = *start;

    std::print("Date de fin (YYYY-MM-DD) : ");
    std::cin >> end_str;
    auto end = parse_date(end_str);
    if (!end)
        throw std::invalid_argument("Date de fin invalide : " + end_str);
    instruction.end_date = *end;

    std::print("Periode de reequilibrage Gave (jours, ex: 30 90 365) : ");
    std::cin >> instruction.rebalance_period_days;
    if (instruction.rebalance_period_days == 0)
        throw std::invalid_argument("La periode de reequilibrage doit etre d'au moins 1 jour.");

    return instruction;
}

static void run_backtest_mode(Database& db)
{
    UserInstruction instruction = read_user_instruction();

    MarketData market(db, instruction.symbols,
                      format_date(instruction.start_date),
                      format_date(instruction.end_date));

    Backtest backtest(market, instruction.symbols);

    EqualWeightStrategy gave_strategy(instruction.rebalance_period_days);
    Portfolio gave_portfolio(instruction.initial_cash);
    BacktestResult gave_result = backtest.execute_backtest(gave_portfolio, gave_strategy);

    BuyAndHoldStrategy bah_strategy;
    Portfolio bah_portfolio(instruction.initial_cash);
    BacktestResult bah_result = backtest.execute_backtest(bah_portfolio, bah_strategy);

    std::print("\nSymboles : ");
    for (size_t i = 0; i < instruction.symbols.size(); ++i) {
        if (i > 0) std::print(", ");
        std::print("{}", instruction.symbols[i]);
    }
    std::println("");
    std::println("Periode  : {} -> {}", format_date(instruction.start_date),
                                        format_date(instruction.end_date));
    std::println("Capital  : {:.2f} $", instruction.initial_cash);
    std::println("Reequilibrage Gave : tous les {} jours", instruction.rebalance_period_days);

    print_result("Gave - Equiponderation", gave_result);
    print_result("Buy & Hold", bah_result);

    double surperf = gave_result.total_return_pct - bah_result.total_return_pct;
    std::println("\nSurperformance Gave vs B&H : {:+.2f} points", surperf);
}

// =============================================================================
// Mode 2 — Indicateurs techniques (batch)
// =============================================================================

static void run_indicators_mode(Database& db)
{
    std::println("Chargement de tous les symboles...");
    MarketData market(db, db.get_symbols());

    const auto& symbols = db.get_symbols();
    std::println("{} symboles charges.", symbols.size());

    std::println("Calcul des indicateurs...");
    auto t_start = std::chrono::steady_clock::now();

    auto results = compute_all(market, symbols);

    auto t_end = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    std::println("Termine en {:.1f} ms ({:.2f} s)", elapsed_ms, elapsed_ms / 1000.0);
    std::println("Symboles traites : {}", results.size());
}

// =============================================================================
// main
// =============================================================================

int main()
{
    try {
        std::println("=== Market Insight ===");
        std::println("1. Backtest Gave vs Buy & Hold");
        std::println("2. Indicateurs techniques (batch, tous les symboles)");
        std::print("Choix : ");

        int choice = 0;
        std::cin >> choice;
        std::cin.ignore();

        Database db;

        if (choice == 1)
            run_backtest_mode(db);
        else if (choice == 2)
            run_indicators_mode(db);
        else
            std::println(stderr, "Choix invalide.");

    } catch (const std::exception& e) {
        std::println(stderr, "Erreur : {}", e.what());
        return 1;
    }

    return 0;
}
