#include <iostream>
#include <iomanip>
#include <sstream>
#include "database.hpp"
#include "date.hpp"
#include "user_instruction.hpp"
#include "market_data.hpp"
#include "portfolio.hpp"
#include "backtest.hpp"
#include "equal_weight_strategy.hpp"
#include "buy_and_hold_strategy.hpp"

static void print_result(const std::string& label, const BacktestResult& result)
{
    std::cout << "\n--- " << label << " ---\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  Valeur finale       : " << result.final_value << " $\n";
    std::cout << "  Rendement total     : " << result.total_return_pct << " %\n";
    std::cout << "  Rendement annualise : " << result.annualized_return_pct << " %\n";
    std::cout << "  Drawdown maximum    : " << result.max_drawdown_pct << " %\n";
    std::cout << "  Reequilibrages      : " << result.total_rebalances << "\n";
}

static UserInstruction read_user_instruction()
{
    UserInstruction instruction{};

    std::cout << "Symboles (separes par des espaces, ex: AAPL MSFT GOOGL) : ";
    std::string symbols_line;
    std::getline(std::cin, symbols_line);
    std::istringstream iss(symbols_line);
    std::string symbol;
    while (iss >> symbol) instruction.symbols.push_back(symbol);

    std::cout << "Capital initial ($) : ";
    std::cin >> instruction.initial_cash;

    std::string start_str, end_str;
    std::cout << "Date la plus lointaine est 1970-01-02" << std::endl;
    std::cout << "Date de debut (YYYY-MM-DD) : ";
    std::cin >> start_str;
    auto start = parse_date(start_str);
    if (!start) throw std::invalid_argument("Date de debut invalide : " + start_str);
    instruction.start_date = *start;

    std::cout << "Date de fin (YYYY-MM-DD) : ";
    std::cin >> end_str;
    auto end = parse_date(end_str);
    if (!end) throw std::invalid_argument("Date de fin invalide : " + end_str);
    instruction.end_date = *end;

    std::cout << "Periode de reequilibrage Gave (jours, ex: 30 90 365) : ";
    std::cin >> instruction.rebalance_period_days;

    return instruction;
}

int main()
{
    try {
        UserInstruction instruction = read_user_instruction();

        Database db;
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

        std::cout << "\nSymboles : ";
        for (size_t i = 0; i < instruction.symbols.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << instruction.symbols[i];
        }
        std::cout << "\n";
        std::cout << "Periode  : " << format_date(instruction.start_date)
                  << " -> " << format_date(instruction.end_date) << "\n";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Capital  : " << instruction.initial_cash << " $\n";
        std::cout << "Reequilibrage Gave : tous les "
                  << instruction.rebalance_period_days << " jours\n";

        print_result("Gave - Equiponderation", gave_result);
        print_result("Buy & Hold", bah_result);

        double surperf = gave_result.total_return_pct - bah_result.total_return_pct;
        std::cout << "\nSurperformance Gave vs B&H : "
                  << std::showpos << surperf << std::noshowpos << " points\n";

    } catch (const std::exception& e) {
        std::cerr << "Erreur : " << e.what() << "\n";
        return 1;
    }

    return 0;
}
