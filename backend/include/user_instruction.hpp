#ifndef USER_INSTRUCTION_HPP
#define USER_INSTRUCTION_HPP

#include <string>
#include <vector>
#include "date.hpp"

/**
 * Parametres fournis par l'utilisateur pour configurer un backtest.
 */
struct UserInstruction {
    std::vector<std::string> symbols;           //
    double                   initial_cash;      // Capital de depart
    Date                     start_date;        // Date de debut du backtest.
    Date                     end_date;          // Date de fin du backtest.
    unsigned int             rebalance_period_days; // Periode de reequilibrage Gave (ex: 30 ou 90 jours).
};

#endif
