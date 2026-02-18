#ifndef USER_INSTRUCTION_HPP
#define USER_INSTRUCTION_HPP

#include <string>
#include <vector>

/**
 * Parametres fournis par l'utilisateur pour configurer un backtest.
 */
struct UserInstruction {
    std::vector<std::string> symbols;           //
    double                   initial_cash;      // Capital de depart
    std::string              start_date;        // Date de debut au format "YYYY-MM-DD".
    std::string              end_date;          // Date de fin au format "YYYY-MM-DD".
    int                      rebalance_period_days; // Periode de reequilibrage Gave (ex: 30 ou 90 jours).
};

#endif
