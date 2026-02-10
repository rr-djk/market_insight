#ifndef PORTFOLIO_HPP
#define PORTFOLIO_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include "transaction.hpp"
#include "price.hpp"

class Portfolio {
public:
    /**
     * Initialise un portefeuille avec un capital de depart.
     * @param initial_cash Montant initial en liquidites.
     */
    explicit Portfolio(double initial_cash);

    /**
     * Achete des actions d'un symbole au prix unitaire donne.
     * @param symbol Ticker de l'action (ex: "AAPL").
     * @param quantity Nombre d'actions a acheter.
     * @param unit_price Prix unitaire de l'action au moment de l'achat.
     * @param date Date de la transaction au format "YYYY-MM-DD".
     * @return true si l'achat a ete execute, false si cash insuffisant.
     */
    bool buy_stock(const std::string& symbol, int quantity,
                   double unit_price, const std::string& date);

    /**
     * Vend des actions d'un symbole au prix unitaire donne.
     * @param symbol Ticker de l'action (ex: "AAPL").
     * @param quantity Nombre d'actions a vendre.
     * @param unit_price Prix unitaire de l'action au moment de la vente.
     * @param date Date de la transaction au format "YYYY-MM-DD".
     * @return true si la vente a ete executee, false si pas assez d'actions detenues.
     */
    bool sell_stock(const std::string& symbol, int quantity,
                    double unit_price, const std::string& date);

    /**
     * Calcule la valeur totale du portefeuille (cash + positions valorisees).
     * @param current_prices Prix actuel de chaque symbole detenu (map symbole -> prix).
     * @return Valeur totale du portefeuille.
     */
    double compute_total_value(const std::unordered_map<std::string, double>& current_prices) const;

    double get_available_cash() const;
    const std::unordered_map<std::string, int>& get_stock_positions() const;
    const std::vector<Transaction>& get_transaction_history() const;

private:
    double available_cash;
    std::unordered_map<std::string, int> stock_positions;
    std::vector<Transaction> transaction_history;
};

#endif
