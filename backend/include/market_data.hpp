#ifndef MARKET_DATA_HPP
#define MARKET_DATA_HPP

#include <unordered_map>
#include <string>
#include <vector>
#include "database.hpp"
#include "price.hpp"

class MarketData {
public:
    /**
     * Charge les prix de cloture depuis la base de donnees pour les symboles donnes.
     * @param db Connexion a la base de donnees PostgreSQL.
     * @param symbols Liste des tickers a charger (ex: {"AAPL", "MSFT"}).
     * @param start_date Date de debut au format "YYYY-MM-DD". Vide pour pas de borne inferieure.
     * @param end_date Date de fin au format "YYYY-MM-DD". Vide pour pas de borne superieure.
     * @warning Seul le champ Price::close est renseigne. Les champs open, high, low et volume
     *          valent toujours 0. Cette surcharge est optimisee pour backtest et indicateurs
     *          techniques qui n'ont besoin que du prix de cloture.
     */
    MarketData(Database& db,
               const std::vector<std::string>& symbols,
               const std::string& start_date = "",
               const std::string& end_date = "");

    /**
     * Cree un contexte de marche a partir de donnees brutes (sans base de donnees).
     * @param raw_data Unordered map associant chaque ticker a son historique de prix.
     */
    explicit MarketData(std::unordered_map<std::string, std::vector<Price>> raw_data);

    /**
     * Retourne l'historique de prix pour un symbole donne.
     * @param symbol Ticker de l'action (ex: "AAPL").
     * @return Vecteur de prix tries par date croissante.
     * @throws std::out_of_range si le symbole n'existe pas dans les donnees chargees.
     */
    const std::vector<Price>& get_prices(const std::string& symbol) const;

    /**
     * Recupere la liste de tous les symboles disponibles en base.
     * @param db Connexion a la base de donnees PostgreSQL.
     * @return Vecteur de symboles tries par ordre alphabetique.
     */
    const std::vector<std::string> get_supported_symbols(Database& db);

private:
    std::unordered_map<std::string, std::vector<Price>> data;
};

#endif
