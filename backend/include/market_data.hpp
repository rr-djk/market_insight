#ifndef MARKET_DATA_HPP
#define MARKET_DATA_HPP

#include <map>
#include <string>
#include <vector>
#include "database.hpp"
#include "price.hpp"

class MarketData {
public:
    /**
     * Charge les prix historiques depuis la base de donnees pour les symboles donnes.
     * @param db Connexion a la base de donnees PostgreSQL.
     * @param symbols Liste des tickers a charger (ex: {"AAPL", "MSFT"}).
     */
    MarketData(Database& db,
               const std::vector<std::string>& symbols);

    /**
     * Cree un contexte de marche a partir de donnees brutes (sans base de donnees).
     * @param raw_data Map associant chaque ticker a son historique de prix.
     */
    explicit MarketData(std::map<std::string, std::vector<Price>> raw_data);

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
    std::map<std::string, std::vector<Price>> data;
};

#endif
