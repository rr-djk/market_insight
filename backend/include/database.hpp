#ifndef DATABASE_HPP
#define DATABASE_HPP

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>
#include <pqxx/pqxx>
#include "price.hpp"

/**
 * @note Non thread-safe. La connexion pqxx::connection sous-jacente ne supporte pas
 *       l'accès concurrent. Ne pas partager une instance Database entre plusieurs threads
 *       sans synchronisation externe. Pendant un appel à get_prices_bulk ou
 *       get_close_prices_bulk, la connexion est en état COPY et ne peut pas
 *       traiter d'autres requêtes simultanément.
 */
class Database {
public:
    /**
     * Ouvre une connexion PostgreSQL à partir des credentials d'un fichier .env.
     * @param env_path Chemin vers le fichier .env contenant POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
     * @note Lance std::runtime_error si le fichier .env est introuvable ou si la connexion échoue.
     */
    Database(const std::string& env_path = "../.env");
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    /**
     * Récupère la liste de tous les symboles boursiers en base.
     * @return Vecteur de symboles triés par ordre alphabétique.
     */
    std::vector<std::string> get_symbols();

    /**
     * Récupère l'historique de prix OHLCV pour un symbole donné.
     * @param symbol Ticker du symbole (ex: "AAPL").
     * @param start_date Date de début au format "YYYY-MM-DD". Vide pour pas de borne inférieure.
     * @param end_date Date de fin au format "YYYY-MM-DD". Vide pour pas de borne supérieure.
     * @return Vecteur de Price trié par date croissante. Vide si le symbole n'existe pas.
     */
    std::vector<Price> get_prices(const std::string& symbol,
                                  const std::string& start_date = "",
                                  const std::string& end_date = "");

    /**
     * Récupère les prix OHLCV de plusieurs symboles en une seule requête.
     * @param symbols Liste de tickers à charger.
     * @param start_date Date de début au format "YYYY-MM-DD". Vide pour pas de borne inférieure.
     * @param end_date Date de fin au format "YYYY-MM-DD". Vide pour pas de borne supérieure.
     * @return Unordered map symbole → vecteur de Price trié par date croissante.
     */
    std::unordered_map<std::string, std::vector<Price>>
    get_prices_bulk(const std::vector<std::string>& symbols,
                    const std::string& start_date = "",
                    const std::string& end_date = "");

    /**
     * Récupère uniquement les prix de clôture de plusieurs symboles en une seule requête.
     * Plus efficace que get_prices_bulk pour les calculs ne nécessitant que le close.
     * @param symbols Liste de tickers à charger.
     * @param start_date Date de début au format "YYYY-MM-DD". Vide pour pas de borne inférieure.
     * @param end_date Date de fin au format "YYYY-MM-DD". Vide pour pas de borne supérieure.
     * @return Unordered map symbole → vecteur de Price trié par date croissante (seul close est renseigné).
     */
    std::unordered_map<std::string, std::vector<Price>>
    get_close_prices_bulk(const std::vector<std::string>& symbols,
                          const std::string& start_date = "",
                          const std::string& end_date = "");

private:
    std::unique_ptr<pqxx::connection> conn;

    /**
     * Parse un fichier .env et retourne les paires clé=valeur.
     * @param path Chemin vers le fichier .env.
     * @return Map des variables d'environnement lues.
     * @note Ignore les lignes vides et les commentaires (commençant par #).
     */
    std::map<std::string, std::string> load_env(const std::string& path);
};

#endif
